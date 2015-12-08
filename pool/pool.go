package pool

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/lostz/Aegis/backend"
	"github.com/lostz/Aegis/mysql"
)

var nowFunc = time.Now
var ErrPoolClosed = errors.New("connection pool closed")
var ErrPoolExhausted = errors.New("connection pool exhausted")
var ErrConnClosed = errors.New("connection closed")

type Pool struct {
	Dial func() (backend.Client, error)
	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c backend.Client, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c backend.Client
	t time.Time
}

func NewPool(newFn func() (backend.Client, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

func (self *Pool) Get() (backend.Client, error) {
	c, err := self.get()
	if err != nil {
		return nil, err
	}
	return &pooledConnection{p: self, c: c}, nil
}

func (self *Pool) ActiveCount() int {
	self.mu.Lock()
	active := self.active
	self.mu.Unlock()
	return active
}

func (self *Pool) Close() error {
	self.mu.Lock()
	idle := self.idle
	self.idle.Init()
	self.closed = true
	self.active -= idle.Len()
	if self.cond != nil {
		self.cond.Broadcast()
	}
	self.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

func (self *Pool) release() {
	self.active -= 1
	if self.cond != nil {
		self.cond.Signal()
	}
}

func (self *Pool) get() (backend.Client, error) {
	self.mu.Lock()

	// Prune stale connections.

	if timeout := self.IdleTimeout; timeout > 0 {
		for i, n := 0, self.idle.Len(); i < n; i++ {
			e := self.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			self.idle.Remove(e)
			self.release()
			self.mu.Unlock()
			ic.c.Close()
			self.mu.Lock()
		}
	}

	for {

		// Get idle connection.

		for i, n := 0, self.idle.Len(); i < n; i++ {
			e := self.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			self.idle.Remove(e)
			test := self.TestOnBorrow
			self.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			self.mu.Lock()
			self.release()
		}

		// Check for pool closed before dialing a new connection.

		if self.closed {
			self.mu.Unlock()
			return nil, ErrPoolClosed
		}

		// Dial new connection if under limit.

		if self.MaxActive == 0 || self.active < self.MaxActive {
			dial := self.Dial
			self.active += 1
			self.mu.Unlock()
			c, err := dial()
			if err != nil {
				self.mu.Lock()
				self.release()
				self.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !self.Wait {
			self.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if self.cond == nil {
			self.cond = sync.NewCond(&self.mu)
		}
		self.cond.Wait()
	}
}

func (self *Pool) put(c backend.Client, forceClose bool) error {
	self.mu.Lock()
	if !self.closed && !forceClose {
		self.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if self.idle.Len() > self.MaxIdle {
			c = self.idle.Remove(self.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if self.cond != nil {
			self.cond.Signal()
		}
		self.mu.Unlock()
		return nil
	}

	self.release()
	self.mu.Unlock()
	return c.Close()
}

type pooledConnection struct {
	p *Pool
	c backend.Client
}

func (self *pooledConnection) Connect(addr string, user string, password string, db string) error {
	return self.c.Connect(addr, user, password, db)
}
func (self *pooledConnection) Execute(command string, args ...interface{}) (*mysql.Result, error) {
	return self.c.Execute(command, args...)
}
func (self *pooledConnection) Rollback() error {
	return self.c.Rollback()
}

func (self *pooledConnection) Close() error {
	c := self.c
	if self.c == nil {
		return nil
	}
	self.p.put(c, false)
	return nil
}
