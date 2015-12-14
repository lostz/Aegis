package pool

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/lostz/Aegis/mysql"
)

type Client interface {
	Connect(addr string, user string, password string, db string) error
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Rollback() error
	CollationId() uint32
	Close() error
}

var nowFunc = time.Now
var ErrPoolClosed = errors.New("connection pool closed")
var ErrPoolExhausted = errors.New("connection pool exhausted")
var ErrConnClosed = errors.New("connection closed")

type Pool struct {
	Dial         func() (Client, error)
	TestOnBorrow func(c Client, t time.Time) error
	MaxIdle      int
	MaxActive    int
	IdleTimeout  time.Duration
	Wait         bool
	mu           sync.Mutex
	cond         *sync.Cond
	closed       bool
	active       int
	idle         list.List
	busy         map[uint32]Client
}

type idleConn struct {
	c Client
	t time.Time
}

func NewPool(newFn func() (Client, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

func (self *Pool) Get() (Client, error) {
	c, err := self.get()
	if err != nil {
		return nil, err
	}
	go self.putBusy(c)
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

func (self *Pool) get() (Client, error) {
	self.mu.Lock()

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

		if self.closed {
			self.mu.Unlock()
			return nil, ErrPoolClosed
		}

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

func (self *Pool) putBusy(c Client) {
	self.busy[c.CollationId()] = c
}

func (self *Pool) removeBusy(c Client) {
	delete(self.busy, c.CollationId())

}

func (self *Pool) put(c Client, forceClose bool) error {
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
		go self.removeBusy(c)
		return nil
	}

	self.release()
	self.mu.Unlock()
	go self.removeBusy(c)
	return c.Close()
}

type pooledConnection struct {
	p *Pool
	c Client
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

func (self *pooledConnection) CollationId() uint32 {
	return self.c.CollationId()
}

func (self *pooledConnection) Close() error {
	c := self.c
	if self.c == nil {
		return nil
	}
	self.p.put(c, false)
	return nil
}
