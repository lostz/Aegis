package server

import (
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/lostz/Aegis/backend"
	"github.com/lostz/Aegis/balance"
	"github.com/lostz/Aegis/config"
	"github.com/lostz/Aegis/logging"
	"github.com/lostz/Aegis/mysql"
	"github.com/lostz/Aegis/pool"
)

var logger = logging.GetLogger("server")

const DefaultConcurrency = 1 * 1024

type Server struct {
	Version    string
	BuildStamp string
	CommitId   string

	perIPConnCounter perIPConnCounter
	running          bool
	listener         net.Listener
	writerPool       pool.Pool
	readerPool       balance.Rebalancer
	Concurrency      int
	MaxConnsPerIP    int
	cfg              *config.Config
}

func NewServer(conf *config.Config) *Server {
	s := &Server{cfg: conf}
	s.Concurrency = 1
	s.MaxConnsPerIP = 1
	return s
}

func (self *Server) Start() error {
	wPool := pool.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (backend.Client, error) {
			conn := &backend.Conn{}
			err := conn.Connect(self.cfg.DBAddr, self.cfg.DBUser, self.cfg.DBPasswd, self.cfg.DB)
			return conn, err
		},
	}
	self.writerPool = wPool
	rs := balance.NewRbServer(20, &wPool)
	self.readerPool = balance.NewRebalancer([]*balance.RbServer{rs})

	return self.ListenAndServe()
}

func (self *Server) WriteTooManyConnection(c net.Conn) {
	s := self.newSessionConn(c)
	err := mysql.NewMysqlError(1203, "Too many connections")
	s.writeError(err)
	c.Close()
}

func (self *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", self.cfg.Addr)
	if err != nil {
		return err
	}
	return self.Serve(ln)

}

func (self *Server) Serve(ln net.Listener) error {
	maxWorkersCount := self.getConcurrency()
	wp := &pool.WorkerPool{
		WorkerFunc:      self.serveConn,
		MaxWorkersCount: maxWorkersCount,
	}
	wp.Start()
	var lastPerIPErrorTime time.Time
	for {
		c, err := acceptConn(self, ln, &lastPerIPErrorTime)
		if err != nil {
			// to many per ip connect
			if err == mysql.ErrTooManyUserConnections {
				self.WriteTooManyConnection(c)
			}
			continue
		}
		if !wp.Serve(c) {
			// worker pool is full
			self.WriteTooManyConnection(c)
			continue

		}

	}
	return nil

}

func (self *Server) getConcurrency() int {
	n := self.Concurrency
	if n <= 0 {
		n = DefaultConcurrency
	}
	return n
}

func (self *Server) serveConn(c net.Conn) error {
	s := self.newSessionConn(c)
	if err := s.Handshake(); err != nil {
		return err
	}
	s.Run()
	return nil

}

func (self *Server) newSessionConn(c net.Conn) *Session {
	s := new(Session)
	s.c = c
	s.pkg = mysql.NewPackets(c)
	s.s = self
	s.pkg.Sequence = 0
	s.connectionId = atomic.AddUint32(&baseConnId, 1)
	s.status = mysql.SERVER_STATUS_AUTOCOMMIT
	s.salt, _ = mysql.RandomBuf(20)
	s.collation = mysql.DEFAULT_COLLATION_ID
	s.charset = mysql.DEFAULT_CHARSET
	return s

}

func acceptConn(s *Server, ln net.Listener, lastPerIPErrorTime *time.Time) (net.Conn, error) {
	for {
		c, err := ln.Accept()
		if err != nil {
			logger.Errorf("Temporary error when accepting new connections: %s", err.Error())
			return nil, io.EOF
		}
		if s.MaxConnsPerIP > 0 {
			pic := wrapPerIPConn(s, c)
			if pic == nil {
				if time.Since(*lastPerIPErrorTime) > time.Minute {
					logger.Errorf("The number of connections from %s exceeds MaxConnsPerIP=%d",
						getConnIP4(c), s.MaxConnsPerIP)
					*lastPerIPErrorTime = time.Now()
				}
				// too many connections
				return c, mysql.ErrTooManyUserConnections
			}
			return pic, nil
		}
		return c, nil
	}

}

func wrapPerIPConn(s *Server, c net.Conn) net.Conn {
	ip := getUint32IP(c)
	if ip == 0 {
		return c
	}
	n := s.perIPConnCounter.Register(ip)
	if n > s.MaxConnsPerIP {
		s.perIPConnCounter.Unregister(ip)
		return nil
	}
	return acquirePerIPConn(c, ip, &s.perIPConnCounter)
}
