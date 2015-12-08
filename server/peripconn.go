package server

import (
	"fmt"
	"net"
	"sync"
)

type perIPConnCounter struct {
	lock sync.Mutex
	m    map[uint32]int
}

func (self *perIPConnCounter) Register(ip uint32) int {
	self.lock.Lock()
	if self.m == nil {
		self.m = make(map[uint32]int)
	}
	n := self.m[ip] + 1
	self.m[ip] = n
	self.lock.Unlock()
	return n
}

func (self *perIPConnCounter) Unregister(ip uint32) {
	self.lock.Lock()
	if self.m == nil {
		self.lock.Unlock()
		panic("BUG: perIPConnCounter.Register() wasn't called")
	}
	n := self.m[ip] - 1
	if n < 0 {
		self.lock.Unlock()
		panic(fmt.Sprintf("BUG: negative per-ip counter=%d for ip=%d", n, ip))
	}
	self.m[ip] = n
	self.lock.Unlock()
}

type perIPConn struct {
	net.Conn

	ip               uint32
	perIPConnCounter *perIPConnCounter

	v interface{}
}

func acquirePerIPConn(conn net.Conn, ip uint32, counter *perIPConnCounter) *perIPConn {
	v := perIPConnPool.Get()
	if v == nil {
		v = &perIPConn{}
	}
	c := v.(*perIPConn)
	c.Conn = conn
	c.ip = ip
	c.perIPConnCounter = counter
	c.v = v
	return c
}

func releasePerIPConn(c *perIPConn) {
	c.Conn = nil
	c.perIPConnCounter = nil
	perIPConnPool.Put(c.v)
}

var perIPConnPool sync.Pool

func (self *perIPConn) Close() error {
	err := self.Conn.Close()
	self.perIPConnCounter.Unregister(self.ip)
	releasePerIPConn(self)
	return err
}

func getUint32IP(c net.Conn) uint32 {
	return ip2uint32(getConnIP4(c))
}

func getConnIP4(c net.Conn) net.IP {
	addr := c.RemoteAddr()
	ipAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return net.IPv4zero
	}
	return ipAddr.IP.To4()
}

func ip2uint32(ip net.IP) uint32 {
	if len(ip) != 4 {
		return 0
	}
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

func uint322ip(ip uint32) net.IP {
	b := make([]byte, 4)
	b[0] = byte(ip >> 24)
	b[1] = byte(ip >> 16)
	b[2] = byte(ip >> 8)
	b[3] = byte(ip)
	return b
}
