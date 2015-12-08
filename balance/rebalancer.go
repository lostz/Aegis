package balance

import (
	"fmt"
	"io"

	"github.com/lostz/Aegis/pool"
)

type Rebalancer struct {
	index         int
	gcb           int
	cw            int
	servers       []*RbServer
	currentWeight int
}

type RbServer struct {
	weight int
	good   bool
	pool   *pool.Pool
}

func NewRbServer(w int, p *pool.Pool) *RbServer {
	return &RbServer{weight: w, good: true, pool: p}

}

func NewRebalancer(s []*RbServer) Rebalancer {
	r := Rebalancer{index: -1, servers: s}
	r.getgcd()
	return r
}

func (self *Rebalancer) Get() (io.Closer, error) {
	for {
		self.index = (self.index + 1) % len(self.servers)
		if self.index == 0 {
			self.cw = self.cw - self.gcb
			if self.cw <= 0 {
				self.cw = self.getMaxCw()
				if self.cw == 0 {
					return nil, fmt.Errorf("no avaliable server")
				}
			}
		}
		if wt := self.servers[self.index].weight; wt >= self.cw {
			con, err := self.servers[self.index].pool.Get()
			return con, err
		}

	}

}

func (self *Rebalancer) getMaxCw() int {
	max := -1
	for _, s := range self.servers {
		if s.weight > max {
			max = s.weight
		}
	}
	return max
}

func (self *Rebalancer) getgcd() {
	m := 1
	for _, s := range self.servers {
		m = gcd(m, s.weight)
	}
	self.gcb = m

}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
