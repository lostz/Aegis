package balance

import (
	"fmt"
	"io"
	"testing"

	"github.com/lostz/Aegis/pool"
)

type Mock struct {
}

func (self Mock) Close() error {
	return nil
}

func TestBalance(t *testing.T) {
	w := pool.NewPool(func() (io.Closer, error) {
		return Mock{}, nil
	}, 10)
	r1 := pool.NewPool(func() (io.Closer, error) {
		return Mock{}, nil
	}, 10)
	r2 := pool.NewPool(func() (io.Closer, error) {
		return Mock{}, nil
	}, 10)
	w1s := &RbServer{weight: 2, pool: w}
	r1s := &RbServer{weight: 4, pool: r1}
	r2s := &RbServer{weight: 8, pool: r2}
	b := Rebalancer{index: -1, servers: []*RbServer{w1s, r1s, r2s}}
	b.getgcd()
	for i := 0; i < 10; i++ {
		fmt.Println(b.Get())
	}

}
