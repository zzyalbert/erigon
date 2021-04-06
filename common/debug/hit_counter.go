package debug

import (
	"fmt"
	"sync/atomic"
)

type HitCounter struct {
	hit        uint64
	miss       uint64
	printEvery uint64
}

func NewCounter(printEvery int) *HitCounter {
	return &HitCounter{0, 0, uint64(printEvery)}
}

func (c *HitCounter) Hit() {
	hit := atomic.AddUint64(&c.hit, 1)

	miss := atomic.LoadUint64(&c.miss)
	printCounter(hit, miss, c.printEvery)
}

func (c *HitCounter) Miss() {
	miss := atomic.AddUint64(&c.miss, 1)

	hit := atomic.LoadUint64(&c.hit)
	printCounter(hit, miss, c.printEvery)
}

func (c *HitCounter) String() string {
	hit := atomic.LoadUint64(&c.hit)
	miss := atomic.LoadUint64(&c.miss)

	return fmt.Sprintf("total %d, hit %d, miss %d, hit rate %.4f", hit+miss, hit, miss, float64(hit)/float64(hit+miss))
}

func printCounter(hit, miss, printEvery uint64) {
	if (hit+miss)%printEvery == 0 {
		fmt.Printf("total %d, hit %d, miss %d, hit rate %.4f\n", hit+miss, hit, miss, float64(hit)/float64(hit+miss))
	}
}
