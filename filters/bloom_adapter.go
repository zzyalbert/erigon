package filters

import (
	"github.com/willf/bloom"
)

var _ Filter = &BloomAdapter{}

type BloomAdapter struct {
	inner *bloom.BloomFilter
}

func NewBloom(inner *bloom.BloomFilter) *BloomAdapter {
	return &BloomAdapter{inner}
}

func (a *BloomAdapter) Add(value []byte) {
	a.inner.Add(value)
}

func (a *BloomAdapter) Test(value []byte) bool {
	return a.inner.Test(value)
}

func (a *BloomAdapter) Name() string {
	return "BLOOM_FILTER"
}
