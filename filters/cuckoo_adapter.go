package filters

import (
	cuckoo "github.com/panmari/cuckoofilter"
)

var _ Filter = &CuckooAdapter{}
var _ Deleter = &CuckooAdapter{}

type CuckooAdapter struct {
	inner *cuckoo.Filter
}

func NewCuckoo(inner *cuckoo.Filter) *CuckooAdapter {
	return &CuckooAdapter{inner}
}

func (a *CuckooAdapter) Add(value []byte) {
	a.inner.Insert(value)
}

func (a *CuckooAdapter) Test(value []byte) bool {
	return a.inner.Lookup(value)
}

func (a *CuckooAdapter) Delete(value []byte) {
	a.inner.Delete(value)
}

func (a *CuckooAdapter) Name() string {
	return "CUCKOO_FILTER"
}
