package filters

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

var _ state.StateReader = &BloomStateReader{}

type BloomStateReader struct {
	inner  state.StateReader
	filter Filter
}

func NewBloomStateReader(bloomFilter Filter, inner state.StateReader) *BloomStateReader {
	return &BloomStateReader{inner, bloomFilter}
}

func (r *BloomStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if !r.filter.Test(address.Bytes()) {
		return nil, nil
	}
	return r.inner.ReadAccountData(address)
}

func (r *BloomStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if !r.filter.Test(address.Bytes()) {
		return nil, nil
	}
	return r.inner.ReadAccountStorage(address, incarnation, key)
}

func (r *BloomStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if !r.filter.Test(address.Bytes()) {
		return nil, nil
	}
	return r.inner.ReadAccountCode(address, codeHash)
}

func (r *BloomStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	if !r.filter.Test(address.Bytes()) {
		return 0, nil
	}
	return r.inner.ReadAccountCodeSize(address, codeHash)
}

func (r *BloomStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if !r.filter.Test(address.Bytes()) {
		return 0, nil
	}
	return r.inner.ReadAccountIncarnation(address)
}
