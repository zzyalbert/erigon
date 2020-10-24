package filters

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"

	"github.com/holiman/uint256"
)

var _ state.WriterWithChangeSets = &BloomStateWriter{}

type BloomStateWriter struct {
	filter Filter
	inner  state.WriterWithChangeSets
}

func NewBloomStateWriter(filter Filter, inner state.WriterWithChangeSets) *BloomStateWriter {
	return &BloomStateWriter{filter, inner}
}

func (w *BloomStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	w.filter.Add(address[:])
	return w.inner.UpdateAccountData(ctx, address, original, account)
}
func (w *BloomStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.filter.Add(address[:])
	return w.inner.UpdateAccountCode(address, incarnation, codeHash, code)
}

func (w *BloomStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if deleter, ok := w.filter.(Deleter); ok {
		deleter.Delete(address.Bytes())
	}
	return w.inner.DeleteAccount(ctx, address, original)
}

func (w *BloomStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	w.filter.Add(address[:])
	return w.inner.WriteAccountStorage(ctx, address, incarnation, key, original, value)
}
func (w *BloomStateWriter) CreateContract(address common.Address) error {
	w.filter.Add(address[:])
	return w.inner.CreateContract(address)
}

func (w *BloomStateWriter) WriteChangeSets() error {
	return w.inner.WriteChangeSets()
}
func (w *BloomStateWriter) WriteHistory() error {
	return w.inner.WriteHistory()
}
