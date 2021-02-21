package stagedsync

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"math/big"
	"time"
)


func SpawnAbsInt(s *StageState, db0 ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, tmpdir string, quit <-chan struct{}, params CallTracesStageParams) error {
	var db ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db0.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = db0.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		db, err = db0.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer db.Rollback()
	}

	endBlock, err := s.ExecutionAt(db)
	if params.ToBlock > 0 && params.ToBlock < endBlock {
		endBlock = params.ToBlock
	}
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	log.Info(fmt.Sprintf("[%s] Run abstract interpretation", logPrefix))
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	if err := runAbsInt(logPrefix, db, s.BlockNumber+1, endBlock, chainConfig, chainContext, bitmapsBufLimit, bitmapsFlushEvery, tmpdir, quit, params); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(db, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := db.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func runAbsInt(logPrefix string, db ethdb.Database, startBlock, endBlock uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, bufLimit datasize.ByteSize, flushEvery time.Duration, tmpdir string, quit <-chan struct{}, params CallTracesStageParams) error {

	//engine := chainContext.Engine()

	//var cache = shards.NewStateCache(32, params.CacheSize)

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {

		blockHash, err2 := rawdb.ReadCanonicalHash(db, blockNum)
		if err2 != nil {
			return fmt.Errorf("%s: getting canonical blockhash for block %d: %v", logPrefix, blockNum, err2)
		}
		block := rawdb.ReadBlock(db, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(db, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		/*var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets
		//reader := state.NewPlainDBState(db, blockNum-1)
		stateReader = state.NewCachedReader(reader, cache)
		//stateWriter = state.NewCachedWriter(state.NewNoopWriter(), cache)

		//tracer := NewAbsIntTracer()


		/*
		vmConfig := &vm.Config{Debug: true, NoReceipts: false, ReadOnly: false, Tracer: tracer}

		receipts, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter)
		if err != nil {
			return fmt.Errorf("[%s] %w", logPrefix, err)
		}

		for _, r := range receipts {
			fmt.Printf("*****************created %v\n", r.ContractAddress.String())
		}*/
	}

	return nil
}

type AbsIntTracer struct {
}

func NewAbsIntTracer() *AbsIntTracer {
	return &AbsIntTracer {

	}
}

func (ct *AbsIntTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int) error {
	if create {
		print("Here2\n")
		hex := hex.EncodeToString(input)
		fmt.Printf("%v\n", hex)
	}
	return nil
}

func (ct *AbsIntTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	//TODO: Populate froms and tos if it is any call opcode
	return nil
}
func (ct *AbsIntTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *AbsIntTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct *AbsIntTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}
func (ct *AbsIntTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *AbsIntTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}