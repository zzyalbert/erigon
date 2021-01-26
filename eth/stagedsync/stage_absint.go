package stagedsync

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

func SpawnAbsInt(s *StageState, db ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, tmpdir string, quit <-chan struct{}, params CallTracesStageParams) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
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


	//if err := promoteCallTraces(logPrefix, tx, s.BlockNumber+1, endBlock, chainConfig, chainContext, bitmapsBufLimit, bitmapsFlushEvery, tmpdir, quit, params); err != nil {
//		return err
//	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
