package stagedsync

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/cetl"
)

func SpawnBlockHashStage(s *StageState, stateDB ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	headHash := rawdb.ReadHeadHeaderHash(stateDB)
	headNumber := rawdb.ReadHeaderNumber(stateDB, headHash)
	if *headNumber == s.BlockNumber {
		s.Done()
		return nil
	}
	tx, err := stateDB.Begin(context.Background(), ethdb.RW)
	defer tx.Rollback()
	if err != nil {
		return err
	}
	cetl.TransformBlockHashes(tx.(ethdb.HasTx).Tx(), s.BlockNumber)
	tx.Commit()
	return s.DoneAndUpdate(stateDB, *headNumber)
}
