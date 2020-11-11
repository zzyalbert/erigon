package generate

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"os"
	"time"
)

func GenerateStateSnapshot2(dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}

	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()

	if snapshotDir != "" {
		var mode torrent.SnapshotMode
		mode, err = torrent.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}

		kv, err = torrent.WrapBySnapshots(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}
	db:=ethdb.NewObjectDatabase(kv)
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket:       dbutils.BucketConfigItem{},
			dbutils.PlainContractCodeBucket:       dbutils.BucketConfigItem{},
			dbutils.CodeBucket:       dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	batch, err:=db.Begin(context.Background())
	if err!=nil {
		return err
	}
	defer batch.Rollback()

	currentBlock,data,err:=stages.GetStageProgress(batch, stages.Execution)
	if err!=nil {
		return err
	}
	fmt.Println("data", string(data))

	stage:=&stagedsync.StageState{
		Stage: stages.Execution,
		BlockNumber: currentBlock,
	}
	u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: toBlock}
	fmt.Println("Start unwind")
	tt:=time.Now()
	err=stagedsync.UnwindExecutionStage(u, stage, batch, false)
	if err!= nil {
		batch.Rollback()
		return err
	}
	fmt.Println("End unwind", time.Since(tt))

	writedb:=ethdb.NewObjectDatabase(snkv)
	mt,err:=writedb.Begin(context.Background())
	if err!=nil {
		return err
	}

	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		err:=mt.Put(dbutils.PlainStateBucket, common.CopyBytes(k), common.CopyBytes(v))
		if err!=nil {
			return false, err
		}
		return true, nil
	})
	if err!=nil {
		return err
	}
	_,err=mt.Commit()
	if err!=nil {
		return err
	}

	//err = db.Walk(dbutils.CodeBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	err:=mt.Put(dbutils.CodeBucket, common.CopyBytes(k), common.CopyBytes(v))
	//	if err!=nil {
	//		return false, err
	//	}
	//	return true, nil
	//})

	return nil
}