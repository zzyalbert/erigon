package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/snapshots/utils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"math/big"
	"time"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func init() {
	withChaindata(generateBodiesSnapshotCmd)
	withSnapshotFile(generateBodiesSnapshotCmd)
	withSnapshotData(generateBodiesSnapshotCmd)
	withBlock(generateBodiesSnapshotCmd)
	withDbType(generateBodiesSnapshotCmd)
	rootCmd.AddCommand(generateBodiesSnapshotCmd)

}

var generateBodiesSnapshotCmd = &cobra.Command{
	Use:     "bodies",
	Short:   "Generate bodies snapshot",
	Example: "go run cmd/snapshots/generator/main.go bodies --block 11000000 --chaindata /media/b00ris/nvme/snapshotsync/tg/chaindata/ --snapshotDir /media/b00ris/nvme/snapshotsync/tg/snapshots/ --snapshotMode \"hb\" --snapshot /media/b00ris/nvme/snapshots/bodies_test",
	RunE: func(cmd *cobra.Command, args []string) error {
		return BodySnapshot(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode, dbType)
	},
}

func BodySnapshot(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string, dbType string) error {
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()
	db := ethdb.NewObjectDatabase(kv)
	var (
		hash common.Hash
		err error
	)
	t := time.Now()
	//todo интегрировать mdbx+lmdb
	//todo добавить генерацию infohash
	//todo не canonical блоки при переносе в EthTx
	//todo тестовый стейдж генерации/переноса данных в снепшот
	//todo подмена на новый снепшот
	//todo идея прунинга на последнем этапе.
	if snapshotDir != "" {
		var mode snapshotsync.SnapshotMode
		mode, err = snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}

		kv, err = snapshotsync.WrapBySnapshotsFromDir(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}

	snKV := utils.OpenSnapshotKV(dbType, func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.EthTx:          dbutils.BucketsConfigs[dbutils.EthTx],
			dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}, snapshotPath)

	snDB := ethdb.NewObjectDatabase(snKV)
	tx,err:=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		return err
	}
	defer tx.Rollback()

	txID:=uint64(0)
	txIDBytes:=make([]byte,8)
	for i := uint64(1); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}

		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		bodyRlp:=rawdb.ReadStorageBodyRLP(db, hash, i)

		bodyForStorage :=new(types.BodyForStorage)
		err = rlp.DecodeBytes(bodyRlp, bodyForStorage)
		if err != nil {
			log.Error("Invalid block body RLP", "hash", hash, "err", err)
			return err
		}

		err = tx.Append(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(i, hash), bodyRlp)
		if err!=nil {
			return err
		}

		for id:=txID; id < txID+uint64(bodyForStorage.TxAmount); id++ {
			binary.BigEndian.Uint64(txIDBytes)
			err = tx.Append(dbutils.EthTx, dbutils.BlockBodyKey(i, hash), bodyRlp)
			if err!=nil {
				return err
			}
		}
	}
	err=tx.Commit()
	if err!=nil {
		return err
	}
	log.Info("Bodies copied", "t", time.Since(t))

	err = snDB.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadNumber error", "err", err)
		return err
	}
	err = snDB.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadHash), hash.Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadHash error", "err", err)
		return err
	}
	snDB.Close()
	log.Info("Finished", "duration", time.Since(t))

	return utils.RmTmpFiles(dbType, snapshotPath)
}
