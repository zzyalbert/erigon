package generate

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"os"
	"os/signal"
	"time"
)

//  ./build/bin/tg --datadir /media/b00ris/nvme/snapshotsync/ --nodiscover --snapshot-mode hb --port 30304
// go run ./cmd/state/main.go stateSnapshot --block 11000000 --chaindata /media/b00ris/nvme/tgstaged/tg/chaindata/ --snapshot /media/b00ris/nvme/snapshots/state
func GenerateStateSnapshot(dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
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
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
		dbutils.PlainStateBucket:       dbutils.BucketConfigItem{},
		dbutils.CodeBucket:       dbutils.BucketConfigItem{},
		dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
	}
	}).Path(snapshotPath).MustOpen()

	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()

	//db := ethdb.NewObjectDatabase(kv)
	sndb := ethdb.NewObjectDatabase(snkv)
	mt:=sndb.NewBatch()

	tx, err := kv.Begin(context.Background(), nil, false)
	if err != nil {
		return err
	}
	tx2, err := kv.Begin(context.Background(), nil, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()


	i:=0
	t:=time.Now()
	tt:=time.Now()
	//st:=0
	err = state.WalkAsOf(tx, dbutils.PlainStateBucket,dbutils.AccountsHistoryBucket, []byte{},0,toBlock+1, func(k []byte, v []byte) (bool, error) {
		i++
		if i%100000==0 {
			fmt.Println(i, common.Bytes2Hex(k),"batch", time.Since(tt))
			tt=time.Now()
			select {
			case <-ch:
				return false, errors.New("interrupted")
			default:

			}
		}
		if len(k) != 20 {
			fmt.Println("ln", len(k))
			return true, nil
		}
		var acc accounts.Account
		if err = acc.DecodeForStorage(v); err != nil {
			return false, fmt.Errorf("decoding %x for %x: %v", v, k, err)
		}

		innerErr:=mt.Put(dbutils.PlainStateBucket, common.CopyBytes(k), common.CopyBytes(v))
		if innerErr!=nil {
			return false, innerErr
		}
		if acc.Incarnation>0 {
			storagePrefix := dbutils.PlainGenerateStoragePrefix(k, acc.Incarnation)
			innerErr = state.WalkAsOf(tx2, dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, storagePrefix, 8*(common.AddressLength+common.IncarnationLength), toBlock+1, func(kk []byte, vv []byte) (bool, error) {
				innerErr=mt.Put(dbutils.PlainStateBucket, dbutils.PlainGenerateCompositeStorageKey(common.BytesToAddress(kk[:common.AddressLength]),acc.Incarnation, common.BytesToHash(kk[common.AddressLength:])), common.CopyBytes(vv))
				if innerErr!=nil {
					fmt.Println("mt.Put", innerErr)
					return false, innerErr
				}
				return true, nil
			})
			if innerErr!=nil {
				fmt.Println("mt.Put", innerErr)
				return false, innerErr
			}

			code,err:=tx.Get(dbutils.CodeBucket, acc.CodeHash.Bytes())
			if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				fmt.Println("1")
				return false, err
			}
			if err := mt.Put(dbutils.CodeBucket, acc.CodeHash.Bytes(), code); err != nil {
				fmt.Println("2")
				return false, err
			}
		}
		if mt.BatchSize() >= mt.IdealBatchSize() {
			ttt:=time.Now()
			innerErr = mt.CommitAndBegin(context.Background())
			if innerErr!=nil {
				fmt.Println("mt.BatchSize", innerErr)
				return false, innerErr
			}
			fmt.Println("Commited", time.Since(ttt))
		}
		return true, nil
	})
	_,err=mt.Commit()
	if err!=nil {
		return err
	}
	fmt.Println("took", time.Since(t))

	return err
}