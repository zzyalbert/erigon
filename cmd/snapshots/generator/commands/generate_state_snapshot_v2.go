package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateStateSnapshotV2Cmd)
	withSnapshotFile(generateStateSnapshotV2Cmd)
	withSnapshotData(generateStateSnapshotV2Cmd)
	withBlock(generateStateSnapshotV2Cmd)
	rootCmd.AddCommand(generateStateSnapshotV2Cmd)

}

//go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state --chaindata /media/b00ris/nvme/backup/snapshotsync/tg/chaindata/ &> /media/b00ris/nvme/copy.log
var generateStateSnapshotV2Cmd = &cobra.Command{
	Use:     "statev2",
	Short:   "Generate state snapshot",
	Example: "go run ./cmd/state/main.go statev2 --block 11000000 --chaindata /media/b00ris/nvme/tgstaged/tg/chaindata/ --snapshot /media/b00ris/nvme/snapshots/state",
	RunE: func(cmd *cobra.Command, args []string) error {
		return GenerateStateSnapshotV2(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func GenerateStateSnapshotV2(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	toBlock = 1000
	dbPath = "/media/b00ris/nvme/fresh_sync/tg/chaindata/"
	//if snapshotPath == "" {
	//	return errors.New("empty snapshot path")
	//}

	//err := os.RemoveAll(snapshotPath)
	//if err != nil {
	//	return err
	//}
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()

	if snapshotDir != "" {
		var mode snapshotsync.SnapshotMode
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}

		kv, err = snapshotsync.WrapBySnapshotsFromDir(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}

	//snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	//	return dbutils.BucketsCfg{
	//		dbutils.PlainStateBucket:        dbutils.BucketConfigItem{},
	//		dbutils.PlainContractCodeBucket: dbutils.BucketConfigItem{},
	//		dbutils.CodeBucket:              dbutils.BucketConfigItem{},
	//		dbutils.StateSnapshotInfoBucket: dbutils.BucketConfigItem{},
	//	}
	//}).Path(snapshotPath).MustOpen()
	//
	//sndb := ethdb.NewObjectDatabase(snkv)
	//mt := sndb.NewBatch()

	tx, err := kv.Begin(context.Background(), nil, ethdb.RO)
	defer tx.Rollback()
	mainCursor := tx.Cursor(dbutils.PlainStateBucket)
	defer mainCursor.Close()
	ahCursorBase := tx.Cursor(dbutils.AccountsHistoryBucket)
	defer ahCursorBase.Close()
	shCursorBase := tx.Cursor(dbutils.StorageHistoryBucket)
	defer shCursorBase.Close()
	acsCursor := tx.CursorDupSort(dbutils.PlainAccountChangeSetBucket)
	defer acsCursor.Close()
	scsCursor := tx.CursorDupSort(dbutils.PlainStorageChangeSetBucket)
	defer scsCursor.Close()

	var ahCursor = ethdb.NewSplitCursor(
		ahCursorBase,
		[]byte{},
		0,                      /* fixedBits */
		common.AddressLength,   /* part1end */
		common.AddressLength,   /* part2start */
		common.AddressLength+8, /* part3start */
	)


	var shCursor = ethdb.NewSplitCursor(
		shCursorBase,
		[]byte{},
		0,
		common.AddressLength,                   /* part1end */
		common.AddressLength,                   /* part2start */
		common.AddressLength+common.HashLength, /* part3start */
	)

	k,v,err:=mainCursor.First()
	fmt.Println(common.Bytes2Hex(k), common.Bytes2Hex(v), err)
	hAccAddr, aTsEnc, _, ahV,err:=ahCursor.Seek()
	for hAccAddr != nil && binary.BigEndian.Uint64(aTsEnc) < toBlock {
		hAccAddr, aTsEnc, _, ahV,err = ahCursor.Next()
		if err != nil {
			return err
		}
	}
	fmt.Println(common.Bytes2Hex(hAccAddr), len(ahV),  binary.BigEndian.Uint64(aTsEnc), err)
	hStorageAddr, hStorageLoc, tsStorageEnc, hStorageV, err:=shCursor.Seek()
	for binary.BigEndian.Uint64(tsStorageEnc) < toBlock {
		if 	hStorageAddr, hStorageLoc, tsStorageEnc, hStorageV, err = shCursor.Next(); err != nil {
			return err
		}
	}
	fmt.Println(common.Bytes2Hex(hStorageAddr), common.Bytes2Hex(hStorageLoc), binary.BigEndian.Uint64(tsStorageEnc), len(hStorageV), err)

	fmt.Println("cycle")
	goOn := true
	//var err error
	for goOn {
		if common.Bytes2Hex(k)=="0000000000000000ffffffffffffffffffffffff" {
			fmt.Println()
		}
		if len(k)==20 {
			cmp, br := common.KeyCmp(k, hAccAddr)
			if br {
				break
			}
			if cmp < 0 {
				fmt.Println("addr cmp<0", len(k), common.Bytes2Hex(k), len(v))
				//goOn, err = walker(k, v)
			} else {
				index := roaring64.New()
				_, err = index.ReadFrom(bytes.NewReader(ahV))
				if err != nil {
					return err
				}
				found, ok := bitmapdb.SeekInBitmap64(index, toBlock)
				changeSetBlock := found
				if ok {
					// Extract value from the changeSet
					csKey := dbutils.EncodeBlockNumber(changeSetBlock)
					kData, data, err3 := acsCursor.SeekBothRange(csKey, hAccAddr)
					if err3 != nil {
						return err3
					}
					if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hAccAddr) {
						return fmt.Errorf("inconsistent account history and changesets, kData %x, csKey %x, data %x, hK %x", kData, csKey, data, hAccAddr)
					}
					data = data[common.AddressLength:]
					if len(data) > 0 { // Skip accounts did not exist
						fmt.Println("hst", common.Bytes2Hex(hAccAddr))
						//goOn, err = walker(hK, data)
					}
				} else if cmp == 0 {
					fmt.Println("cmp==0", common.Bytes2Hex(k))
					//goOn, err = walker(k, v)
				}
			}
			if err != nil {
				return err
			}
			if goOn {
				if cmp <= 0 {
					k, v, err = mainCursor.Next()
					if err != nil {
						return err
					}
					for k != nil  {
						//fmt.Println("skip", len(k), common.Bytes2Hex(k))
						k, v, err = mainCursor.Next()
						if err != nil {
							return err
						}
					}
				}
				if cmp >= 0 {
					hAccAddr0 := hAccAddr
					for hAccAddr != nil && (bytes.Equal(hAccAddr0, hAccAddr) || binary.BigEndian.Uint64(aTsEnc) < toBlock) {
						hAccAddr, aTsEnc, _, ahV, err = ahCursor.Next()
						if err != nil {
							return err
						}
					}
				}
			}

		} else if len(k)==60 {
			addr:=k[:common.AddressLength]
			loc:=k[common.AddressLength+common.IncarnationLength:]
			//incarnationBytes:=k[common.AddressLength:common.AddressLength+common.IncarnationLength]
			//incarnation:=binary.BigEndian.Uint64(incarnationBytes)

			cmp, br := common.KeyCmp(addr, hStorageAddr)
			if br {
				break
			}
			if cmp == 0 {
				cmp, br = common.KeyCmp(loc, hStorageLoc)
			}
			if br {
				break
			}

			//next key in state
			if cmp < 0 {
				//goOn, err = walker(addr, loc, v)
				fmt.Println("str cmp<0", common.Bytes2Hex(k), len(v))
			} else {
				index := roaring64.New()
				if _, err = index.ReadFrom(bytes.NewReader(hStorageV)); err != nil {
					return err
				}
				found, ok := bitmapdb.SeekInBitmap64(index, toBlock)
				changeSetBlock := found

				if ok {
					// Extract value from the changeSet
					csKey := make([]byte, 8+common.AddressLength+common.IncarnationLength)
					copy(csKey[:], dbutils.EncodeBlockNumber(changeSetBlock))
					copy(csKey[8:], hStorageAddr[:]) // address + incarnation
					//todo remove default incarnation
					binary.BigEndian.PutUint64(csKey[8+common.AddressLength:], changeset.DefaultIncarnation)
					kData, data, err3 := scsCursor.SeekBothRange(csKey, hStorageLoc)
					if err3 != nil {
						return err3
					}
					if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hStorageLoc) {
						return fmt.Errorf("inconsistent storage changeset and history kData %x, csKey %x, data %x, hLoc %x", kData, csKey, data, hLoc)
					}
					data = data[common.HashLength:]
					if len(data) > 0 { // Skip deleted entries
						//goOn, err = walker(hAddr, hLoc, data)
						fmt.Println("cs data>=0", common.Bytes2Hex(hStorageAddr), common.Bytes2Hex(hStorageLoc))
					}
				} else if cmp == 0 {
					fmt.Println("cmp==0", common.Bytes2Hex(k))
					//goOn, err = walker(addr, loc, v)
				}
			}
			if err != nil {
				return err
			}
			if goOn {
				if cmp <= 0 {
					if k, v, err = mainCursor.Next(); err != nil {
						return err
					}
				}
				if cmp >= 0 {
					hLoc0 := hStorageLoc
					for hLoc != nil && (bytes.Equal(hLoc0, hStorageLoc) || binary.BigEndian.Uint64(tsStorageEnc) < toBlock) {
						if hAddr, hLoc, tsEnc, hV, err2 = hCursor.Next(); err2 != nil {
							return err2
						}
					}
				}
			}

		} else {
			fmt.Println("incorrect key", common.Bytes2Hex(k))
		}
	}
	return err
	//for i:=0; i<1000; i++ {
	//	k,v,err:=mainCursor.Next()
	//	fmt.Println(len(k), common.Bytes2Hex(k), common.Bytes2Hex(v), err)
	//
	//}
	//state.WalkAsOfAccounts()
	//state.WalkAsOfStorage()
	//i := 0
	//t := time.Now()
	//tt := time.Now()
	//err = state.WalkAsOfAccounts(tx, common.Address{}, toBlock+1, func(k []byte, v []byte) (bool, error) {
	//	i++
	//	if i%100000 == 0 {
	//		fmt.Println(i, common.Bytes2Hex(k), "batch", time.Since(tt))
	//		tt = time.Now()
	//		select {
	//		case <-ctx.Done():
	//			return false, errors.New("interrupted")
	//		default:
	//
	//		}
	//	}
	//	if len(k) != 20 {
	//		return true, nil
	//	}
	//
	//	var acc accounts.Account
	//	if err = acc.DecodeForStorage(v); err != nil {
	//		return false, fmt.Errorf("decoding %x for %x: %v", v, k, err)
	//	}

	//	if acc.Incarnation > 0 {
	//		storagePrefix := dbutils.PlainGenerateStoragePrefix(k, acc.Incarnation)
	//		if acc.IsEmptyRoot() {
	//			t := trie.New(common.Hash{})
	//			j := 0
	//			innerErr := state.WalkAsOfStorage(tx2, common.BytesToAddress(k), acc.Incarnation, common.Hash{}, toBlock+1, func(k1, k2 []byte, vv []byte) (bool, error) {
	//				j++
	//				innerErr1 := mt.Put(dbutils.PlainStateBucket, dbutils.PlainGenerateCompositeStorageKey(k1, acc.Incarnation, k2), common.CopyBytes(vv))
	//				if innerErr1 != nil {
	//					return false, innerErr1
	//				}
	//
	//				h, _ := common.HashData(k1)
	//				t.Update(h.Bytes(), common.CopyBytes(vv))
	//
	//				return true, nil
	//			})
	//			if innerErr != nil {
	//				return false, innerErr
	//			}
	//			acc.Root = t.Hash()
	//		}
	//
	//		if acc.IsEmptyCodeHash() {
	//			codeHash, err1 := tx2.GetOne(dbutils.PlainContractCodeBucket, storagePrefix)
	//			if err1 != nil && errors.Is(err1, ethdb.ErrKeyNotFound) {
	//				return false, fmt.Errorf("getting code hash for %x: %v", k, err1)
	//			}
	//			if len(codeHash) > 0 {
	//				code, err1 := tx2.GetOne(dbutils.CodeBucket, codeHash)
	//				if err1 != nil {
	//					return false, err1
	//				}
	//				if err1 = mt.Put(dbutils.CodeBucket, codeHash, code); err1 != nil {
	//					return false, err1
	//				}
	//				if err1 = mt.Put(dbutils.PlainContractCodeBucket, storagePrefix, codeHash); err1 != nil {
	//					return false, err1
	//				}
	//			}
	//		}
	//	}
	//	newAcc := make([]byte, acc.EncodingLengthForStorage())
	//	acc.EncodeForStorage(newAcc)
	//	innerErr := mt.Put(dbutils.PlainStateBucket, common.CopyBytes(k), newAcc)
	//	if innerErr != nil {
	//		return false, innerErr
	//	}
	//
	//	if mt.BatchSize() >= mt.IdealBatchSize() {
	//		ttt := time.Now()
	//		innerErr = mt.CommitAndBegin(context.Background())
	//		if innerErr != nil {
	//			return false, innerErr
	//		}
	//		fmt.Println("Committed", time.Since(ttt))
	//	}
	//	return true, nil
	//})
	//if err != nil {
	//	return err
	//}
	//_, err = mt.Commit()
	//if err != nil {
	//	return err
	//}
	//fmt.Println("took", time.Since(t))
	//
	return nil
	//return VerifyStateSnapshot(ctx, dbPath, snapshotFile, block)
}
