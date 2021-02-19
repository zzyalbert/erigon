package debug

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestDebugState(t *testing.T) {
	dbNew, err:=ethdb.Open("/media/b00ris/nvme/snsync_test/tg/chaindata/", false)
	if err != nil {
		t.Fatal(err)
	}

	addr := []byte{119, 218, 94, 108, 114, 251, 54, 188, 225, 217, 121, 143, 123, 205, 241, 209, 143, 69, 156, 46}
	t.Log(dbNew.Get(dbutils.PlainStateBucket, addr))
	//i:=0
	err = dbNew.ClearBuckets(dbutils.CurrentStateBucket, dbutils.ContractCodeBucket, dbutils.IntermediateTrieHashBucket)
	if err!=nil {
		t.Fatal()
	}

	if err := stages.SaveStageProgress(dbNew, stages.IntermediateHashes, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageUnwind(dbNew, stages.IntermediateHashes, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageProgress(dbNew, stages.HashState, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageUnwind(dbNew, stages.HashState, 0); err != nil {
		t.Fatal(err)
	}
	//dbNew.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println(i, common.Bytes2Hex(k))
	//	i++
	//	return true, nil
	//})
	/*
		dht server on [::]:44419: falling back on starting nodes
		INFO [01-31|14:31:31.589] [6/14 HashState] ETL [1/2] Extracting    from=PLAIN-SCS current key=0000000000b377a927054b13b1b798b345b591a4d22e6562d47ea75a0000000000000001 alloc=561.84MiB sys=1.51GiB numGC=128
		INFO [01-31|14:31:42.382] [6/14 HashState] DONE                    in=1m40.546349661s
		INFO [01-31|14:31:42.382] [7/14 IntermediateHashes] Generating intermediate hashes from=11750432  to=11763530
		INFO [01-31|14:32:38.527] [7/14 IntermediateHashes] Calculating Merkle root current key=09040407...
		curr: 0f0f0f0f0f0302040d0a0a0c0d0906080a0a0a050a0b0f0b070b070d09080703090d000a0d0e0f01000f08080000020109050b040c0c0a00090301010f0f0f0210, succ: 0f0f0f0f0f0302040d0a0a0c0d0906080a0a0a050a0b0f0b070b070d09080703090d000a0d0e0f01000f08080000020109050b040c0c0a00090301010f0f0f0210, maxLen 65, groups: [111111111111111 111111111111111 111111111111111 111111111111111 111111111111111 111], precLen: 5, succLen: 65, buildExtensions: false
		panic: runtime error: index out of range [65] with length 65

		goroutine 47512 [running]:
		github.com/ledgerwatch/turbo-geth/turbo/trie.GenStructStep(0xc0344b7950, 0xc01337c750, 0x41, 0xc1, 0xc02810f290, 0x41, 0x81, 0x16fc640, 0xc00415b200, 0xc006a70d90, ...)
			github.com/ledgerwatch/turbo-geth/turbo/trie/gen_struct_step.go:120 +0x10fe
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*RootHashAggregator).genStructAccount(0xc0015f8a00, 0x50, 0x40)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:705 +0x1ea
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*RootHashAggregator).Receive(0xc0015f8a00, 0xc0344b7b01, 0xc046a681c0, 0x40, 0x40, 0x0, 0x0, 0x0, 0xc0111a1800, 0x0, ...)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:511 +0x985
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*FlatDBTrieLoader).CalcTrieRoot(0xc0111a1680, 0x1700e20, 0xc043602050, 0xc0053358c0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:406 +0x54a
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.incrementIntermediateHashes(0xc032fa4560, 0x17, 0xc01c3ba090, 0x1700e20, 0xc043602050, 0xb37f4a, 0x1, 0xc00021b380, 0x27, 0xe35d3d592a009418, ...)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stage_interhashes.go:280 +0x76c
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.SpawnIntermediateHashesStage(0xc01c3ba090, 0x1700d80, 0xc00020eb10, 0x12ac301, 0xc00021b380, 0x27, 0xc0053358c0, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stage_interhashes.go:63 +0x82a
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.DefaultStages.func7.1(0xc01c3ba090, 0x16c8b60, 0xc00417b440, 0x12, 0x16efba0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stagebuilder.go:219 +0x65
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.(*State).runStage(0xc00417b440, 0xc00694bdb0, 0x16efba0, 0xc00020eb10, 0x16efba0, 0xc00020eb10, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/state.go:224 +0x155
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.(*State).Run(0xc00417b440, 0x16f5b80, 0xc00020eb10, 0x16f5b80, 0xc00020eb10, 0xc0002fc828, 0x1700d80)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/state.go:200 +0x348
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).syncWithPeer(0xc00034a000, 0xc0078ad680, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0xc000236fc0, 0xc013742c30, 0x0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:612 +0xc68
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).synchronise(0xc00034a000, 0xc0047fce70, 0x10, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0x1, 0xc000236fc0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:461 +0x3a5
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).Synchronise(0xc00034a000, 0xc0047fce70, 0x10, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0xc000000001, 0xc000236fc0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:371 +0xa6
		github.com/ledgerwatch/turbo-geth/eth.(*ProtocolManager).doSync(0xc001344500, 0xc013a8be80, 0x2193040, 0x16ec7a0)
			github.com/ledgerwatch/turbo-geth/eth/sync.go:315 +0x10f
		github.com/ledgerwatch/turbo-geth/eth.(*chainSyncer).startSync.func1(0xc003eb3320, 0xc013a8be80)
			github.com/ledgerwatch/turbo-geth/eth/sync.go:286 +0x38
		created by github.com/ledgerwatch/turbo-geth/eth.(*chainSyncer).startSync
			github.com/ledgerwatch/turbo-geth/eth/sync.go:286 +0x76


	*/
}
func TestDebug2(t *testing.T) {
	//46147
	db, err := ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}

	hash:=common.HexToHash(snapshotsync.HeaderHash11kk)
	t.Log(hash.String())
	number := rawdb.ReadHeaderNumber(db, hash)
	t.Log(*number)
	body:=rawdb.ReadBody(db, hash, *number)
	b,err:=rlp.EncodeToBytes(body)
	if err!=nil {
		t.Fatal(err)
	}
	headerBytes:=rawdb.ReadHeaderRLP(db, hash, *number)
	header:=new(types.Header)
	err = rlp.DecodeBytes(headerBytes, header)
	if err!=nil {
		t.Fatal(err)
	}
	t.Log("hash", header.Number.Uint64(), header.Hash().String())
	t.Log("header",common.Bytes2Hex(headerBytes))
	t.Log("body",common.Bytes2Hex(b))

}
func TestDebug3(t *testing.T) {
	//46147
	db, err := ethdb.Open("/media/b00ris/nvme/snsync_test/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}

	hash:=common.HexToHash(snapshotsync.HeaderHash11kk)
	t.Log(hash.String())
	number := rawdb.ReadHeaderNumber(db, hash)
	header:=rawdb.ReadHeaderByNumber(db, *number)
	spew.Dump(header)
	t.Log(header.Hash().String())
}

func TestDebug(t *testing.T) {
	//46147
	db, err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}

	dbNew, err:=ethdb.Open("/media/b00ris/nvme/snsync_test/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	bKv, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path("/media/b00ris/nvme/snsync_test/tg/snapshots" + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_bodies]
	}).Open()
	dbSn:=ethdb.NewObjectDatabase(bKv)


	snapshotKV, innerErr := snapshotsync.WrapBySnapshotsFromDir(dbNew.KV(),"/media/b00ris/nvme/snsync_test/tg/snapshots/", snapshotsync.SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    true,
		Receipts: false,
	})
	if innerErr != nil {
		t.Fatal(err)
	}
	dbNew.SetKV(snapshotKV)

	blockNumber :=uint64(46147)

	hash, err:=rawdb.ReadCanonicalHash(db, blockNumber)
	if err != nil {
		t.Fatal(err)
	}
	hash2, err:=rawdb.ReadCanonicalHash(dbNew, blockNumber)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(hash.String())
	fmt.Println(hash2.String())

	bb1:=rawdb.ReadStorageBodyRLP(db, hash, blockNumber)
	bR1:=new(types.BodyForStorage)
	err = rlp.DecodeBytes(bb1,bR1)
	if err != nil {
		t.Fatal(err)
	}


	spew.Dump(bR1)

	bl1:=rawdb.ReadBody(db, hash, blockNumber)
	bl2:=rawdb.ReadBody(dbNew, hash, blockNumber)
	bl3:=rawdb.ReadBody(dbSn, hash, blockNumber)
	fmt.Println(reflect.DeepEqual(bl1, bl2), reflect.DeepEqual(bl1, bl3))

	spew.Dump(bl1)
	spew.Dump(bl3)
	spew.Dump(bl2)

	k,_,err:=db.Last(dbutils.EthTx)
	fmt.Println(binary.BigEndian.Uint64(k))
	k,_,err = dbSn.Last(dbutils.EthTx)
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println(binary.BigEndian.Uint64(k))

	hash3, err:=rawdb.ReadCanonicalHash(dbNew, 11000000)
	if err != nil {
		t.Fatal(err)
	}

	bb2:=rawdb.ReadStorageBodyRLP(dbNew, hash3, 11000000)
	bR2:=new(types.BodyForStorage)
	err = rlp.DecodeBytes(bb2,bR2)
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(bR2)



	mem:=ethdb.NewMemDatabase()
	snapshotKV, innerErr = snapshotsync.WrapBySnapshotsFromDir(mem.KV(),"/media/b00ris/nvme/snsync_test/tg/snapshots/", snapshotsync.SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    true,
		Receipts: false,
	})
	if innerErr != nil {
		t.Fatal(err)
	}
	mem.SetKV(snapshotKV)

	k,_,err = mem.Last(dbutils.EthTx)
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("k", k)
	fmt.Println(binary.BigEndian.Uint64(k), err)

	/*
		858581106
		858581106
		(*types.BodyForStorage)(0xc00036de30)({
		BaseTxId: (uint64) 858580934,
				TxAmount: (uint32) 173,
				Uncles: ([]*types.Header) {
			}
		})
	*/
	//b:=rawdb.ReadHeaderRLP(db, hash, 11_000_000)
	//t.Log(hash.String(), len(b))
	//h:=new(types.Header)
	//err = rlp.DecodeBytes(b, h)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//fmt.Println("-----")
	//fmt.Println(common.Bytes2Hex(b))
	//fmt.Println("-----")
	//fmt.Println(db.Get(dbutils.HeaderPrefix, dbutils.HeaderTDKey(11_000_000, hash)))
	//spew.Dump(h)


}

func TestDebug5(t *testing.T) {
	//46147
	kv, err := ethdb.NewLMDB().Path("/media/b00ris/nvme/snapshots/headers/").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
			dbutils.HeadersSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Flags(func(u uint) uint {
		return u | lmdb.Readonly
	}).Open()
	if err != nil {
		t.Fatal(err)
	}

	kv2:=ethdb.NewSnapshot2KV().DB(ethdb.NewLMDB().InMem().MustOpen()).SnapshotDB([]string{dbutils.HeaderPrefix}, kv).MustOpen()
	db:=ethdb.NewObjectDatabase(kv2)
	var lastHash common.Hash
	num:=1
	for j:=0; j<5; j++ {
		err=db.Walk(dbutils.HeaderPrefix, []byte{}, 0, func(k, v []byte) (bool, error) {
			header:=new(types.Header)
			err:=rlp.DecodeBytes(v, header)
			if err!=nil {
				return false, err
			}
			for i:=0; i<=num; i++ {
				lastHash = header.Hash()
			}
			return true, nil
		})
		if err!=nil {
			t.Fatal(err)
		}

	}
	fmt.Println(lastHash.String())
}
/*
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (126.46s)
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (245.59s)

=== RUN   TestDebug5
MustOpen
0 {[h] 0xc0004c8080}
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (101.84s)

0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (92.76s)


0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (457.69s)
*/

// 0

func TestCheckValue(t *testing.T) {
	//46147
	db, err := ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	numOfEmpty:=0
	i:=1000000
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if len(k)==20 {
			return true, nil
		}
		if len(v)==0 {
			numOfEmpty++
		}
		i--
		if i==0 {
			fmt.Println(len(k),common.Bytes2Hex(k), len(v))
			i=1000000
		}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(numOfEmpty)
}

func TestWalk(t *testing.T) {
	db, err := ethdb.Open("/media/b00ris/nvme/tmp/debug", true)
	kv := db.KV()
	mode:=snapshotsync.SnapshotMode{
		State: true,
	}

	kv, err = snapshotsync.WrapBySnapshotsFromDir(kv, "/media/b00ris/nvme/snapshots/", mode)
	if err != nil {
		t.Fatal(err)
	}

	db.SetKV(kv)

	err  = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println(common.Bytes2Hex(k))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

}

func TestSizesCheck(t *testing.T) {
	//46147
	db, err := ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	m:=make(map[int]uint64)
	i:=1000000
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if len(k)==20 {
			return true, nil
		}
		m[len(v)]++
		i--
		if i==0 {
			fmt.Println(len(k),common.Bytes2Hex(k), len(v))
			i=1000000
		}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(m)
	val, err:=json.Marshal(m)
	fmt.Println(val)
	fmt.Println(err)
}

func TestTxLookupData(t *testing.T) {
	db, err := ethdb.Open("/media/b00ris/nvme/sync115/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
	}).MustOpen()
	kvb:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/bodies/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_bodies]
	}).MustOpen()
	tx,err:=kvb.Begin(context.Background(), nil, ethdb.RO)
	if err!=nil {
		t.Fatal(err)
	}

	v,err:=tx.GetOne(dbutils.EthTx, dbutils.EncodeBlockNumber(944576795))
	if err!=nil {
		t.Fatal(err)
	}
	txs:=new(types.Transaction)
	if err := rlp.DecodeBytes(v, txs); err != nil {
		t.Fatal(err)
	}

	fmt.Println("i", txs.Hash().String())
/*
   0x82554bfa38315263192106dd390b97b698f40b1d134eda048fc43bbe0448676b 11500000
   944576795 189
   944576795 0x82554bfa38315263192106dd390b97b698f40b1d134eda048fc43bbe0448676b

   0x82554bfa38315263192106dd390b97b698f40b1d134eda048fc43bbe0448676b 11500000
   944576795 189
   944576795 0x82554bfa38315263192106dd390b97b698f40b1d134eda048fc43bbe0448676b
 */

	tx.Rollback()
	t.Fatal()
	snkv:=ethdb.NewSnapshot2KV().DB(db.KV()).SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, kv).SnapshotDB([]string{dbutils.BlockBodyPrefix, dbutils.EthTx}, kvb).MustOpen()
	db.SetKV(snkv)
	mp:=make(map[string]int)
	startKey:=dbutils.HeaderKey(11500000, common.Hash{})
	err = db.Walk(dbutils.HeaderPrefix, startKey, 0, func(k, v []byte) (bool, error) {
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadBody(db, blockHash, blocknum)
		if body == nil {
			return false, fmt.Errorf("tx lookup generation, empty block body %d, hash %x",  blocknum, v)
		}

		fmt.Println("block", blocknum, len(body.Transactions), len(mp))
		for _, tx := range body.Transactions {
			fmt.Println(tx.Hash().String(), blocknum)
			mp[tx.Hash().String()]++
		}

		if blocknum>11500000 {
			return false, nil
		}

		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("checkDupl")
	for k,v:=range  mp {
		if v>1 {
			fmt.Println("duplicate", k, v)
		}
	}
}
func TestEthTx(t *testing.T) {
	db, err := ethdb.Open("/media/b00ris/nvme/sync115/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	//kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
	//	return u|lmdb.Readonly
	//}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	//	return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
	//}).MustOpen()
	//kvb:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/bodies/").Flags(func(u uint) uint {
	//	return u|lmdb.Readonly
	//}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	//	return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_bodies]
	//}).MustOpen()

	//snkv:=ethdb.NewSnapshot2KV().DB(db.KV()).SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, kv).SnapshotDB([]string{dbutils.BlockBodyPrefix}, kvb).MustOpen()
	//db.SetKV(snkv)
	//mp:=make(map[string]int)
	//startKey:=dbutils.HeaderKey(11499998, common.Hash{})
	i:=0
	err = db.Walk(dbutils.EthTx, []byte{}, 0, func(k, v []byte) (bool, error) {
		//fmt.Println(binary.BigEndian.Uint64(k), v)
		if i>100 {
			return false, nil
		}
		i++
		txs:=new(types.Transaction)
		if err := rlp.DecodeBytes(v, txs); err != nil {
			return false, fmt.Errorf("broken tx rlp: %w", err)
		}
		fmt.Println(i, txs.Hash().String())
		i++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
}



func TestName(t *testing.T) {
	snapshotDir:="/media/b00ris/nvme/snapshots/state115"
	chaindataDir:="/media/b00ris/nvme/fresh_sync/tg/chaindata"
	tmpDbDir:="/media/b00ris/nvme/tmp/debug2"
	tmpDbDir2:="/media/b00ris/nvme/tmp/debug3"
	kv:=ethdb.NewLMDB().Path(snapshotDir).Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_state]
	}).MustOpen()

	chaindata,err:=ethdb.Open(chaindataDir, true)
	if err!=nil {
		t.Fatal(err)
	}
	//tmpDb:=ethdb.NewMemDatabase()
	os.RemoveAll(tmpDbDir)
	os.RemoveAll(tmpDbDir2)
	tmpDb,err:=ethdb.Open(tmpDbDir, false)
	if err!=nil {
		t.Fatal(err)
	}
	snkv:=ethdb.NewSnapshot2KV().DB(tmpDb.KV()).SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.CodeBucket, dbutils.ContractCodeBucket}, kv).SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.KV()).MustOpen()
	db:=ethdb.NewObjectDatabase(snkv)

	tx,err:=db.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	blockNum:=uint64(11500001)
	limit:=uint64(1000)
	blockchain, err := core.NewBlockChain(tx, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: true,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(tx)
	cc.SetEngine(ethash.NewFaker())

	stateReaderWriter := NewDebugReaderWriter(state.NewPlainStateReader(tx), state.NewPlainStateWriter(tx, tx,blockNum))
	tt1:=time.Now()
	for i:=blockNum; i<blockNum+limit; i++ {
		fmt.Println("exsecuted", i)
		stateReaderWriter.UpdateWriter(state.NewPlainStateWriter(tx, tx, i))

		block, err:=rawdb.ReadBlockByNumber(chaindata, i)
		if err!=nil {
			t.Fatal(err)
		}
		_, err = core.ExecuteBlockEphemerally(blockchain.Config(), blockchain.GetVMConfig(), cc, cc.Engine(), block, stateReaderWriter, stateReaderWriter)
		if err != nil {
			t.Fatal(err)
		}

	}
	tx.Rollback()
	tt2:=time.Now()
	fmt.Println("End")
	spew.Dump("readAcc",len(stateReaderWriter.readAcc))
	spew.Dump("readStr",len(stateReaderWriter.readStorage))
	spew.Dump("createdContracts", len(stateReaderWriter.createdContracts))
	spew.Dump("deleted",len(stateReaderWriter.deletedAcc))

	//checkDB:=ethdb.NewMemDatabase()
	checkDB,err:=ethdb.Open(tmpDbDir2, false)
	if err!=nil {
		t.Fatal(err)
	}


	tt3:=time.Now()
	tx,err=checkDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	accs:=stateReaderWriter.AllAccounts()
	for i:=range accs {
		v, err:=db.Get(dbutils.PlainStateBucket, i.Bytes())
		if err==nil && len(v)>0 {
			innerErr:=tx.Put(dbutils.PlainStateBucket, i.Bytes(), v)
			if innerErr!=nil {
				t.Fatal(innerErr)
			}
		}
	}
	for i:=range stateReaderWriter.readStorage {
		v, err:=db.Get(dbutils.PlainStateBucket, []byte(i))
		if err==nil && len(v)>0 {
			innerErr:=tx.Put(dbutils.PlainStateBucket, []byte(i), v)
			if innerErr!=nil {
				t.Fatal(innerErr)
			}
		}
	}
	for i:=range stateReaderWriter.readCodes {
		v, err:=db.Get(dbutils.CodeBucket, i.Bytes())
		if err==nil && len(v)>0 {
			innerErr := tx.Put(dbutils.CodeBucket, i.Bytes(), v)
			if innerErr != nil {
				t.Fatal(innerErr)
			}
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	tt4:=time.Now()
	checkSnkv:=ethdb.NewSnapshot2KV().DB(checkDB.KV()).SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.KV()).MustOpen()
	checkDB.SetKV(checkSnkv)

	tx,err=checkDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	blockchain, err = core.NewBlockChain(tx, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: true,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	checkContext := &core.TinyChainContext{}
	checkContext.SetDB(tx)
	checkContext.SetEngine(ethash.NewFaker())

	for i:=blockNum; i<blockNum+limit; i++ {
		fmt.Println("checked", i)
		block, err:=rawdb.ReadBlockByNumber(chaindata, i)
		if err!=nil {
			t.Fatal(err)
		}
		_, err = core.ExecuteBlockEphemerally(blockchain.Config(), blockchain.GetVMConfig(), checkContext, cc.Engine(), block, state.NewPlainStateReader(tx),  state.NewPlainStateWriter(tx, tx,i))
		if err != nil {
			t.Fatal(err)
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	t.Log("exsecution:", tt2.Sub(tt1))
	t.Log("writing to temp db:", tt4.Sub(tt3))
	t.Log("checking:", time.Since(tt4))



}


var _ state.StateReader = &DebugReaderWriter{}
var _ state.WriterWithChangeSets = &DebugReaderWriter{}

func NewDebugReaderWriter(r state.StateReader, w state.WriterWithChangeSets) *DebugReaderWriter {
	return &DebugReaderWriter{
		r:   r,
		w: w,
		readAcc: make(map[common.Address]struct{}),
		readStorage: make(map[string]struct{}),
		readCodes: make(map[common.Hash]struct{}),
		readIncarnations: make(map[common.Address]struct{}),

		updatedAcc: make(map[common.Address]struct{}),
		updatedStorage:make(map[string]struct{}),
		updatedCodes: make(map[common.Hash]struct{}),
		deletedAcc: make(map[common.Address]struct{}),
		createdContracts: make(map[common.Address]struct{}),


	}
}
type DebugReaderWriter struct {
	r state.StateReader
	w state.WriterWithChangeSets
	readAcc map[common.Address]struct{}
	readStorage map[string]struct{}
	readCodes map[common.Hash] struct{}
	readIncarnations map[common.Address] struct{}
	updatedAcc map[common.Address]struct{}
	updatedStorage map[string]struct{}
	updatedCodes map[common.Hash]struct{}
	deletedAcc map[common.Address]struct{}
	createdContracts map[common.Address]struct{}
}
func (d *DebugReaderWriter) UpdateWriter(w state.WriterWithChangeSets) {
	d.w = w
}

func (d *DebugReaderWriter) ReadAccountData(address common.Address) (*accounts.Account, error) {
	d.readAcc[address] = struct{}{}
	return d.r.ReadAccountData(address)
}

func (d *DebugReaderWriter) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	d.readStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.r.ReadAccountStorage(address, incarnation, key)
}

func (d *DebugReaderWriter) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	d.readCodes[codeHash] = struct{}{}
	return d.r.ReadAccountCode(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return d.r.ReadAccountCodeSize(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountIncarnation(address common.Address) (uint64, error) {
	d.readIncarnations[address] = struct{}{}
	return d.r.ReadAccountIncarnation(address)
}

func (d *DebugReaderWriter) WriteChangeSets() error {
	return d.w.WriteChangeSets()
}

func (d *DebugReaderWriter) WriteHistory() error {
	return d.w.WriteHistory()
}

func (d *DebugReaderWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	d.updatedAcc[address] = struct{}{}
	return d.w.UpdateAccountData(ctx, address, original, account)
}

func (d *DebugReaderWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	d.updatedCodes[codeHash] = struct{}{}
	return d.w.UpdateAccountCode(address, incarnation, codeHash, code)
}

func (d *DebugReaderWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	d.deletedAcc[address]= struct{}{}
	return d.w.DeleteAccount(ctx, address, original)
}

func (d *DebugReaderWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	d.updatedStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.w.WriteAccountStorage(ctx, address, incarnation, key, original, value)
}

func (d *DebugReaderWriter) CreateContract(address common.Address) error {
	d.createdContracts[address] = struct{}{}
	return d.w.CreateContract(address)
}

func (d *DebugReaderWriter) AllAccounts() map[common.Address]struct{}  {
	accs:=make(map[common.Address]struct{})
	for i:=range d.readAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.updatedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.readIncarnations {
		accs[i]=struct{}{}
	}
	for i:=range d.deletedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.createdContracts {
		accs[i]=struct{}{}
	}
	return accs
}
func (d *DebugReaderWriter) AllStorage() map[string]struct{}  {
	st:=make(map[string]struct{})
	for i:=range d.readStorage {
		st[i]=struct{}{}
	}
	for i:=range d.updatedStorage {
		st[i]=struct{}{}
	}
	return st
}
func (d *DebugReaderWriter) AllCodes() map[common.Hash]struct{}  {
	c:=make(map[common.Hash]struct{})
	for i:=range d.readCodes {
		c[i]=struct{}{}
	}
	for i:=range d.updatedCodes {
		c[i]=struct{}{}
	}
	return c
}