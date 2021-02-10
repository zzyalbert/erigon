// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"encoding/binary"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
)

func TestFastSyncDisabling64(t *testing.T) { testFastSyncDisabling(t, 64) }
func TestFastSyncDisabling65(t *testing.T) { testFastSyncDisabling(t, 65) }

// Tests that fast sync gets disabled as soon as a real block is successfully
// imported into the blockchain.
func testFastSyncDisabling(t *testing.T, protocol int) {
	t.Skip("should be restored. skipped for turbo-geth")

	// Create a pristine protocol manager, check that fast sync is left enabled
	pmEmpty, clear := newTestProtocolManagerMust(t, downloader.FastSync, 0, nil, nil)
	defer clear()
	if atomic.LoadUint32(&pmEmpty.fastSync) == 0 {
		t.Fatalf("fast sync disabled on pristine blockchain")
	}
	// Create a full protocol manager, check that fast sync gets disabled
	pmFull, clearFull := newTestProtocolManagerMust(t, downloader.FastSync, 1024, nil, nil)
	defer clearFull()
	if atomic.LoadUint32(&pmFull.fastSync) == 1 {
		t.Fatalf("fast sync not disabled on non-empty blockchain")
	}

	// Sync up the two peers
	io1, io2 := p2p.MsgPipe()

	go pmFull.handle(pmFull.newPeer(protocol, p2p.NewPeer(enode.ID{}, "empty", nil), io2, pmFull.txpool.Get))   //nolint:errcheck
	go pmEmpty.handle(pmEmpty.newPeer(protocol, p2p.NewPeer(enode.ID{}, "full", nil), io1, pmEmpty.txpool.Get)) //nolint:errcheck

	time.Sleep(250 * time.Millisecond)
	op := peerToSyncOp(downloader.FastSync, pmEmpty.peers.BestPeer())
	if err := pmEmpty.doSync(op); err != nil {
		t.Fatal("sync failed:", err)
	}

	// Check that fast sync was disabled
	if atomic.LoadUint32(&pmEmpty.fastSync) == 1 {
		t.Fatalf("fast sync not disabled after successful synchronisation")
	}
}

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
	number := rawdb.ReadHeaderNumber(db, hash)
	body:=rawdb.ReadBody(db, hash, *number)
	b,err:=rlp.EncodeToBytes(body)
	if err!=nil {
		t.Fatal(err)
	}
	header:=rawdb.ReadHeaderRLP(db, hash, *number)

	t.Log("header",common.Bytes2Hex(header))
	t.Log("body",common.Bytes2Hex(b))

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