package debug

import (
	"context"
	"encoding/binary"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	trnt "github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"testing"
)

func TestCanonical(t *testing.T) {
	path1:="/media/b00ris/nvme/tmp/canonical1"
	path2:="/media/b00ris/nvme/tmp/canonical2"
	os.RemoveAll(path1)
	os.RemoveAll(path2)
	kv1:=ethdb.NewLMDB().Path(path1).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: {},
		}
	}).MustOpen()
	kv2:=ethdb.NewLMDB().Path(path2).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: {},
		}
	}).MustOpen()
	db1:=ethdb.NewObjectDatabase(kv1)
	db2:=ethdb.NewObjectDatabase(kv2)
	err:=db1.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(0, common.Hash{}), []byte{1})
	if err!=nil {
		t.Fatal(err)
	}
	err=db1.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{}), []byte{1})
	if err!=nil {
		t.Fatal(err)
	}
	err=db2.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(0, common.Hash{}), []byte{1})
	if err!=nil {
		t.Fatal(err)
	}
	err=db2.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{}), []byte{1})
	if err!=nil {
		t.Fatal(err)
	}

	db1.Close()
	db2.Close()
	_=db1
	_=db2
	t.Log(os.Remove(path1+"/LOCK"))
	t.Log(os.Remove(path1+"/lock.mdb"))
	t.Log(os.Remove(path2+"/LOCK"))
	t.Log(os.Remove(path2+"/lock.mdb"))

	info, err := trnt.BuildInfoBytesForLMDBSnapshot(path1)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(metainfo.HashBytes(infoBytes1))


	info2, err := trnt.BuildInfoBytesForLMDBSnapshot(path2)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes2, err := bencode.Marshal(info2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(metainfo.HashBytes(infoBytes2))

}
//301a763f9516b3605e0be39335e5df67eadc8ada
func TestHeadersCanonical(t *testing.T) {
	snapshotPath:="/media/b00ris/nvme/tmp/canonical1"
	dbPath:="/media/b00ris/nvme/fresh_sync/tg/chaindata/"
	toBlock:=uint64(11500000)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()

	snKV := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	db := ethdb.NewObjectDatabase(kv)
	k,_,err:=db.Last(dbutils.HeaderPrefix)
	if err!=nil{
		t.Fatal()
	}
	t.Log(common.Bytes2Hex(k))
	t.Log(binary.BigEndian.Uint64(k))

	if err==nil {
		t.Fatal()
	}
	snDB := ethdb.NewObjectDatabase(snKV)
	tx,err:=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var hash common.Hash
	var header []byte
	for i := uint64(1); i <= toBlock; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			t.Fatal(err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			t.Fatal(err)
		}

		err = tx.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(i, hash), header)
		if err != nil {
			t.Fatal(err)
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	snDB.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}
	err = os.Remove(snapshotPath + "/LOCK")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}

	info, err := trnt.BuildInfoBytesForLMDBSnapshot(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(metainfo.HashBytes(infoBytes1))

}

/*
   canonical_test.go:231: before b5dc25fa7ee38d5e7e1c3f3cd35f66b9cdac0a0e
   canonical_test.go:287: after rebuild d2d13a7aff295393bbb1b03b0c57c9fe6f7a2648
   canonical_test.go:347: from scratch d5e1a0f378f40d3680b67ed164c5834378c31c4b

    canonical_test.go:231: before b5dc25fa7ee38d5e7e1c3f3cd35f66b9cdac0a0e
    canonical_test.go:287: after rebuild d2d13a7aff295393bbb1b03b0c57c9fe6f7a2648
    canonical_test.go:347: from scratch d5e1a0f378f40d3680b67ed164c5834378c31c4b

    canonical_test.go:231: before b5dc25fa7ee38d5e7e1c3f3cd35f66b9cdac0a0e
    canonical_test.go:287: after rebuild d2d13a7aff295393bbb1b03b0c57c9fe6f7a2648
    canonical_test.go:347: from scratch d5e1a0f378f40d3680b67ed164c5834378c31c4b
 */
func TestAddHeadersToCanonical(t *testing.T) {
	snapshotPath:="/media/b00ris/nvme/tmp/canonical2"
	os.RemoveAll(snapshotPath)
	dbPath:="/media/b00ris/nvme/fresh_sync/tg/chaindata/"
	toBlock:=uint64(11500000)
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()
	db := ethdb.NewObjectDatabase(kv)

	snKV := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()
	snDB := ethdb.NewObjectDatabase(snKV)
	var hash common.Hash
	var header []byte
	var err error

	tx,err:=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= toBlock; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			t.Fatal(err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			t.Fatal(err)
		}

		err = tx.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(i, hash), header)
		if err != nil {
			t.Fatal(err)
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	snDB.Close()

	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}
	err = os.Remove(snapshotPath + "/LOCK")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}

	info, err := trnt.BuildInfoBytesForLMDBSnapshot(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("before", metainfo.HashBytes(infoBytes1))



	newHeight:=uint64(11700000)
	snKV = ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()
	snDB = ethdb.NewObjectDatabase(snKV)
	tx,err=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	for i := toBlock+1; i <= newHeight; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			t.Fatal(err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			t.Fatal(err)
		}

		err = tx.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(i, hash), header)
		if err != nil {
			t.Fatal(err)
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	snDB.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}
	err = os.Remove(snapshotPath + "/LOCK")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}

	info, err = trnt.BuildInfoBytesForLMDBSnapshot(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err = bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("after rebuild", metainfo.HashBytes(infoBytes1))




	err=os.RemoveAll(snapshotPath)
	if err!=nil {
		t.Fatal(err)
	}
	snKV = ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	snDB = ethdb.NewObjectDatabase(snKV)
	tx,err=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := uint64(1); i <= newHeight; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			t.Fatal(err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			t.Fatal(err)
		}

		err = tx.Append(dbutils.HeaderPrefix, dbutils.HeaderKey(i, hash), header)
		if err != nil {
			t.Fatal(err)
		}
	}
	_,err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	snDB.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}
	err = os.Remove(snapshotPath + "/LOCK")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}

	info, err = trnt.BuildInfoBytesForLMDBSnapshot(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err = bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("from scratch", metainfo.HashBytes(infoBytes1))

}