package ethdb

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	require "github.com/stretchr/testify/require"
	"pgregory.net/rapid"
	"testing"
)

/*boris$ go test ./ethdb/ -run "TestCursorMDBXvsLMDBMachine" -rapid.checks 15000 -tags "mdbx" -v
=== RUN   TestCursorMDBXvsLMDBMachine
mdb.c:5701: Assertion 'root > 1' failed in mdb_page_search()
SIGABRT: abort
PC=0x7fff6c277b66 m=3 sigcode=0
signal arrived during cgo execution

goroutine 13 [syscall, locked to thread]:
runtime.cgocall(0x472eb40, 0xc0000b95f0, 0x4870c00)
        /usr/local/go/src/runtime/cgocall.go:154 +0x5b fp=0xc0000b95c0 sp=0xc0000b9588 pc=0x4004dfb
github.com/ledgerwatch/lmdb-go/lmdb._Cfunc_mdb_cursor_get(0x510d9d0, 0x510c8f0, 0x510d090, 0x8, 0xc000000000)
        _cgo_gotypes.go:509 +0x48 fp=0xc0000b95f0 sp=0xc0000b95c0 pc=0x4300528
github.com/ledgerwatch/lmdb-go/lmdb.(*Cursor).getVal0.func1(0xc0040f8bc0, 0x8, 0x5700100)
        /Users/boris/go/pkg/mod/github.com/ledgerwatch/lmdb-go@v1.17.4/lmdb/cursor.go:191 +0xd4 fp=0xc0000b9640 sp=0xc0000b95f0 pc=0x4309a14
github.com/ledgerwatch/lmdb-go/lmdb.(*Cursor).getVal0(0xc0040f8bc0, 0x8, 0xffff87b204003eba, 0xc0040e77e0)
        /Users/boris/go/pkg/mod/github.com/ledgerwatch/lmdb-go@v1.17.4/lmdb/cursor.go:191 +0x39 fp=0xc0000b9678 sp=0xc0000b9640 pc=0x4302859
github.com/ledgerwatch/lmdb-go/lmdb.(*Cursor).Get(0xc0040f8bc0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0xffff87b2, 0x1, ...)
        /Users/boris/go/pkg/mod/github.com/ledgerwatch/lmdb-go@v1.17.4/lmdb/cursor.go:149 +0x412 fp=0xc0000b96f8 sp=0xc0000b9678 pc=0x43027d2
github.com/ledgerwatch/turbo-geth/ethdb.(*LmdbCursor).next(...)
        /Users/boris/go/src/github.com/ledgerwatch/turbo-geth/ethdb/kv_lmdb.go:865
github.com/ledgerwatch/turbo-geth/ethdb.(*LmdbCursor).Next(0xc0040faa20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0)
        /Users/boris/go/src/github.com/ledgerwatch/turbo-geth/ethdb/kv_lmdb.go:1026 +0x65 fp=0xc0000b9860 sp=0xc0000b96f8 pc=0x4569345
github.com/ledgerwatch/turbo-geth/ethdb.(*cursorMDBXvsLMDBMachine).Next(0xc004048600, 0xc0005b6000)
        /Users/boris/go/src/github.com/ledgerwatch/turbo-geth/ethdb/mdbx_property_test.go:207 +0xcc fp=0xc0000b9940 sp=0xc0000b9860 pc=0x45a648c
runtime.call16(0xc0005aaea0, 0xc0040bb408, 0xc002bebee0, 0x1000000010)
        /usr/local/go/src/runtime/asm_amd64.s:550 +0x3e fp=0xc0000b9960 sp=0xc0000b9940 pc=0x4071a3e
reflect.callMethod(0xc0040fe080, 0xc0000b9a38, 0xc0000b9a20)
        /usr/local/go/src/reflect/value.go:733 +0x1f6 fp=0xc0000b9a08 sp=0xc0000b9960 pc=0x40dca76
reflect.methodValueCall(0xc0005b6000, 0xc0005e6300, 0x2c0000c0000b9a88, 0x4551e5b, 0x100000000000002, 0x43b42, 0xc0005b6000, 0xc0000b9aa8, 0x4960a28, 0xc0000b9aa9, ...)
        /usr/local/go/src/reflect/asm_amd64.s:39 +0x42 fp=0xc0000b9a38 sp=0xc0000b9a08 pc=0x40e7262
pgregory.net/rapid.runAction(0xc0005b6000, 0xc0040fe080, 0x4790000)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/statemachine.go:167 +0x83 fp=0xc0000b9a98 sp=0xc0000b9a38 pc=0x455a863
pgregory.net/rapid.(*stateMachine).executeAction(0xc0040a3140, 0xc0005b6000, 0xc00013aba0)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/statemachine.go:142 +0x125 fp=0xc0000b9af0 sp=0xc0000b9a98 pc=0x455a725
pgregory.net/rapid.Run.func1(0xc0005b6000)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/statemachine.go:76 +0x168 fp=0xc0000b9b68 sp=0xc0000b9af0 pc=0x455e908
pgregory.net/rapid.checkOnce(0xc0005b6000, 0xc0005c64b0, 0x0)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/engine.go:276 +0x77 fp=0xc0000b9b98 sp=0xc0000b9b68 pc=0x454f837
pgregory.net/rapid.findBug(0x4a31308, 0xc0005e6300, 0x3a98, 0x5bb61fa800000001, 0xc0005c64b0, 0xc0000544d0, 0x16d417b9, 0xc000056900, 0xc0000544e0)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/engine.go:248 +0x285 fp=0xc0000b9c68 sp=0xc0000b9b98 pc=0x454f2e5
pgregory.net/rapid.doCheck(0x4a31308, 0xc0005e6300, 0x0, 0x0, 0x3a98, 0x5bb61fa800000001, 0xc0005c64b0, 0x40, 0x66, 0x5651ff8, ...)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/engine.go:178 +0xc5 fp=0xc0000b9d70 sp=0xc0000b9c68 pc=0x454e525
pgregory.net/rapid.checkTB(0x4a31308, 0xc0005e6300, 0xc0005c64b0)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/engine.go:123 +0xd4 fp=0xc0000b9f30 sp=0xc0000b9d70 pc=0x454d494
pgregory.net/rapid.Check(0xc0005e6300, 0xc0005c64b0)
        /Users/boris/go/pkg/mod/pgregory.net/rapid@v0.4.6/engine.go:94 +0x51 fp=0xc0000b9f58 sp=0xc0000b9f30 pc=0x454d391
github.com/ledgerwatch/turbo-geth/ethdb.TestCursorMDBXvsLMDBMachine(0xc0005e6300)
        /Users/boris/go/src/github.com/ledgerwatch/turbo-geth/ethdb/mdbx_property_test.go:14 +0x55 fp=0xc0000b9f80 sp=0xc0000b9f58 pc=0x45a3ed5
testing.tRunner(0xc0005e6300, 0x495e9f8)
        /usr/local/go/src/testing/testing.go:1194 +0xef fp=0xc0000b9fd0 sp=0xc0000b9f80 pc=0x411e52f
runtime.goexit()
        /usr/local/go/src/runtime/asm_amd64.s:1371 +0x1 fp=0xc0000b9fd8 sp=0xc0000b9fd0 pc=0x4073481
created by testing.(*T).Run
        /usr/local/go/src/testing/testing.go:1239 +0x2b3

goroutine 1 [chan receive]:
testing.(*T).Run(0xc0005e6300, 0x493bc29, 0x1b, 0x495e9f8, 0x4095d01)
        /usr/local/go/src/testing/testing.go:1240 +0x2da
testing.runTests.func1(0xc0005e6180)
        /usr/local/go/src/testing/testing.go:1512 +0x78
testing.tRunner(0xc0005e6180, 0xc000211de0)
        /usr/local/go/src/testing/testing.go:1194 +0xef
testing.runTests(0xc0005c6498, 0x4ea2040, 0x19, 0x19, 0xc01f54b3e6cd5598, 0x8bb6479bcb, 0x4eaf220, 0x4931db7)
        /usr/local/go/src/testing/testing.go:1510 +0x2fe
testing.(*M).Run(0xc0005b6080, 0x0)
        /usr/local/go/src/testing/testing.go:1418 +0x1eb
main.main()
        _testmain.go:93 +0x138

rax    0x0
rbx    0x70000a3ef000
rcx    0x70000a3ee958
rdx    0x0
rdi    0x1103
rsi    0x6
rbp    0x70000a3ee990
rsp    0x70000a3ee958
r8     0x7fffa4b39048
r9     0x40
r10    0x0
r11    0x206
r12    0x1103
r13    0x510d9d0
r14    0x6
r15    0x2d
rip    0x7fff6c277b66
rflags 0x206
cs     0x7
fs     0x0
gs     0x0
FAIL    github.com/ledgerwatch/turbo-geth/ethdb 16.318s
FAIL
*/
//go test ./ethdb/ -run "TestCursorMDBXvsLMDBMachine" -rapid.checks 15000 -tags "mdbx" -v
func TestCursorMDBXvsLMDBMachine(t *testing.T) {
	t.Skip("remove when it become stable for 200 rounds")
	rapid.Check(t, rapid.Run(&cursorMDBXvsLMDBMachine{}))
}

type cursorMDBXvsLMDBMachine struct {
	bucket string
	lmdbKV RwKV
	mdbxKV RwKV

	lmdbTX RwTx
	mdbxTX RwTx

	lmdbCursor RwCursor
	mdbxCursor RwCursor

	snapshotKeys [][20]byte
	newKeys      [][20]byte
	allKeys      [][20]byte
}

func (m *cursorMDBXvsLMDBMachine) Init(t *rapid.T) {
	m.bucket = dbutils.PlainStateBucket
	m.lmdbKV = NewLMDB().InMem().MustOpen()
	m.mdbxKV = NewMDBX().InMem().MustOpen()
	m.snapshotKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate keys").([][20]byte)
	m.newKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate new keys").([][20]byte)
	notExistingKeys := rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate not excisting keys").([][20]byte)
	m.allKeys = append(m.snapshotKeys, notExistingKeys...)

	txLmdb, err := m.lmdbKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txLmdb.Rollback()

	txMdbx, err := m.mdbxKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txMdbx.Rollback()
	for _, key := range m.snapshotKeys {
		innerErr := txLmdb.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txMdbx.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}

	//save snapshot and wrap new write db
	err = txLmdb.Commit()
	require.NoError(t, err)
	err = txMdbx.Commit()
	require.NoError(t, err)

}

func (m *cursorMDBXvsLMDBMachine) Check(t *rapid.T) {
}

func (m *cursorMDBXvsLMDBMachine) Cleanup() {
	if m.lmdbTX != nil {
		m.lmdbTX.Rollback()
	}
	if m.mdbxTX != nil {
		m.mdbxTX.Rollback()
	}

	m.lmdbKV.Close()
	m.lmdbKV = nil
	m.lmdbTX = nil
	m.mdbxKV.Close()
	m.mdbxKV = nil
	m.mdbxTX = nil
}

func (m *cursorMDBXvsLMDBMachine) Begin(t *rapid.T) {
	if m.mdbxTX != nil && m.lmdbTX != nil {
		return
	}

	mtx, err := m.mdbxKV.BeginRw(context.Background())
	require.NoError(t, err)
	lmdbTx, err := m.lmdbKV.BeginRw(context.Background())
	require.NoError(t, err)
	m.mdbxTX = mtx
	m.lmdbTX = lmdbTx
}

func (m *cursorMDBXvsLMDBMachine) Rollback(t *rapid.T) {
	if m.mdbxTX == nil && m.lmdbTX == nil {
		return
	}
	m.lmdbTX.Rollback()
	m.mdbxTX.Rollback()
	m.lmdbTX = nil
	m.mdbxTX = nil
	m.lmdbCursor = nil
	m.mdbxCursor = nil
}

func (m *cursorMDBXvsLMDBMachine) Commit(t *rapid.T) {
	if m.mdbxTX == nil && m.lmdbTX == nil {
		return
	}
	err := m.mdbxTX.Commit()
	require.NoError(t, err)
	err = m.lmdbTX.Commit()
	require.NoError(t, err)
	m.lmdbTX = nil
	m.mdbxTX = nil
	m.lmdbCursor = nil
	m.mdbxCursor = nil
}

func (m *cursorMDBXvsLMDBMachine) Cursor(t *rapid.T) {
	if m.mdbxTX == nil && m.lmdbTX == nil {
		return
	}
	if m.mdbxCursor != nil && m.lmdbCursor != nil {
		return
	}
	var err error
	m.mdbxCursor, err = m.mdbxTX.RwCursor(m.bucket)
	require.NoError(t, err)
	m.lmdbCursor, err = m.lmdbTX.RwCursor(m.bucket)
	require.NoError(t, err)
}

func (m *cursorMDBXvsLMDBMachine) CloseCursor(t *rapid.T) {
	if m.mdbxTX == nil && m.lmdbTX == nil {
		return
	}
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}
	m.mdbxCursor.Close()
	m.lmdbCursor.Close()
	m.mdbxCursor = nil
	m.lmdbCursor = nil
}

func (m *cursorMDBXvsLMDBMachine) First(t *rapid.T) {
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}
	k1, v1, err1 := m.mdbxCursor.First()
	k2, v2, err2 := m.lmdbCursor.First()
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorMDBXvsLMDBMachine) Last(t *rapid.T) {
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}
	k1, v1, err1 := m.mdbxCursor.Last()
	k2, v2, err2 := m.lmdbCursor.Last()
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorMDBXvsLMDBMachine) Seek(t *rapid.T) {
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "get random key").([20]byte)
	k1, v1, err1 := m.mdbxCursor.Seek(key[:])
	k2, v2, err2 := m.lmdbCursor.Seek(key[:])
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorMDBXvsLMDBMachine) SeekExact(t *rapid.T) {
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}

	key := rapid.SampledFrom(m.allKeys).Draw(t, "get random key").([20]byte)
	k1, v1, err1 := m.mdbxCursor.SeekExact(key[:])
	k2, v2, err2 := m.lmdbCursor.SeekExact(key[:])
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorMDBXvsLMDBMachine) Next(t *rapid.T) {
	if m.mdbxCursor == nil && m.lmdbCursor == nil {
		return
	}
	k1, v1, err1 := m.mdbxCursor.Next()
	k2, v2, err2 := m.lmdbCursor.Next()

	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

//go test ./ethdb/ -run "TestMdxbGetAndPut" -rapid.checks 150 -tags "mdbx" -v
func TestMdxbGetAndPut(t *testing.T) {
	//t.Skip("remove when it become stable for 200 rounds")
	rapid.Check(t, rapid.Run(&getPutkvMachine{}))
}

type getPutkvMachine struct {
	bucket       string
	lmdbKV       RwKV
	mdbxKV       RwKV
	snapshotKeys [][20]byte
	newKeys      [][20]byte
	allKeys      [][20]byte

	snTX    RwTx
	modelTX RwTx
}

func (m *getPutkvMachine) Init(t *rapid.T) {
	m.bucket = dbutils.PlainStateBucket
	m.lmdbKV = NewLMDB().InMem().MustOpen()
	m.mdbxKV = NewMDBX().InMem().MustOpen()
	m.snapshotKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate keys").([][20]byte)
	m.newKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate new keys").([][20]byte)
	notExistingKeys := rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate not excisting keys").([][20]byte)
	m.allKeys = append(m.snapshotKeys, notExistingKeys...)

	lmdbTX, err := m.lmdbKV.BeginRw(context.Background())
	require.NoError(t, err)

	mdbxTX, err := m.mdbxKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer mdbxTX.Rollback()
	for _, key := range m.snapshotKeys {
		innerErr := lmdbTX.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = mdbxTX.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}

	//save snapshot and wrap new write db
	err = lmdbTX.Commit()
	require.NoError(t, err)
	err = mdbxTX.Commit()
	require.NoError(t, err)
}

func (m *getPutkvMachine) Cleanup() {
	if m.snTX != nil {
		m.snTX.Rollback()
	}
	if m.modelTX != nil {
		m.modelTX.Rollback()
	}
	m.lmdbKV.Close()
	m.mdbxKV.Close()
}

func (m *getPutkvMachine) Check(t *rapid.T) {
}

func (m *getPutkvMachine) Get(t *rapid.T) {
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "get a key").([20]byte)
	var (
		v1, v2     []byte
		err1, err2 error
	)

	v1, err1 = m.snTX.GetOne(m.bucket, key[:])
	v2, err2 = m.modelTX.GetOne(m.bucket, key[:])

	require.Equal(t, err1, err2)
	require.Equal(t, v1, v2)
}

func (m *getPutkvMachine) Put(t *rapid.T) {
	if len(m.newKeys) == 0 {
		return
	}
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.newKeys).Draw(t, "put a key").([20]byte)
	m.allKeys = append(m.allKeys, key)
	for i, v := range m.newKeys {
		if v == key {
			m.newKeys = append(m.newKeys[:i], m.newKeys[i+1:]...)
		}
	}
	err := m.snTX.Put(m.bucket, key[:], []byte("put"+common.Bytes2Hex(key[:])))
	require.NoError(t, err)

	err = m.modelTX.Put(m.bucket, key[:], []byte("put"+common.Bytes2Hex(key[:])))
	require.NoError(t, err)
}

func (m *getPutkvMachine) Delete(t *rapid.T) {
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "delete a key").([20]byte)

	err := m.snTX.Delete(m.bucket, key[:], nil)
	require.NoError(t, err)

	err = m.modelTX.Put(m.bucket, key[:], nil)
	require.NoError(t, err)
}

func (m *getPutkvMachine) Begin(t *rapid.T) {
	if m.modelTX != nil && m.snTX != nil {
		return
	}
	mtx, err := m.mdbxKV.BeginRw(context.Background())
	require.NoError(t, err)
	sntx, err := m.lmdbKV.BeginRw(context.Background())
	require.NoError(t, err)
	m.modelTX = mtx
	m.snTX = sntx
}

func (m *getPutkvMachine) Rollback(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	m.snTX.Rollback()
	m.modelTX.Rollback()
	m.snTX = nil
	m.modelTX = nil
}

func (m *getPutkvMachine) Commit(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	err := m.modelTX.Commit()
	require.NoError(t, err)
	err = m.snTX.Commit()
	require.NoError(t, err)
	m.snTX = nil
	m.modelTX = nil
}
