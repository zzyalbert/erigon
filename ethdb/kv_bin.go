package ethdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"io"
	"os"
	"sort"

	//"sort"
	"unsafe"
)

var (
	_ KV             = &BinKV{}
	_ Tx             = &BinTX{}
	_ Cursor         = &BinCursor{}
)

func NewBinKV(dir string) (*BinKV, error)  {
	index,err:=os.Open(dir+"/index.sn")
	if err != nil {
		return nil, err
	}
	b:=make([]byte, 8)
	_,err = index.Read(b)
	if err != nil {
		return nil, err
	}

	data,err:=os.Open(dir+"/data.sn")
	if err != nil {
		return nil, err
	}

	return &BinKV{
		index: index,
		data: data,
		numOfElements: binary.BigEndian.Uint64(b),
	}, nil
}

type BinKV struct {
	index *os.File
	data *os.File
	cache [][]byte
	numOfElements uint64
}

func (b *BinKV) View(ctx context.Context, f func(tx Tx) error) error {
	return f(&BinTX{kv:b, key: make([]byte,56)})
}

func (b *BinKV) Update(ctx context.Context, f func(tx Tx) error) error {
	return f(&BinTX{kv:b, key: make([]byte,56)})
}

func (b *BinKV) Close() {
	b.index.Close()
	b.data.Close()
}

func (b *BinKV) Begin(ctx context.Context, parent Tx, flags TxFlags) (Tx, error) {
	return &BinTX{kv: b, key: make([]byte,56)}, nil
}

func (b *BinKV) AllBuckets() dbutils.BucketsCfg {
	panic("implement me")
}


type BinTX struct {
	kv *BinKV
	key []byte
}

func (b *BinTX) Cursor(bucket string) Cursor {
	return &BinCursor{
		tx:b,
	}
}

func (b *BinTX) CursorDupSort(bucket string) CursorDupSort {
	panic("implement me")
}

func (b *BinTX) CursorDupFixed(bucket string) CursorDupFixed {
	panic("implement me")
}

func (b *BinTX) GetOne(bucket string, seek []byte) (val []byte, err error) {
	var index int
	if len(seek)==40 {
		index = int(binary.BigEndian.Uint64(seek[:8])) - 1
	} else {
		lastIndex:=0
		panic("sdas")
		index=sort.Search(int(b.kv.numOfElements), func(i int) bool {
			lastIndex = i
			_,err:=b.kv.index.Seek(int64(8+i*(40+8+8)), io.SeekStart)
			if err!=nil {
				panic(err)
			}
			_, err=io.ReadFull(b.kv.index, b.key)
			if err!=nil {
				panic(err)
			}

			res := bytes.Compare(b.key, seek)
			return res >= 0
		})
		if index==int(b.kv.numOfElements) {
			index = lastIndex
		}
	}

	//_,err=b.kv.index.Seek(int64(8+index*(40+8+8)), io.SeekStart)
	//if err!=nil {
	//	return  nil, err
	//}


	//_, err=io.ReadFull(b.kv.index, b.key)
	_,err=b.kv.index.ReadAt(b.key, int64(8+index*(40+8+8)))
	if err!=nil {
		return  nil, err
	}

	if len(seek)==40 && !bytes.Equal(seek, b.key[:40]) {
		fmt.Println(index, binary.BigEndian.Uint64(b.key[:8]), binary.BigEndian.Uint64(seek[:8]),)
		fmt.Println(common.Bytes2Hex(b.key))
		fmt.Println(common.Bytes2Hex(seek))
		return nil, ErrKeyNotFound
	}
	v:=make([]byte, binary.BigEndian.Uint64(b.key[48:56]))
	//_, err = b.kv.data.Seek(int64(binary.BigEndian.Uint64(b.key[40:48])), io.SeekStart)
	//if err!=nil {
	//	return  nil, err
	//}

	_,err=b.kv.data.ReadAt(v, int64(binary.BigEndian.Uint64(b.key[40:48])))
	//_, err=io.ReadFull(b.kv.data, v)
	if err!=nil {
		return  nil, err
	}

	//b.currentIndex = int64(index)+1
	//b.offsetData = int64(binary.BigEndian.Uint64(pointBin)+binary.BigEndian.Uint64(sizeBin))
	return v, nil

}

func (b *BinTX) HasOne(bucket string, key []byte) (bool, error) {
	panic("implement me")
}

func (b *BinTX) Commit(ctx context.Context) error {
	return nil
}

func (b *BinTX) Rollback() {

}

func (b *BinTX) BucketSize(name string) (uint64, error) {
	panic("implement me")
}

func (b *BinTX) Comparator(bucket string) dbutils.CmpFunc {
	panic("implement me")
}

func (tx *BinTX) Cmp(bucket string, a, b []byte) int {
	panic("implement me")
}

func (tx *BinTX) DCmp(bucket string, a, b []byte) int {
	panic("implement me")
}

func (b *BinTX) Sequence(bucket string, amount uint64) (uint64, error) {
	panic("implement me")
}

func (b *BinTX) CHandle() unsafe.Pointer {
	panic("implement me")
}


type BinCursor struct {
	tx *BinTX
	offsetIndex int64
	offsetData int64
	currentKey []byte
	currentVal []byte
	currentIndex int64

	nextKey []byte
	nextVal []byte
}

func (b *BinCursor) Prefix(v []byte) Cursor {
	panic("implement me")
}

func (b *BinCursor) Prefetch(v uint) Cursor {
	panic("implement me")
}

func (b *BinCursor) First() ([]byte, []byte, error) {
	b.offsetIndex =8
	_, err:=b.tx.kv.index.Seek(8, io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}
	key:=make([]byte, 56)
	_, err=io.ReadFull(b.tx.kv.index, key)
	if err!=nil {
		return nil, nil, err
	}
	b.offsetIndex+=56
	b.currentIndex = 0
	b.currentKey = key
	v:=make([]byte, binary.BigEndian.Uint64(key[48:56]))
	_, err=b.tx.kv.data.Seek(int64(binary.BigEndian.Uint64(key[40:48])), io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}

	_, err=io.ReadFull(b.tx.kv.data, v)
	if err!=nil {
		return nil, nil, err
	}
	b.offsetData = int64(binary.BigEndian.Uint64(key[48:56]))
	b.currentKey = key
	b.currentVal=v
	return key[:40], v, nil
}

func (b *BinCursor) Seek(seek []byte) ([]byte, []byte, error) {
	key:=make([]byte, 56)
	lastIndex:=0
	index:=sort.Search(int(b.tx.kv.numOfElements), func(i int) bool {
		lastIndex = i
		_,err:=b.tx.kv.index.Seek(int64(8+i*(40+8+8)), io.SeekStart)
		if err!=nil {
			panic(err)
		}
		_, err=io.ReadFull(b.tx.kv.index, key)
		if err!=nil {
			panic(err)
		}

		res := bytes.Compare(key, seek)
		return res >= 0
	})
	//fmt.Println(index, lastIndex)
	if index==int(b.tx.kv.numOfElements) {
		index = lastIndex
	}
	b.currentIndex = int64(index)
	b.offsetIndex=int64(8+index*(40+8+8))
	_,err:=b.tx.kv.index.Seek(b.offsetIndex, io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}

	_, err=io.ReadFull(b.tx.kv.index, key)
	if err!=nil {
		return nil, nil, err
	}


	//fmt.Println("bin", binary.BigEndian.Uint64(pointBin), binary.BigEndian.Uint64(sizeBin))

	v:=make([]byte, binary.BigEndian.Uint64(key[48:56]))
	_, err = b.tx.kv.data.Seek(int64(binary.BigEndian.Uint64(key[40:48])), io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}

	_, err=io.ReadFull(b.tx.kv.data, v)
	if err!=nil {
		return nil, nil, err
	}
	b.currentIndex = int64(index)+1
	b.offsetData = int64(binary.BigEndian.Uint64(key[40:48])+binary.BigEndian.Uint64(key[48:56]))

	b.currentKey = key
	b.currentVal=v

	return key[:40], v, nil

}


func (b *BinCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	panic("implement me")
}

func (b *BinCursor) Next() ([]byte, []byte, error) {
	if b.currentIndex>= int64(b.tx.kv.numOfElements)-1 {
		return nil, nil, nil
	}
	numOfRead:=100
	if b.currentIndex+int64(numOfRead) >= int64(b.tx.kv.numOfElements)-1 {
		numOfRead = int(int64(b.tx.kv.numOfElements)-1 - b.currentIndex)
	}
	var key,val []byte
	var  err error
	if len(b.nextKey)==0 {
		key=make([]byte, 56*numOfRead)
		_, err=io.ReadFull(b.tx.kv.index, key)
		if err!=nil {
			return nil, nil, err
		}
		var bodiesSize int
		for i:=0;i<numOfRead;i++ {
			bodiesSize+=int(binary.BigEndian.Uint64(key[i*56+48:i*56+56]))
		}
		b.nextVal = make([]byte, bodiesSize)
		b.nextKey = key[56:]
		key = key[:56]

		_, err = b.tx.kv.data.Seek(int64(binary.BigEndian.Uint64(key[40:48])), io.SeekStart)
		if err!=nil {
			return nil, nil, err
		}
		_, err=io.ReadFull(b.tx.kv.data, b.nextVal)
		if err!=nil {
			return nil, nil, err
		}

		val = b.nextVal[:int(binary.BigEndian.Uint64(key[48:56]))]
		b.nextVal = b.nextVal[int(binary.BigEndian.Uint64(key[48:56])):]
		b.offsetIndex+=int64(56*numOfRead)
		b.offsetData+=int64(bodiesSize)
		//fmt.Println(bodiesSize)

	} else {
		key = b.nextKey[:56]
		b.nextKey = b.nextKey[56:]
		val = b.nextVal[:int(binary.BigEndian.Uint64(key[48:56]))]
		b.nextVal = b.nextVal[int(binary.BigEndian.Uint64(key[48:56])):]
	}
	b.currentIndex++

	b.currentKey = key
	b.currentVal = val

	return key[:40], val, nil
}

func (b *BinCursor) Prev() ([]byte, []byte, error) {
	panic("implement me")
}

func (b *BinCursor) Last() ([]byte, []byte, error) {
	b.offsetIndex =8+int64(b.tx.kv.numOfElements-1)*56
	_, err:=b.tx.kv.index.Seek(b.offsetIndex, io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}
	key:=make([]byte, 56)
	_, err=io.ReadFull(b.tx.kv.index, key)
	if err!=nil {
		return nil, nil, err
	}
	b.offsetIndex+=56
	b.currentIndex = int64(b.tx.kv.numOfElements-1)
	b.currentKey = key
	v:=make([]byte, binary.BigEndian.Uint64(key[48:56]))
	_, err=b.tx.kv.data.Seek(int64(binary.BigEndian.Uint64(key[40:48])), io.SeekStart)
	if err!=nil {
		return nil, nil, err
	}

	_, err=io.ReadFull(b.tx.kv.data, v)
	if err!=nil {
		return nil, nil, err
	}
	b.offsetData = int64(binary.BigEndian.Uint64(key[48:56]))
	b.currentKey = key
	b.currentVal=v
	return key[:40], v, nil

}

func (b *BinCursor) Current() ([]byte, []byte, error) {
	//key:=make([]byte, 56)
	//_, err:=b.tx.kv.index.Seek(-(40+8+8), io.SeekCurrent)
	//if err!=nil {
	//	return nil, nil, err
	//}
	// _,err=io.ReadFull(b.tx.kv.index,key)
	//if err != nil {
	//	return nil, nil, err
	//}
	//_, err = b.tx.kv.data.Seek(-int64(binary.BigEndian.Uint64(key[48:56])), io.SeekCurrent)
	//if err != nil {
	//	return nil, nil, err
	//}
	//v:=make([]byte, binary.BigEndian.Uint64(key[48:56]))
	//_,err=io.ReadFull(b.tx.kv.data,v)
	//if err != nil {
	//	return nil, nil, err
	//}
	return b.currentKey[:40], b.currentVal, nil
}

func (b *BinCursor) Put(k, v []byte) error {
	panic("implement me")
}

func (b *BinCursor) Append(k []byte, v []byte) error {
	panic("implement me")
}

func (b *BinCursor) Delete(k, v []byte) error {
	panic("implement me")
}

func (b *BinCursor) DeleteCurrent() error {
	panic("implement me")
}

func (b *BinCursor) Reserve(k []byte, n int) ([]byte, error) {
	panic("implement me")
}

func (b *BinCursor) PutCurrent(key, value []byte) error {
	panic("implement me")
}

func (b *BinCursor) Count() (uint64, error) {
	panic("implement me")
}

func (b *BinCursor) Close() {
	panic("implement me")
}
