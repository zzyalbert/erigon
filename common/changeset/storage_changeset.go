package changeset

import (
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	DefaultIncarnation = uint64(1)
)

var (
	ErrNotFound  = errors.New("not found")
	ErrFindValue = errors.New("find value error")
)

/* Plain changesets (key is a common.Address) */

func NewStorageChangeSetPlain() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.AddressLength + common.HashLength + common.IncarnationLength,
	}
}

func EncodeStoragePlain(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	return encodeStorage2(blockN, s, common.AddressLength, f)
}

type StorageChangeSetPlain struct{ c ethdb.CursorDupSort }

func (b StorageChangeSetPlain) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k[:common.AddressLength], k[common.AddressLength:])
}

func (b StorageChangeSetPlain) FindWithIncarnation(blockNumber uint64, k []byte) ([]byte, error) {
	return findInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k)
}

func (b StorageChangeSetPlain) FindWithoutIncarnation(blockNumber uint64, addressToFind []byte, keyToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, addressToFind, keyToFind)
}

// RewindDataPlain generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(losPrefix string, db ethdb.Tx, timestampSrc, timestampDst uint64, quit <-chan struct{}) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	if err := walkAndCollect(
		losPrefix+" accounts",
		collector.AccountWalker,
		db, dbutils.PlainAccountChangeSetBucket,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		losPrefix+" storage",
		collector.StorageWalker,
		db, dbutils.PlainStorageChangeSetBucket,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return nil, nil, err
	}

	return collector.AccountData, collector.StorageData, nil
}

type rewindDataCollector struct {
	AccountData map[string][]byte
	StorageData map[string][]byte
}

func newRewindDataCollector() *rewindDataCollector {
	return &rewindDataCollector{make(map[string][]byte), make(map[string][]byte)}
}

func (c *rewindDataCollector) AccountWalker(k, v []byte) error {
	if _, ok := c.AccountData[string(k)]; !ok {
		c.AccountData[string(k)] = v
	}
	return nil
}

func (c *rewindDataCollector) StorageWalker(k, v []byte) error {
	if _, ok := c.StorageData[string(k)]; !ok {
		c.StorageData[string(k)] = v
	}
	return nil
}

func walkAndCollect(logPrefix string, collectorFunc func([]byte, []byte) error, db ethdb.Tx, bucket string, timestampDst, timestampSrc uint64, quit <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	fromDBFormat := FromDBFormat(Mapper[bucket].KeySize)
	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	i := 0

	err = ethdb.Walk(c, dbutils.EncodeBlockNumber(timestampDst), 0, func(dbKey, dbValue []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}
		timestamp, k, v := fromDBFormat(dbKey, dbValue)
		if timestamp > timestampSrc {
			return false, nil
		}
		i += len(k) + len(v)
		if innerErr := collectorFunc(common.CopyBytes(k), common.CopyBytes(v)); innerErr != nil {
			return false, innerErr
		}
		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] collecting changes", logPrefix), "block_number", timestamp)
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("collected: %dM bytes\n", i/1024/1024)
	return nil
}
