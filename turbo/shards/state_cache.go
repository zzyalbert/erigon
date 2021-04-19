package shards

import (
	"bytes"
	"container/heap"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

// LRU state cache consists of two structures - B-Tree and binary heap
// Every element is marked either as Read, Updated, or Deleted via flags

// Metrics
var (
	AccRead       = metrics.NewRegisteredCounter("cache/acc_read/total", nil)
	AccReadHit    = metrics.NewRegisteredCounter("cache/acc_read/hits", nil)
	StRead        = metrics.NewRegisteredCounter("cache/st_read/total", nil)
	StReadHit     = metrics.NewRegisteredCounter("cache/st_read/hits", nil)
	WritesRead    = metrics.NewRegisteredCounter("cache/writes/total", nil)
	WritesReadHit = metrics.NewRegisteredCounter("cache/writes/hits", nil)
)

const (
	ModifiedFlag    uint16 = 1 // Set when the item is different seek what is last committed to the database
	AbsentFlag      uint16 = 2 // Set when the item is absent in the state
	DeletedFlag     uint16 = 4 // Set when the item is marked for deletion, even though it might have the value in it
	UnprocessedFlag uint16 = 8 // Set when there is a modification in the item that invalidates merkle root calculated previously
)

// Sizes of B-tree items for the purposes of keeping track of the size of reads and writes
// The sizes of the nodes of the B-tree are not accounted for, because their are private to the `btree` package
// +16 means - each item has 2 words overhead: 1 for interface, 1 for pointer to item.
const (
	accountItemSize      = int(unsafe.Sizeof(AccountItem{}) + 16)
	accountWriteItemSize = int(unsafe.Sizeof(AccountWriteItem{})+16) + accountItemSize
	storageItemSize      = int(unsafe.Sizeof(StorageItem{}) + 16)
	storageWriteItemSize = int(unsafe.Sizeof(StorageWriteItem{})+16) + storageItemSize
	codeItemSize         = int(unsafe.Sizeof(CodeItem{}) + 16)
	codeWriteItemSize    = int(unsafe.Sizeof(CodeWriteItem{})+16) + codeItemSize
)

// AccountSeek allows to traverse sub-tree
type AccountSeek struct {
	seek       []byte
	fixedBytes int
	mask       byte
}

// StorageSeek allows to traverse sub-tree
type StorageSeek struct {
	address     common.Hash
	incarnation uint64
	seek        []byte
	fixedBytes  int
	mask        byte
}

// AccountItem is an element in the `readWrites` B-tree representing an Ethereum account. It can mean either value
// just read seek the database and cache (read), or value that is different seek what the last committed value
// in the DB is (write). Reads can be removed or evicted seek the B-tree at any time, because this
// does not hurt the consistency. Writes cannot be removed or evicted one by one, therefore they can
// either be deleted all together, or committed all together and turned into reads.
type AccountItem struct {
	sequence int
	queuePos int
	flags    uint16
	address  common.Address
	account  accounts.Account
}

// AccountWriteItem is an item in the `writes` B-tree. As can be seen, it always references a corresponding
// `AccountItem`. There can be `AccountItem` without corresponding `AccountWriteItem` (in that case `AccountItem`
// represents a cached read), but there cannot be `AccountWriteItem` without a corresponding `AccountItem`.
// Such pair represents an account that has been modified in the cache, but the modification has not been committed
// to the database yet. The correspondence of an `ai AccountItem` and an `awi AccountWriteItem` implies that
// `keccak(awi.address) == ai.address`.
type AccountWriteItem struct {
	ai *AccountItem
}

type StorageItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	incarnation uint64
	address     common.Address
	location    common.Hash
	value       uint256.Int
}

type StorageWriteItem struct {
	si *StorageItem
}

type CodeItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	addrHash    common.Hash
	incarnation uint64
	code        []byte
}

type CodeWriteItem struct {
	address common.Address
	ci      *CodeItem
}

type CacheItem interface {
	btree.Item
	GetSequence() int
	SetSequence(sequence int)
	GetSize() int
	GetQueuePos() int
	SetQueuePos(pos int)
	HasFlag(flag uint16) bool        // Check if specified flag is set
	SetFlags(flags uint16)           // Set specified flags, but leaves other flags alone
	ClearFlags(flags uint16)         // Clear specified flags, but laves other flags alone
	CopyValueFrom(item CacheItem)    // Copy value (not key) seek given item
	HasPrefix(prefix CacheItem) bool // Whether this item has specified item as a prefix
}

type CacheWriteItem interface {
	btree.Item
	GetCacheItem() CacheItem
	SetCacheItem(item CacheItem)
	GetSize() int
}

func (r *AccountSeek) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return less(r.seek, i.address.Bytes(), r.fixedBytes, r.mask)
		//return bytes.Compare(r.seek, i.address.Bytes()) < 0
	case *AccountWriteItem:
		return less(r.seek, i.ai.address.Bytes(), r.fixedBytes, r.mask)
		//return bytes.Compare(r.seek, i.ai.address.Bytes()) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (r *StorageSeek) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *StorageItem:
		c := bytes.Compare(r.address.Bytes(), i.address.Bytes())
		if c != 0 {
			return c < 0
		}
		if r.incarnation < i.incarnation {
			return true
		}
		return less(r.seek, i.location.Bytes(), r.fixedBytes, r.mask)
		//return bytes.Compare(r.seek, i.location.Bytes()) < 0
	case *StorageWriteItem:
		c := bytes.Compare(r.address.Bytes(), i.si.address.Bytes())
		if c != 0 {
			return c < 0
		}
		if r.incarnation < i.si.incarnation {
			return true
		}
		return less(r.seek, i.si.location.Bytes(), r.fixedBytes, r.mask)
		//return bytes.Compare(r.seek, i.si.location.Bytes()) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (ai *AccountItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return bytes.Compare(ai.address.Bytes(), i.address.Bytes()) < 0
	case *AccountWriteItem:
		return bytes.Compare(ai.address.Bytes(), i.ai.address.Bytes()) < 0
	case *AccountSeek:
		return less(ai.address.Bytes(), i.seek, i.fixedBytes, i.mask)
		//return bytes.Compare(ai.address.Bytes(), i.seek) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (awi *AccountWriteItem) GetCacheItem() CacheItem     { return awi.ai }
func (awi *AccountWriteItem) SetCacheItem(item CacheItem) { awi.ai = item.(*AccountItem) }
func (awi *AccountWriteItem) GetSize() int                { return accountWriteItemSize }
func (awi *AccountWriteItem) Less(than btree.Item) bool {
	return awi.ai.Less(than)
}

func (ai *AccountItem) GetSequence() int         { return ai.sequence }
func (ai *AccountItem) SetSequence(sequence int) { ai.sequence = sequence }
func (ai *AccountItem) GetSize() int             { return accountItemSize }
func (ai *AccountItem) GetQueuePos() int         { return ai.queuePos }
func (ai *AccountItem) SetQueuePos(pos int)      { ai.queuePos = pos }
func (ai *AccountItem) HasFlag(flag uint16) bool { return ai.flags&flag != 0 }
func (ai *AccountItem) SetFlags(flags uint16)    { ai.flags |= flags }
func (ai *AccountItem) ClearFlags(flags uint16)  { ai.flags &^= flags }
func (ai *AccountItem) String() string           { return fmt.Sprintf("AccountItem(address=%x)", ai.address) }

func (ai *AccountItem) CopyValueFrom(item CacheItem) {
	otherAi, ok := item.(*AccountItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountItem, got %T", item))
	}
	ai.account.Copy(&otherAi.account)
}

func (swi *StorageWriteItem) Less(than btree.Item) bool {
	return swi.si.Less(than)
}
func (swi *StorageWriteItem) GetCacheItem() CacheItem     { return swi.si }
func (swi *StorageWriteItem) SetCacheItem(item CacheItem) { swi.si = item.(*StorageItem) }
func (swi *StorageWriteItem) GetSize() int                { return storageWriteItemSize }

func (si *StorageItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *StorageItem:
		c := bytes.Compare(si.address.Bytes(), i.address.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.incarnation {
			return true
		}
		return bytes.Compare(si.location.Bytes(), i.location.Bytes()) < 0
	case *StorageWriteItem:
		c := bytes.Compare(si.address.Bytes(), i.si.address.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.si.incarnation {
			return true
		}
		return bytes.Compare(si.location.Bytes(), i.si.location.Bytes()) < 0
	case *StorageSeek:
		c := bytes.Compare(si.address.Bytes(), i.address.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.incarnation {
			return true
		}
		return less(si.location.Bytes(), i.seek, i.fixedBytes, i.mask)
		//return bytes.Compare(si.location.Bytes(), i.seek) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}
func (si *StorageItem) GetSequence() int         { return si.sequence }
func (si *StorageItem) SetSequence(sequence int) { si.sequence = sequence }
func (si *StorageItem) GetSize() int             { return storageItemSize }
func (si *StorageItem) GetQueuePos() int         { return si.queuePos }
func (si *StorageItem) SetQueuePos(pos int)      { si.queuePos = pos }
func (si *StorageItem) HasFlag(flag uint16) bool { return si.flags&flag != 0 }
func (si *StorageItem) SetFlags(flags uint16)    { si.flags |= flags }
func (si *StorageItem) ClearFlags(flags uint16)  { si.flags &^= flags }
func (si *StorageItem) String() string {
	return fmt.Sprintf("StorageItem(address=%x,incarnation=%d,location=%x)", si.address, si.incarnation, si.location)
}

func (si *StorageItem) CopyValueFrom(item CacheItem) {
	otherSi, ok := item.(*StorageItem)
	if !ok {
		panic(fmt.Sprintf("expected StorageCacheItem, got %T", item))
	}
	si.value.Set(&otherSi.value)
}

func (ci *CodeItem) Less(than btree.Item) bool {
	ci2 := than.(*CodeItem)
	c := bytes.Compare(ci.addrHash.Bytes(), ci2.addrHash.Bytes())
	if c != 0 {
		return c < 0
	}
	if ci.incarnation == ci2.incarnation {
		return false
	}
	if ci.incarnation < ci2.incarnation {
		return false
	}
	return true
}

func (cwi *CodeWriteItem) Less(than btree.Item) bool {
	i := than.(*CodeWriteItem)
	c := bytes.Compare(cwi.ci.addrHash.Bytes(), i.ci.addrHash.Bytes())
	if c == 0 {
		return cwi.ci.incarnation < i.ci.incarnation
	}
	return c < 0
}

func (cwi *CodeWriteItem) GetCacheItem() CacheItem     { return cwi.ci }
func (cwi *CodeWriteItem) SetCacheItem(item CacheItem) { cwi.ci = item.(*CodeItem) }
func (cwi *CodeWriteItem) GetSize() int                { return codeWriteItemSize + len(cwi.ci.code) }
func (ci *CodeItem) GetSequence() int                  { return ci.sequence }
func (ci *CodeItem) SetSequence(sequence int)          { ci.sequence = sequence }
func (ci *CodeItem) GetSize() int                      { return codeItemSize + len(ci.code) }
func (ci *CodeItem) GetQueuePos() int                  { return ci.queuePos }
func (ci *CodeItem) SetQueuePos(pos int)               { ci.queuePos = pos }
func (ci *CodeItem) HasFlag(flag uint16) bool          { return ci.flags&flag != 0 }
func (ci *CodeItem) SetFlags(flags uint16)             { ci.flags |= flags }
func (ci *CodeItem) ClearFlags(flags uint16)           { ci.flags &^= flags }
func (ci *CodeItem) String() string {
	return fmt.Sprintf("CodeItem(address=%x,incarnation=%d)", ci.addrHash, ci.incarnation)
}

func (ci *CodeItem) CopyValueFrom(item CacheItem) {
	otherCi, ok := item.(*CodeItem)
	if !ok {
		panic(fmt.Sprintf("expected CodeCacheItem, got %T", item))
	}
	ci.code = make([]byte, len(otherCi.code))
	copy(ci.code, otherCi.code)
}

// Heap for reads
type ReadHeap struct {
	items []CacheItem
}

func (rh ReadHeap) Len() int           { return len(rh.items) }
func (rh ReadHeap) Less(i, j int) bool { return rh.items[i].GetSequence() < rh.items[j].GetSequence() }
func (rh ReadHeap) Swap(i, j int) {
	// Swap queue positions in the B-tree leaves too
	rh.items[i].SetQueuePos(j)
	rh.items[j].SetQueuePos(i)
	rh.items[i], rh.items[j] = rh.items[j], rh.items[i]
}

func (rh *ReadHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	cacheItem := x.(CacheItem)
	cacheItem.SetQueuePos(len(rh.items))
	rh.items = append(rh.items, cacheItem)
}

func (rh *ReadHeap) Pop() interface{} {
	cacheItem := rh.items[len(rh.items)-1]
	rh.items = rh.items[:len(rh.items)-1]
	return cacheItem
}

// StateCache is the structure containing B-trees and priority queues for the state cache
type StateCache struct {
	readWrites  [5]*btree.BTree   // Mixed reads and writes
	writes      [5]*btree.BTree   // Only writes for the effective iteration
	readQueue   [5]ReadHeap       // Priority queue of read elements eligible for eviction (sorted by sequence)
	limit       datasize.ByteSize // Total size of the readQueue (if new item causes the size to go over the limit, some existing items are evicted)
	readSize    int
	writeSize   int
	sequence    int                // Current sequence assigned to any item that has been "touched" (created, deleted, read). Incremented after every touch
	unprocQueue [5]UnprocessedHeap // Priority queue of items appeared since last root calculation processing (sorted by the keys - address, incarnation, location)

}

func id(a interface{}) uint8 {
	switch a.(type) {
	case *AccountItem, *AccountWriteItem, *AccountSeek:
		return 0
	case *StorageItem, *StorageWriteItem, *StorageSeek:
		return 1
	case *CodeItem, *CodeWriteItem:
		return 2
	case *AccountTrieItem, *AccountTrieWriteItem:
		return 3
	case *StorageTrieItem, *StorageTrieWriteItem:
		return 4
	default:
		panic(fmt.Sprintf("unexpected type: %T", a))
	}
}

// NewStateCache create a new state cache based on the B-trees of specific degree. The second and the third parameters are the limit on the number of reads and writes to cache, respectively
func NewStateCache(degree int, limit datasize.ByteSize) *StateCache {
	var sc StateCache
	sc.limit = limit
	for i := 0; i < len(sc.readWrites); i++ {
		sc.readWrites[i] = btree.New(degree)
	}
	for i := 0; i < len(sc.writes); i++ {
		sc.writes[i] = btree.New(degree)
	}
	for i := 0; i < len(sc.readQueue); i++ {
		heap.Init(&sc.readQueue[i])
	}
	for i := 0; i < len(sc.unprocQueue); i++ {
		heap.Init(&sc.unprocQueue[i])
	}
	return &sc
}

// Clone creates a clone cache which can be modified independently, but it shares the parts of the cache that are common
func (sc *StateCache) Clone() *StateCache {
	var clone StateCache
	for i := range clone.readWrites {
		clone.readWrites[i] = sc.readWrites[i].Clone()
		clone.writes[i] = sc.writes[i].Clone()
		clone.limit = sc.limit
		heap.Init(&clone.readQueue[i])
		heap.Init(&clone.unprocQueue[i])
	}
	return &clone
}

func (sc *StateCache) Purge() {
	for i := 0; i < len(sc.readWrites); i++ {
		sc.readWrites[i] = btree.New(32)
	}
	for i := 0; i < len(sc.writes); i++ {
		sc.writes[i] = btree.New(32)
	}
	for i := 0; i < len(sc.readQueue); i++ {
		for sc.readQueue[i].Len() > 0 {
			heap.Pop(&sc.readQueue[i])
		}
		heap.Init(&sc.readQueue[i])
	}
	for i := 0; i < len(sc.unprocQueue); i++ {
		for sc.unprocQueue[i].Len() > 0 {
			heap.Pop(&sc.unprocQueue[i])
		}
		heap.Init(&sc.unprocQueue[i])
	}
	runtime.GC()
}

func (sc *StateCache) get(key btree.Item) (CacheItem, bool) {
	WritesRead.Inc(1)
	item := sc.readWrites[id(key)].Get(key)
	if item == nil {
		return nil, false
	}
	WritesReadHit.Inc(1)
	cacheItem := item.(CacheItem)
	if cacheItem.HasFlag(DeletedFlag) || cacheItem.HasFlag(AbsentFlag) {
		return nil, true
	}
	return cacheItem, true
}

// GetAccount searches and account with given address, without modifying any structures
// Second return value is true if such account is found
func (sc *StateCache) GetAccount(address []byte) (*accounts.Account, bool) {
	AccRead.Inc(1)
	var key AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.address[:])
	if item, ok := sc.get(&key); ok {
		if item != nil {
			AccReadHit.Inc(1)
			return &item.(*AccountItem).account, true
		}
		return nil, true
	}
	return nil, false
}

// GetDeletedAccount attempts to retrieve the last version of account before it was deleted
func (sc *StateCache) GetDeletedAccount(address []byte) *accounts.Account {
	key := &AccountItem{}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.address[:])
	item := sc.readWrites[id(key)].Get(key)
	if item == nil {
		return nil
	}
	ai := item.(*AccountItem)
	if !ai.HasFlag(DeletedFlag) {
		return nil
	}
	return &ai.account
}

// GetStorage searches storage item with given address, incarnation, and location, without modifying any structures
// Second return value is true if such item is found
func (sc *StateCache) GetStorage(address []byte, incarnation uint64, location []byte) ([]byte, bool) {
	StRead.Inc(1)
	var key StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.address[:])
	key.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(key.location[:])
	if item, ok := sc.get(&key); ok {
		if item != nil {
			StReadHit.Inc(1)
			return item.(*StorageItem).value.Bytes(), true
		}
		return nil, true
	}
	return nil, false
}

// GetCode searches contract code with given address, without modifying any structures
// Second return value is true if such item is found
func (sc *StateCache) GetCode(address []byte, incarnation uint64) ([]byte, bool) {
	var key CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	key.incarnation = incarnation
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return item.(*CodeItem).code, true
		}
		return nil, true
	}
	return nil, false
}

func (sc *StateCache) setRead(item CacheItem, absent bool) {
	id := id(item)
	if sc.readWrites[id].Get(item) != nil {
		panic(fmt.Sprintf("item must not be present in the cache before doing setRead: %s", item))
	}
	item.SetSequence(sc.sequence)
	sc.sequence++
	item.ClearFlags(ModifiedFlag | DeletedFlag)
	if absent {
		item.SetFlags(AbsentFlag)
	} else {
		item.ClearFlags(AbsentFlag)
	}

	if sc.limit != 0 && sc.readSize+item.GetSize() > int(sc.limit) {
		for sc.readQueue[id].Len() > 0 && sc.readSize+item.GetSize() > int(sc.limit) {
			// Read queue cannot grow anymore, need to evict one element
			cacheItem := heap.Pop(&sc.readQueue[id]).(CacheItem)
			sc.readSize -= cacheItem.GetSize()
			sc.readWrites[id].Delete(cacheItem)
		}
	}
	// Push new element on the read queue
	heap.Push(&sc.readQueue[id], item)
	sc.readWrites[id].ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
}

func (sc *StateCache) readQueuesLen() (res int) {
	for i := 0; i < len(sc.readQueue); i++ {
		res += sc.readQueue[i].Len()
	}
	return
}

// SetAccountReadPlain adds given account to the cache, marking it as a read (not written)
func (sc *StateCache) SetAccountReadPlain(address []byte, account *accounts.Account) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.address[:])
	ai.account.Copy(account)
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) SetAccountRead(address common.Address, account *accounts.Account) {
	ai := AccountItem{address: address}
	ai.account.Copy(account)
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) GetAccountByHashedAddress(address common.Hash) (*accounts.Account, bool) {
	var key AccountItem
	key.address.SetBytes(address.Bytes())
	if item, ok := sc.get(&key); ok {
		if item != nil {
			StReadHit.Inc(1)
			return &item.(*AccountItem).account, true
		}
		return nil, true
	}
	return nil, false
}

// SetAccountReadPlain adds given account address to the cache, marking it as a absent
func (sc *StateCache) SetAccountAbsentPlain(address []byte) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.address[:])
	sc.setRead(&ai, true /* absent */)
}

func (sc *StateCache) setWrite(item CacheItem, writeItem CacheWriteItem, delete bool) {
	id := id(item)
	// Check if this is going to be modification of the existing entry
	if existing := sc.writes[id].Get(writeItem); existing != nil {
		cacheWriteItem := existing.(CacheWriteItem)
		cacheItem := cacheWriteItem.GetCacheItem()
		sc.readSize += item.GetSize()
		sc.readSize -= cacheItem.GetSize()
		sc.writeSize += writeItem.GetSize()
		sc.writeSize -= cacheWriteItem.GetSize()
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.CopyValueFrom(item)
			cacheItem.ClearFlags(DeletedFlag)
		}
		cacheItem.SetSequence(sc.sequence)
		sc.sequence++
		return
	}
	// Now see if there is such item in the readWrite B-tree - then we replace read entry with write entry
	if existing := sc.readWrites[id].Get(item); existing != nil {
		cacheItem := existing.(CacheItem)
		// Remove seek the reads queue
		if sc.readQueue[id].Len() > 0 {
			heap.Remove(&sc.readQueue[id], cacheItem.GetQueuePos())
		}
		sc.readSize += item.GetSize()
		sc.readSize -= cacheItem.GetSize()
		cacheItem.SetFlags(ModifiedFlag)
		cacheItem.ClearFlags(AbsentFlag)
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.CopyValueFrom(item)
			cacheItem.ClearFlags(DeletedFlag)
		}
		cacheItem.SetSequence(sc.sequence)
		sc.sequence++
		writeItem.SetCacheItem(cacheItem)
		sc.writes[id].ReplaceOrInsert(writeItem)
		sc.writeSize += writeItem.GetSize()
		return
	}
	if sc.limit != 0 && sc.readSize+item.GetSize() > int(sc.limit) {
		for sc.readQueue[id].Len() > 0 && sc.readSize+item.GetSize() > int(sc.limit) {
			// There is no space available, need to evict one read element
			cacheItem := heap.Pop(&sc.readQueue[id]).(CacheItem)
			sc.readWrites[id].Delete(cacheItem)
			sc.readSize -= cacheItem.GetSize()
		}
	}
	item.SetSequence(sc.sequence)
	sc.sequence++
	item.SetFlags(ModifiedFlag)
	item.ClearFlags(AbsentFlag)
	if delete {
		item.SetFlags(DeletedFlag)
	} else {
		item.ClearFlags(DeletedFlag)
	}
	sc.readWrites[id].ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
	writeItem.SetCacheItem(item)
	sc.writes[id].ReplaceOrInsert(writeItem)
	sc.writeSize += writeItem.GetSize()
}

// SetAccountWritePlain adds given account to the cache, marking it as written (cannot be evicted)
func (sc *StateCache) SetAccountWritePlain(address []byte, account *accounts.Account) {
	var ai AccountItem
	var awi AccountWriteItem
	copy(ai.address[:], address)
	ai.account.Copy(account)
	awi.ai = &ai
	sc.setWrite(&ai, &awi, false /* delete */)
}

// SetAccountDeletePlain is very similar to SetAccountWritePlain with the difference that there no set value
func (sc *StateCache) SetAccountDeletePlain(address []byte) {
	var ai AccountItem
	copy(ai.address[:], address)
	var awi AccountWriteItem
	awi.ai = &ai
	sc.setWrite(&ai, &awi, true /* delete */)
}

func (sc *StateCache) SetStorageReadPlain(address []byte, incarnation uint64, location []byte, value []byte) {
	var si StorageItem
	copy(si.address[:], address)
	si.incarnation = incarnation
	copy(si.location[:], location)
	si.value.SetBytes(value)
	sc.setRead(&si, false /* absent */)
}

func (sc *StateCache) SetStorageAbsentPlain(address []byte, incarnation uint64, location []byte) {
	var si StorageItem
	copy(si.address[:], address)
	si.incarnation = incarnation
	copy(si.location[:], location)
	sc.setRead(&si, true /* absent */)
}

func (sc *StateCache) SetStorageWritePlain(address []byte, incarnation uint64, location []byte, value []byte) {
	var si StorageItem
	copy(si.address[:], address)
	si.incarnation = incarnation
	copy(si.location[:], location)
	si.value.SetBytes(value)
	var swi StorageWriteItem
	swi.si = &si
	sc.setWrite(&si, &swi, false /* delete */)
}

func (sc *StateCache) SetStorageDeletePlain(address []byte, incarnation uint64, location []byte) {
	var si StorageItem
	copy(si.address[:], address)
	si.incarnation = incarnation
	copy(si.location[:], location)
	var swi StorageWriteItem
	swi.si = &si
	sc.setWrite(&si, &swi, true /* delete */)
}

func (sc *StateCache) SetCodeRead(address []byte, incarnation uint64, code []byte) {
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = make([]byte, len(code))
	copy(ci.code, code)
	sc.setRead(&ci, false /* absent */)
}

func (sc *StateCache) SetCodeAbsent(address []byte, incarnation uint64) {
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	sc.setRead(&ci, true /* absent */)
}

func (sc *StateCache) SetCodeWrite(address []byte, incarnation uint64, code []byte) {
	// Check if this is going to be modification of the existing entry
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = make([]byte, len(code))
	copy(ci.code, code)
	var cwi CodeWriteItem
	copy(cwi.address[:], address)
	cwi.ci = &ci
	sc.setWrite(&ci, &cwi, false /* delete */)
}

func (sc *StateCache) SetCodeDelete(address []byte, incarnation uint64) {
	// Check if this is going to be modification of the existing entry
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = nil
	var cwi CodeWriteItem
	copy(cwi.address[:], address)
	cwi.ci = &ci
	sc.setWrite(&ci, &cwi, true /* delete */)
}

func (sc *StateCache) PrepareWrites() [5]*btree.BTree {
	var writes [5]*btree.BTree
	for i := 0; i < len(sc.writes); i++ {
		sc.writes[i].Ascend(func(i btree.Item) bool {
			writeItem := i.(CacheWriteItem)
			cacheItem := writeItem.GetCacheItem()
			cacheItem.ClearFlags(ModifiedFlag)
			if cacheItem.HasFlag(DeletedFlag) {
				cacheItem.ClearFlags(DeletedFlag)
				cacheItem.SetFlags(AbsentFlag)
			}
			return true
		})
		writes[i] = sc.writes[i].Clone()
		sc.writes[i].Clear(true /* addNodesToFreeList */)
		sc.writeSize = 0
	}
	return writes
}

func WalkWrites(
	writes [5]*btree.BTree,
	accountWrite func(address []byte, account *accounts.Account) error,
	accountDelete func(address []byte, original *accounts.Account) error,
	storageWrite func(address []byte, incarnation uint64, location []byte, value []byte) error,
	storageDelete func(address []byte, incarnation uint64, location []byte) error,
	codeWrite func(address []byte, incarnation uint64, code []byte) error,
	codeDelete func(address []byte, incarnation uint64) error,
) error {
	var err error
	for i := 0; i < len(writes); i++ {
		writes[i].Ascend(func(i btree.Item) bool {
			switch it := i.(type) {
			case *AccountWriteItem:
				if it.ai.flags&AbsentFlag != 0 {
					if err = accountDelete(it.ai.address.Bytes(), &it.ai.account); err != nil {
						return false
					}
				} else {
					if err = accountWrite(it.ai.address.Bytes(), &it.ai.account); err != nil {
						return false
					}
				}
			case *StorageWriteItem:
				if it.si.flags&AbsentFlag != 0 {
					if err = storageDelete(it.si.address.Bytes(), it.si.incarnation, it.si.location.Bytes()); err != nil {
						return false
					}
				} else {
					if err = storageWrite(it.si.address.Bytes(), it.si.incarnation, it.si.location.Bytes(), it.si.value.Bytes()); err != nil {
						return false
					}
				}
			case *CodeWriteItem:
				if it.ci.flags&AbsentFlag != 0 {
					if err = codeDelete(it.address.Bytes(), it.ci.incarnation); err != nil {
						return false
					}
				} else {
					if err = codeWrite(it.address.Bytes(), it.ci.incarnation, it.ci.code); err != nil {
						return false
					}
				}
			}
			return true
		})
	}

	return err
}

func (sc *StateCache) TurnWritesToReads(writes [5]*btree.BTree) {
	for i := 0; i < len(writes); i++ {
		readQueue := &sc.readQueue[i]
		writes[i].Ascend(func(it btree.Item) bool {
			cacheWriteItem := it.(CacheWriteItem)
			cacheItem := cacheWriteItem.GetCacheItem()
			if !cacheItem.HasFlag(ModifiedFlag) {
				// Cannot touch items that have been modified since we have taken away the writes
				heap.Push(readQueue, cacheItem)
			}
			return true
		})
	}
}

func (sc *StateCache) TotalCount() (res int) {
	for i := 0; i < len(sc.readWrites); i++ {
		res += sc.readWrites[i].Len()
	}
	return
}
func (sc *StateCache) WriteCount() (res int) {
	for i := 0; i < len(sc.readWrites); i++ {
		res += sc.writes[i].Len()
	}
	return
}
func (sc *StateCache) WriteSize() int { return sc.writeSize }
func (sc *StateCache) ReadSize() int  { return sc.readSize }

func less(k, k2 []byte, fixedbytes int, mask byte) (isLess bool) {
	cmp := bytes.Compare(k[:fixedbytes], k2[:fixedbytes])
	if cmp == 0 {
		cmp = int(k[fixedbytes]&mask) - int(k2[fixedbytes]&mask)
	}
	if cmp == 0 {
		cmp = len(k) - len(k2)
	}
	return cmp < 0
}
