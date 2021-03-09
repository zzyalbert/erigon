package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"reflect"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)


var emptyCodeHash = crypto.Keccak256(nil)
var emptyCodeHashH = common.BytesToHash(emptyCodeHash)

/*
	before 3:
	addr1(f22b):""
	addr2(1f0e):""
	addr3(3e05):""
	addr4(d12e):""
	block 3
	addr1(f22b):"block3"
	addr2(1f0e):""
	addr3(3e05):"state"
	addr4(d12e):"block3"
	block 5
	addr1(f22b):"state"
	addr2(1f0e):"state"
	addr3(3e05):"state"
	addr4(d12e):""
*/

func TestWalkAsOfStatePlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := state.NewTrieDbState(common.Hash{}, db, 1)

	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	key := common.Hash{123}
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
	}

	emptyValAcc := accounts.NewAccount()
	emptyAccBytes := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyAccBytes)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3AccBytes := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3AccBytes)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateAccBytes := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateAccBytes)

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	//
	//block6Expected := &changeset.ChangeSet{
	//	Changes: make([]changeset.Change, 0),
	//}
	//


	writeBlockData(t, tds, 3, []accData{
		{
			addr:   addrs[0],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
		{
			addr:   addrs[2],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
		{
			addr:   addrs[3],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
	})

	writeBlockData(t, tds, 5, []accData{
		{
			addr:   addrs[0],
			oldVal: block3ValAcc,
			newVal: stateValAcc,
		},
		{
			addr:   addrs[1],
			oldVal: &emptyValAcc,
			newVal: stateValAcc,
		},
		{
			addr:   addrs[3],
			oldVal: block3ValAcc,
			newVal: nil,
		},
	})

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addrs[0],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			block3Val,
		},
		{
			addrs[2],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			block3Val,
		},
	})

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addrs[0],
			changeset.DefaultIncarnation,
			key,
			block3Val,
			stateVal,
		},
		{
			addrs[1],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			changeset.DefaultIncarnation,
			key,
			block3Val,
			emptyVal,
		},
	})

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	var err error
	tx, err1 := db.KV().Begin(context.Background(), nil, ethdb.RO)
	if err1 != nil {
		t.Fatalf("create tx: %v", err1)
	}
	defer tx.Rollback()

	err=WalkThroughState(context.Background(), tx, 2, func(k, v []byte) (bool, error) {
			fmt.Println(common.Bytes2Hex(k), common.Bytes2Hex(v))
			err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)


	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err=WalkThroughState(context.Background(), tx, 4, func(k, v []byte) (bool, error) {
		//fmt.Println(common.Bytes2Hex(k), common.Bytes2Hex(v))
		block4.Changes = append(block4.Changes, changeset.Change{
			Key:   common.CopyBytes(k),
			Value:  common.CopyBytes(v),
		})
		return true, nil
	})
	fmt.Println("+block4")
	for _,v:=range block4.Changes {
		fmt.Println(common.Bytes2Hex(v.Key), common.Bytes2Hex(v.Value))
	}
	fmt.Println("-block4")
	block4Expected.Changes = []changeset.Change{
		{
			Key:   addrs[0].Bytes(),
			Value: block3AccBytes,
		},
		{
			Key:   append(addrs[0].Bytes(), key.Bytes()...),
			Value: block3Val.Bytes(),
		},
		{
			Key:   addrs[2].Bytes(),
			Value: block3AccBytes,
		},
		{
			Key:   append(addrs[2].Bytes(), key.Bytes()...),
			Value: stateVal.Bytes(),
		},
		{
			Key:   addrs[3].Bytes(),
			Value: block3AccBytes,
		},
		{
			Key:   append(addrs[3].Bytes(), key.Bytes()...),
			Value: block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	//for _, addr := range addrs {
	//	if err = state.WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 4, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
	//		err = block4.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//		return true, nil
	//	}); err != nil {
	//		t.Fatal(err)
	//	}
	//}
	//
	//block4Expected.Changes = []changeset.Change{
	//	{
	//		Key:   withoutInc(addrs[0], key),
	//		Value: block3Val.Bytes(),
	//	},
	//	{
	//		Key:   withoutInc(addrs[2], key),
	//		Value: stateVal.Bytes(),
	//	},
	//	{
	//		Key:   withoutInc(addrs[3], key),
	//		Value: block3Val.Bytes(),
	//	},
	//}
	//assertChangesEquals(t, block4, block4Expected)
	//
	//block6 := &changeset.ChangeSet{
	//	Changes: make([]changeset.Change, 0),
	//}
	//for _, addr := range addrs {
	//	if err = state.WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 6, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
	//		err = block6.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//		return true, nil
	//	}); err != nil {
	//		t.Fatal(err)
	//	}
	//}
	//
	//block6Expected.Changes = []changeset.Change{
	//	{
	//		Key:   withoutInc(addrs[0], key),
	//		Value: stateVal.Bytes(),
	//	},
	//	{
	//		Key:   withoutInc(addrs[1], key),
	//		Value: stateVal.Bytes(),
	//	},
	//	{
	//		Key:   withoutInc(addrs[2], key),
	//		Value: stateVal.Bytes(),
	//	},
	//}
	//assertChangesEquals(t, block6, block6Expected)
}
//
//func TestWalkAsOfAccountPlain(t *testing.T) {
//	db := ethdb.NewMemDatabase()
//	defer db.Close()
//	tds := state.NewTrieDbState(common.Hash{}, db, 1)
//	emptyValAcc := accounts.NewAccount()
//	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
//	emptyValAcc.EncodeForStorage(emptyVal)
//
//	block3ValAcc := emptyValAcc.SelfCopy()
//	block3ValAcc.Nonce = 3
//	block3ValAcc.Initialised = true
//	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
//	block3ValAcc.EncodeForStorage(block3Val)
//
//	stateValAcc := emptyValAcc.SelfCopy()
//	stateValAcc.Nonce = 5
//	stateValAcc.Initialised = true
//	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
//	stateValAcc.EncodeForStorage(stateVal)
//
//	numOfAccounts := uint8(4)
//	addrs := make([]common.Address, numOfAccounts)
//	addrHashes := make([]common.Hash, numOfAccounts)
//	for i := uint8(0); i < numOfAccounts; i++ {
//		addrs[i] = common.Address{i + 1}
//		addrHash, _ := common.HashData(addrs[i].Bytes())
//		addrHashes[i] = addrHash
//	}
//
//	block2 := &changeset.ChangeSet{
//		Changes: make([]changeset.Change, 0),
//	}
//
//	block2Expected := &changeset.ChangeSet{
//		Changes: make([]changeset.Change, 0),
//	}
//
//	writeBlockData(t, tds, 3, []accData{
//		{
//			addr:   addrs[0],
//			oldVal: &emptyValAcc,
//			newVal: block3ValAcc,
//		},
//		{
//			addr:   addrs[2],
//			oldVal: &emptyValAcc,
//			newVal: block3ValAcc,
//		},
//		{
//			addr:   addrs[3],
//			oldVal: &emptyValAcc,
//			newVal: block3ValAcc,
//		},
//	})
//
//	writeBlockData(t, tds, 5, []accData{
//		{
//			addr:   addrs[0],
//			oldVal: block3ValAcc,
//			newVal: stateValAcc,
//		},
//		{
//			addr:   addrs[1],
//			oldVal: &emptyValAcc,
//			newVal: stateValAcc,
//		},
//		{
//			addr:   addrs[3],
//			oldVal: block3ValAcc,
//			newVal: nil,
//		},
//	})
//
//	tx, err1 := db.KV().Begin(context.Background(), nil, ethdb.RO)
//	if err1 != nil {
//		t.Fatalf("create tx: %v", err1)
//	}
//	defer tx.Rollback()
//	if err := state.WalkAsOfAccounts(tx, common.Address{}, 2, func(k []byte, v []byte) (b bool, e error) {
//		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
//		if innerErr != nil {
//			t.Fatal(innerErr)
//		}
//		return true, nil
//	}); err != nil {
//		t.Fatal(err)
//	}
//	assertChangesEquals(t, block2, block2Expected)
//
//	block4 := &changeset.ChangeSet{
//		Changes: make([]changeset.Change, 0),
//	}
//
//	block4Expected := &changeset.ChangeSet{
//		Changes: []changeset.Change{
//			{
//				Key:   addrs[0].Bytes(),
//				Value: block3Val,
//			},
//			{
//				Key:   addrs[2].Bytes(),
//				Value: block3Val,
//			},
//			{
//				Key:   addrs[3].Bytes(),
//				Value: block3Val,
//			},
//		},
//	}
//
//	if err := state.WalkAsOfAccounts(tx, common.Address{}, 4, func(k []byte, v []byte) (b bool, e error) {
//		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
//		if innerErr != nil {
//			t.Fatal(innerErr)
//		}
//		return true, nil
//	}); err != nil {
//		t.Fatal(err)
//	}
//	assertChangesEquals(t, block4, block4Expected)
//
//	block6 := &changeset.ChangeSet{
//		Changes: make([]changeset.Change, 0),
//	}
//
//	block6Expected := &changeset.ChangeSet{
//		Changes: []changeset.Change{
//			{
//				Key:   addrs[0].Bytes(),
//				Value: stateVal,
//			},
//			{
//				Key:   addrs[1].Bytes(),
//				Value: stateVal,
//			},
//			{
//				Key:   addrs[2].Bytes(),
//				Value: block3Val,
//			},
//		},
//	}
//
//	if err := state.WalkAsOfAccounts(tx, common.Address{}, 6, func(k []byte, v []byte) (b bool, e error) {
//		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
//		if innerErr != nil {
//			t.Fatal(innerErr)
//		}
//		return true, nil
//	}); err != nil {
//		t.Fatal(err)
//	}
//	assertChangesEquals(t, block6, block6Expected)
//}
//
//func TestWalkAsOfAccountPlain_WithChunks(t *testing.T) {
//	db := ethdb.NewMemDatabase()
//	defer db.Close()
//	tds := state.NewTrieDbState(common.Hash{}, db, 1)
//	emptyValAcc := accounts.NewAccount()
//	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
//	emptyValAcc.EncodeForStorage(emptyVal)
//
//	block3ValAcc := emptyValAcc.SelfCopy()
//	block3ValAcc.Nonce = 3
//	block3ValAcc.Initialised = true
//	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
//	block3ValAcc.EncodeForStorage(block3Val)
//
//	stateValAcc := emptyValAcc.SelfCopy()
//	stateValAcc.Nonce = 5
//	stateValAcc.Initialised = true
//	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
//	stateValAcc.EncodeForStorage(stateVal)
//
//	numOfAccounts := uint8(4)
//	addrs := make([]common.Address, numOfAccounts)
//	addrHashes := make([]common.Hash, numOfAccounts)
//	for i := uint8(0); i < numOfAccounts; i++ {
//		addrs[i] = common.Address{i + 1}
//		addrHash, _ := common.HashData(addrs[i].Bytes())
//		addrHashes[i] = addrHash
//	}
//
//	addr1Old := emptyValAcc.SelfCopy()
//	addr1Old.Initialised = true
//	addr1Old.Nonce = 1
//	addr2Old := emptyValAcc.SelfCopy()
//	addr2Old.Initialised = true
//	addr2Old.Nonce = 1
//	addr3Old := emptyValAcc.SelfCopy()
//	addr3Old.Initialised = true
//	addr3Old.Nonce = 1
//
//	var addr1New, addr2New, addr3New *accounts.Account
//
//	writeBlockData(t, tds, 1, []accData{
//		{
//			addr:   addrs[0],
//			oldVal: &emptyValAcc,
//			newVal: addr1Old,
//		},
//		{
//			addr:   addrs[1],
//			oldVal: &emptyValAcc,
//			newVal: addr1Old,
//		},
//		{
//			addr:   addrs[2],
//			oldVal: &emptyValAcc,
//			newVal: addr1Old,
//		},
//	})
//
//	for i := 2; i < 1100; i++ {
//		addr1New = addr1Old.SelfCopy()
//		addr1New.Nonce = uint64(i)
//		addr2New = addr2Old.SelfCopy()
//		addr2New.Nonce = uint64(i)
//		addr3New = addr3Old.SelfCopy()
//		addr3New.Nonce = uint64(i)
//		writeBlockData(t, tds, uint64(i), []accData{
//			{
//				addr:   addrs[0],
//				oldVal: addr1Old,
//				newVal: addr1New,
//			},
//			{
//				addr:   addrs[1],
//				oldVal: addr2Old,
//				newVal: addr2New,
//			},
//			{
//				addr:   addrs[2],
//				oldVal: addr3Old,
//				newVal: addr3New,
//			},
//		})
//		addr1Old = addr1New.SelfCopy()
//		addr2Old = addr2New.SelfCopy()
//		addr3Old = addr3New.SelfCopy()
//	}
//
//	addr1New = addr1Old.SelfCopy()
//	addr1New.Nonce = 1100
//	addr2New = addr2Old.SelfCopy()
//	addr2New.Nonce = 1100
//	addr3New = addr3Old.SelfCopy()
//	addr3New.Nonce = 1100
//
//	writeBlockData(t, tds, 1100, []accData{
//		{
//			addr:   addrs[0],
//			oldVal: addr1Old,
//			newVal: addr1New,
//		},
//		{
//			addr:   addrs[1],
//			oldVal: addr1Old,
//			newVal: addr1New,
//		},
//		{
//			addr:   addrs[2],
//			oldVal: addr1Old,
//			newVal: addr1New,
//		},
//	})
//
//	tx, err1 := db.KV().Begin(context.Background(), nil, ethdb.RO)
//	if err1 != nil {
//		t.Fatalf("create tx: %v", err1)
//	}
//	defer tx.Rollback()
//	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
//		obtained := &changeset.ChangeSet{
//			Changes: make([]changeset.Change, 0),
//		}
//
//		if err := state.WalkAsOfAccounts(tx, common.Address{}, blockNum, func(k []byte, v []byte) (b bool, e error) {
//			innerErr := obtained.Add(common.CopyBytes(k), common.CopyBytes(v))
//			if innerErr != nil {
//				t.Fatal(innerErr)
//			}
//			return true, nil
//		}); err != nil {
//			t.Fatal(err)
//		}
//
//		acc := addr1Old.SelfCopy()
//		acc.Nonce = blockNum - 1
//		accBytes := make([]byte, acc.EncodingLengthForStorage())
//		acc.EncodeForStorage(accBytes)
//		expected := &changeset.ChangeSet{
//			Changes: []changeset.Change{
//				{
//					Key:   addrs[0].Bytes(),
//					Value: accBytes,
//				},
//				{
//					Key:   addrs[1].Bytes(),
//					Value: accBytes,
//				},
//				{
//					Key:   addrs[2].Bytes(),
//					Value: accBytes,
//				},
//			},
//		}
//		assertChangesEquals(t, obtained, expected)
//	}
//}
//
//func TestWalkAsOfStoragePlain_WithChunks(t *testing.T) {
//	db := ethdb.NewMemDatabase()
//	defer db.Close()
//	tds := state.NewTrieDbState(common.Hash{}, db, 1)
//
//	numOfAccounts := uint8(4)
//	addrs := make([]common.Address, numOfAccounts)
//	addrHashes := make([]common.Hash, numOfAccounts)
//	for i := uint8(0); i < numOfAccounts; i++ {
//		addrs[i] = common.Address{i + 1}
//		addrHash, _ := common.HashData(addrs[i].Bytes())
//		addrHashes[i] = addrHash
//	}
//	key := common.Hash{123}
//	emptyVal := uint256.NewInt()
//
//	val := uint256.NewInt().SetBytes([]byte("block 1"))
//	writeStorageBlockData(t, tds, 1, []storageData{
//		{
//			addr:   addrs[0],
//			inc:    1,
//			key:    key,
//			oldVal: emptyVal,
//			newVal: val,
//		},
//		{
//			addr:   addrs[1],
//			inc:    1,
//			key:    key,
//			oldVal: emptyVal,
//			newVal: val,
//		},
//		{
//			addr:   addrs[2],
//			inc:    1,
//			key:    key,
//			oldVal: emptyVal,
//			newVal: val,
//		},
//	})
//
//	prev := val
//	for i := 2; i < 1100; i++ {
//		val = uint256.NewInt().SetBytes([]byte("block " + strconv.Itoa(i)))
//		writeStorageBlockData(t, tds, uint64(i), []storageData{
//			{
//				addr:   addrs[0],
//				inc:    1,
//				key:    key,
//				oldVal: prev,
//				newVal: val,
//			},
//			{
//				addr:   addrs[1],
//				inc:    1,
//				key:    key,
//				oldVal: prev,
//				newVal: val,
//			},
//			{
//				addr:   addrs[2],
//				inc:    1,
//				key:    key,
//				oldVal: prev,
//				newVal: val,
//			},
//		})
//		prev = val
//	}
//
//	val = uint256.NewInt().SetBytes([]byte("block 1100"))
//
//	writeStorageBlockData(t, tds, 1100, []storageData{
//		{
//			addr:   addrs[0],
//			inc:    1,
//			key:    key,
//			oldVal: prev,
//			newVal: val,
//		},
//		{
//			addr:   addrs[1],
//			inc:    1,
//			key:    key,
//			oldVal: prev,
//			newVal: val,
//		},
//		{
//			addr:   addrs[2],
//			inc:    1,
//			key:    key,
//			oldVal: prev,
//			newVal: val,
//		},
//	})
//
//	tx, err1 := db.KV().Begin(context.Background(), nil, ethdb.RO)
//	if err1 != nil {
//		t.Fatalf("create tx: %v", err1)
//	}
//	defer tx.Rollback()
//	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
//		obtained := &changeset.ChangeSet{
//			Changes: make([]changeset.Change, 0),
//		}
//
//		for _, addr := range addrs {
//			if err := state.WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, blockNum, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
//				if innerErr := obtained.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v)); innerErr != nil {
//					t.Fatal(innerErr)
//				}
//				return true, nil
//			}); err != nil {
//				t.Fatal(err)
//			}
//		}
//		valBytes := uint256.NewInt().SetBytes([]byte("block " + strconv.FormatUint(blockNum-1, 10))).Bytes()
//		expected := &changeset.ChangeSet{
//			Changes: []changeset.Change{
//				{
//					Key:   append(addrs[0].Bytes(), key.Bytes()...),
//					Value: valBytes,
//				},
//				{
//					Key:   append(addrs[1].Bytes(), key.Bytes()...),
//					Value: valBytes,
//				},
//				{
//					Key:   append(addrs[2].Bytes(), key.Bytes()...),
//					Value: valBytes,
//				},
//			},
//		}
//		assertChangesEquals(t, obtained, expected)
//	}
//}

type accData struct {
	addr   common.Address
	oldVal *accounts.Account
	newVal *accounts.Account
}

func writeBlockData(t *testing.T, tds *state.TrieDbState, blockNum uint64, data []accData) {
	tds.SetBlockNr(blockNum)
	var blockWriter = tds.PlainStateWriter()

	for i := range data {
		if data[i].newVal != nil {
			if err := blockWriter.UpdateAccountData(context.Background(), data[i].addr, data[i].oldVal, data[i].newVal); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := blockWriter.DeleteAccount(context.Background(), data[i].addr, data[i].oldVal); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}
}

type storageData struct {
	addr   common.Address
	inc    uint64
	key    common.Hash
	oldVal *uint256.Int
	newVal *uint256.Int
}

func writeStorageBlockData(t *testing.T, tds *state.TrieDbState, blockNum uint64, data []storageData) {
	tds.SetBlockNr(blockNum)
	var blockWriter = tds.PlainStateWriter()

	for i := range data {
		if err := blockWriter.WriteAccountStorage(context.Background(),
			data[i].addr,
			data[i].inc,
			&data[i].key,
			data[i].oldVal,
			data[i].newVal); err != nil {
			t.Fatal(err)
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}
}
func assertChangesEquals(t *testing.T, changesObtained, changesExpected *changeset.ChangeSet) {
	t.Helper()
	sort.Sort(changesObtained)
	sort.Sort(changesExpected)
	if !reflect.DeepEqual(changesObtained, changesExpected) {
		fmt.Println("expected:")
		fmt.Println(changesExpected.String())
		fmt.Println("obtained:")
		fmt.Println(changesObtained.String())
		t.Fatal("block result is incorrect")
	}
}
