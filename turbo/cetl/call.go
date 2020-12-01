package cetl

/*
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
typedef int (*tg_blockhashes)(void* txn, uint64_t block_num);

int call_tg_blockhashes(void* func_ptr, void* txn, uint64_t block_num) {
	return ((tg_blockhashes)func_ptr)(txn, block_num);

}
*/
import "C"

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TransformBlockHashes(txn ethdb.Tx, startBlock uint64) {
	f, err := LoadCETLFunctionPointer()
	cStartBlock := C.uint64_t(startBlock)
	if err != nil {
		panic(err)
	}
	_ = C.call_tg_blockhashes(f, txn.CHandle(), cStartBlock)
}
