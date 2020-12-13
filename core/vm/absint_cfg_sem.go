package vm

import "github.com/holiman/uint256"

type CfgOpSem struct {
	reverts          bool
	halts            bool
	isPush           bool
	isDup            bool
	isSwap           bool
	numBytes         int
	opNum            int
	numPush          int
	numPop           int
	isJump           bool
	opcode           OpCode
	stackReadIndices []int
}

type CfgAbsSem map[OpCode]*CfgOpSem

func NewCfgAbsSem() *CfgAbsSem {
	jt := newIstanbulInstructionSet()

	sem := CfgAbsSem{}

	for opcode, op := range jt {
		if op == nil {
			continue
		}
		opsem := CfgOpSem{}
		opsem.opcode = OpCode(opcode)
		opsem.reverts = op.reverts
		opsem.halts = op.halts
		opsem.isPush = op.isPush
		opsem.isDup = op.isDup
		opsem.isSwap = op.isSwap
		opsem.opNum = op.opNum
		opsem.numPush = op.numPush
		opsem.numPop = op.numPop
		opsem.isJump = opsem.opcode == JUMP || opsem.opcode == JUMPI
		//opsem.isExit = op.

		if opsem.isPush {
			opsem.numBytes = op.opNum + 1
		} else {
			opsem.numBytes = 1
		}

		if opsem.isDup {
			opsem.stackReadIndices = []int{opsem.opNum - 1}
		} else if opsem.isSwap {
			opsem.stackReadIndices = []int{0, opsem.opNum}
		} else { // need to check if this is correct. using approximation for now
			for i := 0; i < opsem.numPop; i++ {
				opsem.stackReadIndices = append(opsem.stackReadIndices, i)
			}
		}

		sem[opsem.opcode] = &opsem
	}

	return &sem
}

func (sem *CfgOpSem) getPushValue(code []byte, pc int) uint256.Int {
	pushByteSize := sem.opNum
	startMin := pc + 1
	if startMin >= len(code) {
		startMin = len(code)
	}
	endMin := startMin + pushByteSize
	if startMin+pushByteSize >= len(code) {
		endMin = len(code)
	}
	integer := new(uint256.Int)
	integer.SetBytes(code[startMin:endMin])
	return *integer
}