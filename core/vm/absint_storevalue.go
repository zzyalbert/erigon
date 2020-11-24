package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/logrusorgru/aurora"
	"sort"
)

type instr struct {
	pc 		int
	opcode 	OpCode
	value  	*uint256.Int
	sem  	*CfgOpSem
}

type block struct {
	instrs  []*instr
	succs   []*block
	prevs   []*block
	beginPc int
	endPc   int
	isExit  bool
}

type prog struct {
	blocks map[int]*block	//entry block always first
}

func (p prog) print(instr2state map[*instr]*astate) {
	blockList := make([]*block, 0)
	for _, block := range p.blocks {
		blockList = append(blockList, block)
	}
	sort.SliceStable(blockList, func(i, j int) bool {
		return blockList[i].beginPc < blockList[j].beginPc
	})

	for _, block := range blockList {
		succPcs := make([]int, 0)
		for _, succblk := range block.succs {
			succPcs = append(succPcs, succblk.beginPc)
		}
		if block.isExit {
			succPcs = append(succPcs, -1)
		}

		for _, instr := range block.instrs {
			var vstr string
			if instr.value != nil {
				vstr = fmt.Sprintf("%v %v", instr.opcode, instr.value.Hex())
			} else {
				vstr = fmt.Sprintf("%v", instr.opcode)
			}

			succsstr := ""
			if instr.sem.isJump {
				succsstr = fmt.Sprintf("%v", succPcs)

			}

			ststr := instr2state[instr].String(true)

			fmt.Printf("%3v %-25v %-10v %v\n", aurora.Yellow(instr.pc), aurora.Green(vstr), aurora.Magenta(succsstr), ststr)
		}

		fmt.Printf("\n")
	}
}

func toProg(code []byte, proof *CfgProof) *prog {
	sem := NewCfgAbsSem()

	prog := prog{blocks: make(map[int]*block)}
	for _, prfblk := range proof.Blocks {
		block := block{beginPc: prfblk.Entry.Pc, endPc: prfblk.Exit.Pc}
		prog.blocks[block.beginPc] = &block

		for pc := prfblk.Entry.Pc; pc <= prfblk.Exit.Pc; {
			instr := instr{pc: pc}
			block.instrs = append(block.instrs, &instr)

			instr.opcode = OpCode(code[pc])

			instr.sem = (*sem)[instr.opcode]
			if instr.sem == nil {
				instr.opcode = REVERT //this may not be the right choice of semantics
				instr.sem = (*sem)[instr.opcode]
			}

			if instr.sem.isPush {
				value := instr.sem.getPushValue(code, pc)
				instr.value = &value
			}

			pc += instr.sem.numBytes
		}
	}

	for _, prfblk := range proof.Blocks {
		block := prog.blocks[prfblk.Entry.Pc]
		for _, succ := range prfblk.Succs {
			succblk := prog.blocks[succ]
			block.succs = append(block.succs, succblk)
			succblk.prevs = append(succblk.prevs, block)
		}
		block.isExit = len(block.succs) == 0 || prfblk.IsInvalidJump
	}

	return &prog
}


func apply(st0 *astate, x *instr) *astate {
	st1 := emptyState()

	for _, stack0 := range st0.stackset {
		stack1 := stack0.Copy()

		if !stack0.hasIndices(x.sem.numPop - 1) {
			continue
		}

		if x.sem.isPush {
			pushValue := *x.value
			stack1.Push(AbsValueConcrete(pushValue))
		} else if x.sem.isDup {
			if !stack0.hasIndices(x.sem.opNum - 1) {
				continue
			}

			value := stack1.values[x.sem.opNum-1]
			stack1.Push(value)
		} else if x.sem.isSwap {
			opNum := x.sem.opNum

			if !stack0.hasIndices(0, opNum) {
				continue
			}

			a := stack1.values[0]
			b := stack1.values[opNum]
			stack1.values[0] = b
			stack1.values[opNum] = a

		} else {
			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			for i := 0; i < x.sem.numPush; i++ {
				stack1.Push(AbsValueTop(0))
			}
		}

		stack1.updateHash()
		st1.Add(stack1)
	}

	return st1
}

func StorageFlowAnalysis(code []byte, proof *CfgProof) {
	prog := toProg(code, proof)

	entry := make(map[*block]*astate)
	exit := make(map[*block]*astate)
	worklist := make([]*block, 0)
	for _, block := range prog.blocks {
		entry[block] = emptyState()
		exit[block] = emptyState()
		worklist = append(worklist, block)
	}

	instr2state := make(map[*instr]*astate)
	for len(worklist) > 0 {
		block := worklist[0]
		worklist = worklist[1:]

		var st *astate
		if len(block.prevs) == 0 {
			st = initState()
		} else {
			st = emptyState()
			for _, prev := range block.prevs {
				st = Lub(st, exit[prev])
			}
		}

		for _, instr := range block.instrs  {
			instr2state[instr] = st
			st = apply(st, instr)
		}

		if !Eq(st, exit[block]) {
			exit[block] = st
			for _, succ := range block.succs {
				worklist = append(worklist, succ)
			}
		}
	}

	prog.print(instr2state)
}
