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

type StorageFlowResult struct {
	IsStaticStateAccess   bool
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

			var ststr string
			if instr2state[instr] != nil {
				ststr = instr2state[instr].String(true)
			}

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
			stack1.Push(AbsValueStatic())
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

func flatten(st *astate) *astate {
	stf := emptyState()
	stf.stackset = append(stf.stackset, newStack())

	i := 0
	for true {
		lubv := AbsValueBot(0)

		foundStackElement := false
		for _, stack := range st.stackset {
			if i < len(stack.values) {
				v := stack.values[i]
				lubv = AbsValueLub(lubv, v)
				foundStackElement = true
			}
		}

		if !foundStackElement {
			break
		}

		stf.stackset[0].values = append(stf.stackset[0].values, lubv)
		i++
	}

	return stf
}

func StorageFlowAnalysis(code []byte, proof *CfgProof) StorageFlowResult {
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
	iterCount := 0
	result := StorageFlowResult{}
	isDynamic := false
	for len(worklist) > 0 {
		//fmt.Printf("worklist size: %v %v\n", len(worklist), iterCount)

		block := worklist[0]
		worklist = worklist[1:]

		var st *astate
		if len(block.prevs) == 0 {
			st = initState()
		} else {
			st = emptyState()
			for _, prev := range block.prevs {
				st = Lub(st, exit[prev])
				st = flatten(st)
			}
		}

		for _, instr := range block.instrs  {
			instr2state[instr] = st
			if isDynamicAccess(st, instr) {
				isDynamic = true
				break
			}
			st = apply(st, instr)
		}

		if !Eq(st, exit[block]) {
			//print("----------------")
			//fmt.Printf("%v\n", st.String(true))
			//fmt.Printf("%v\n", exit[block].String(true))
			//print("----------------")
			exit[block] = st
			for _, succ := range block.succs {
				worklist = append(worklist, succ)
			}
		}

		iterCount++
	}

	prog.print(instr2state)

	if !isDynamic {
		result.IsStaticStateAccess = true
	}

	return result
}

func isDynamicAccess(st *astate, instr * instr) bool {
	if instr.opcode == SLOAD {
		for _, stack := range st.stackset {
			if len(stack.values) > 0 {
				if  stack.values[0] != AbsValueStatic() {
					return true
				}
			}
		}
	}
	return false
}
