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
	instrs  		[]*instr
	succs   		[]*block
	prevs   		[]*block
	beginPc 		int
	endPc   		int
	isExit  		bool
	fallThruSucc 	*block
}

func (b *block) lastInstr() *instr {
	return b.instrs[len(b.instrs)-1]
}

type prog struct {
	blocks 		map[int]*block	//entry block always first
	jumpDestPcs map[int]bool
}

type StorageFlowResult struct {
	IsStaticStateAccess   bool
}

func (p *prog) isJumpDest(x *uint256.Int) bool {
	return x.IsUint64() && p.jumpDestPcs[int(x.Uint64())]
}

func (p *prog) print(instr2state map[*instr]*astate) {
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

	prog := prog{
		blocks: make(map[int]*block),
		jumpDestPcs: make(map[int]bool),
	}

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

			if instr.opcode == JUMPDEST {
				prog.jumpDestPcs[instr.pc] = true
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

			if block.endPc + 1 == succblk.beginPc {
				block.fallThruSucc = succblk
			}
		}
		block.isExit = len(block.succs) == 0 || prfblk.IsInvalidJump
	}

	return &prog
}


func apply(prog *prog, st0 *astate, x *instr) *astate {
	st1 := emptyState()

	for _, stack0 := range st0.stackset {
		stack1 := stack0.Copy()

		if !stack0.hasIndices(x.sem.numPop - 1) {
			continue
		}

		if x.sem.isPush {
			if prog.isJumpDest(x.value) || isFF(x.value) {
				stack1.Push(AbsValueConcrete(*x.value))
			} else {
				stack1.Push(AbsValueStatic())
			}
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

		} else if x.opcode == AND {
			if !stack0.hasIndices(0, 1) {
				continue
			}

			a := stack1.Pop(0)
			b := stack1.Pop(0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt()
				v.And(a.value, b.value)
				stack1.Push(AbsValueConcrete(*v))
			} else {
				stack1.Push(AbsValueTop(0))
			}
		} else if x.opcode == PC {
			v := uint256.NewInt()
			v.SetUint64(uint64(x.pc))
			stack1.Push(AbsValueConcrete(*v))
		} else {
			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			for i := 0; i < x.sem.numPush; i++ {
				stack1.Push(AbsValueTop(0))
			}
		}

		stack1.updateHash()

		if len(stack1.values) <= 1024 {
			st1.Add(stack1)
		}
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
	exit := make(map[*block]map[*block]*astate)
	instr2state := make(map[*instr]*astate)

	worklist := make([]*block, 0)
	for _, blk := range prog.blocks {
		entry[blk] = emptyState()
		worklist = append(worklist, blk)

		exit[blk] = make(map[*block]*astate)
		for _, succ := range blk.succs {
			exit[blk][succ] = emptyState()
		}

		for _, instr := range blk.instrs {
			instr2state[instr] = emptyState()
		}
 	}

	iterCount := 0
	result := StorageFlowResult{}
	isDynamic := false
	for len(worklist) > 0 {
		/*if iterCount % 1000 == 0 {
			prog.print(instr2state)
		}*/

		block := worklist[0]
		worklist = worklist[1:]

		var st *astate
		if len(block.prevs) == 0 {
			st = initState()
		} else {
			st = emptyState()
			for _, prev := range block.prevs {
				st = Lub(st, exit[prev][block])
				//st = flatten(st)
			}
		}

		for i := 0; i < len(block.instrs); i++ {
			instr := block.instrs[i]
			instr2state[instr] = st
			if isDynamicAccess(st, instr) {
				isDynamic = true
				break
			}
			st = apply(prog, st, instr)
		}

		for _, succ := range block.succs {
			if block.fallThruSucc == succ {
				if !Eq(st, exit[block][succ]) {
					exit[block][succ] = st
					worklist = append(worklist, succ)
				}
			} else {
				prevst := instr2state[block.lastInstr()]

				filtered := emptyState()
				for _, stack := range prevst.stackset {
					if len(stack.values) > 0 {
						elm0 := stack.values[0]
						if elm0.kind == ConcreteValue && elm0.value.IsUint64() && int(elm0.value.Uint64()) == succ.beginPc {
							filtered.Add(stack)
						}
					}
				}
				st = apply(prog, prevst, block.lastInstr())
				//fmt.Printf("jump: %v->%v\n\tprevst: %v\n\tfilt: %v\n\tst: %v\n", block.endPc, succ.beginPc, prevst.String(true), filtered.String(true), st.String(true))

				if !Eq(st, exit[block][succ]) {
					exit[block][succ] = st
					worklist = append(worklist, succ)
				}
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
