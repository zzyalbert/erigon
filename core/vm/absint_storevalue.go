package vm

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/logrusorgru/aurora"
	"golang.org/x/crypto/sha3"
	"log"
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
	blockList 	[]*block
}

type StorageFlowResult struct {
	IsStaticStateAccess bool
	Error               bool
	ErrorJumpToTop      bool
}

func (p *prog) isJumpDest(x *uint256.Int) bool {
	return x.IsUint64() && p.jumpDestPcs[int(x.Uint64())]
}

func (p *prog) print(instr2state map[*instr]*astate) {

	for _, block := range p.blockList {
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
			var vcolor aurora.Value
			if instr.opcode == SLOAD || instr.opcode == MSTORE || instr.opcode == MSTORE8 || instr.opcode == MLOAD || instr.opcode == SHA3 {
				vcolor = aurora.Cyan(vstr)
			} else {
				vcolor = aurora.Green(vstr)
			}

			succsstr := ""
			if instr.sem.isJump {
				succsstr = fmt.Sprintf("%v", succPcs)
			}

			var ststr string
			/*if (block.instrs[0] == instr || block.lastInstr() == instr) && instr2state[instr] != nil {
				ststr = "\n" + instr2state[instr].StringFull()
			}*/
			if instr2state[instr] != nil {
				ststr = instr2state[instr].String(true)
			}

			fmt.Printf("%3v %-25v %-10v %v\n", aurora.Yellow(instr.pc), vcolor, aurora.Magenta(succsstr), ststr)
		}

		fmt.Printf("\n")
	}
}

func isAtMostStatic(kind AbsValueKind) bool {
	return kind == ConcreteValue || kind == BotValue || kind == StaticValue
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
			succblk.prevs = append(succblk.prevs, block)

			if block.endPc + block.lastInstr().sem.numBytes == succblk.beginPc {
				block.fallThruSucc = succblk
			}
		}
		block.isExit = len(block.succs) == 0 || prfblk.IsInvalidJump
	}

	prog.blockList = make([]*block, 0)
	for _, block := range prog.blocks {
		prog.blockList = append(prog.blockList, block)
	}
	sort.SliceStable(prog.blockList, func(i, j int) bool {
		return prog.blockList[i].beginPc < prog.blockList[j].beginPc
	})

	return &prog
}


func apply(prog *prog, st0 *astate, x *instr) *astate {
	st1 := emptyState()

	for _, stack0 := range st0.stackset {
		stack1 := stack0.Copy()

		if !stack0.hasIndices(x.sem.numPop - 1) {
			continue
		}

		if !stack0.hasIndices(x.sem.stackReadIndices...) {
			continue
		}

		if x.sem.isPush {
			if true || prog.isJumpDest(x.value) || isFF(x.value) {
				stack1.Push(AbsValueConcrete(*x.value))
			} else {
				stack1.Push(AbsValueStatic())
			}
		} else if x.sem.isDup {
			value := stack1.values[x.sem.opNum-1]
			stack1.Push(value)
		} else if x.sem.isSwap {
			opNum := x.sem.opNum

			a := stack1.values[0]
			b := stack1.values[opNum]
			stack1.values[0] = b
			stack1.values[opNum] = a

		} else if x.opcode == AND || x.opcode == SUB || x.opcode == ADD {
			a := stack1.Pop(0)
			b := stack1.Pop(0)

			if a.kind == ConcreteValue && b.kind == ConcreteValue {
				v := uint256.NewInt()
				if x.opcode == AND {
					v.And(a.value, b.value)
				} else if x.opcode == SUB {
					v.Sub(a.value, b.value)
				} else if x.opcode == ADD {
					v.Add(a.value, b.value)
				} else {
					log.Fatal("bad opcode")
				}
				stack1.Push(AbsValueConcrete(*v))
			} else if isAtMostStatic(a.kind) && isAtMostStatic(b.kind) {
				stack1.Push(AbsValueStatic())
			} else {
				stack1.Push(AbsValueTop(0))
			}
		} else if x.opcode == PC {
			v := uint256.NewInt()
			v.SetUint64(uint64(x.pc))
			stack1.Push(AbsValueConcrete(*v))
		} else if x.opcode == SLOAD {
			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			stack1.Push(AbsValueTop(0))
		} else if x.opcode == MSTORE || x.opcode == MSTORE8 {
			offset := stack1.values[0]
			store := stack1.values[1]

			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}


			if stack1.memory == nil {
				//memory stays top
			} else if offset.kind != ConcreteValue { //offset is symbolic
				//memory becomes top
				stack1.SetMemoryTop()
			} else {
				if !offset.value.IsUint64() {
					log.Fatal("high offset")
				}

				if x.opcode == MSTORE {
					if store.kind == ConcreteValue {
						for i, b := range store.value.Bytes32() {
							bv := uint256.NewInt()
							bv.SetBytes([]byte{b})
							stack1.SetMemory(offset.value.Uint64()+uint64(i), AbsValueConcrete(*bv))
						}
					} else if store.kind == StaticValue {
						for i := 0; i < 32; i++ {
							stack1.SetMemory(offset.value.Uint64()+uint64(i), AbsValueStatic())
						}
					} else if store.kind == TopValue {
						for i := 0; i < 32; i++ {
							stack1.SetMemory(offset.value.Uint64()+uint64(i), AbsValueTop(0))
						}
					} else {
						log.Fatal("invalid value")
					}
				} else if x.opcode == MSTORE8 {
					if store.kind == ConcreteValue {
						bv := uint256.NewInt()
						if store.value == nil {
							log.Fatal("store is nil")
						}
						stval := store.value.Bytes32()
						stval0 := stval[0]
						bv.SetBytes([]byte{stval0})
						stack1.SetMemory(offset.value.Uint64(), AbsValueConcrete(*bv))
					} else if store.kind == StaticValue {
						stack1.SetMemory(offset.value.Uint64(), AbsValueStatic())
					}else if store.kind == TopValue {
						stack1.SetMemory(offset.value.Uint64(), AbsValueTop(0))
					} else {
						log.Fatal("invalid value")
					}
				} else {
					log.Fatal("bad opcode")
				}
			}
		} else if x.opcode == MLOAD { //memory readers
			offset := stack1.values[0]

			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			if stack1.memory == nil {
				stack1.Push(AbsValueTop(0))
			} else if offset.kind != ConcreteValue { //offset is symbolic
				stack1.Push(AbsValueTop(0))
			} else {
				if !offset.value.IsUint64() {
					log.Fatal("high offset")
				}

				var barr []byte
				isTop := false
				isStatic := false

				//fmt.Printf("state: %v\n", stack1.String(true))
				for i := 0; i < 32; i++ {
					offseti := offset.value.Uint64() + uint64(i)
					av := stack1.memory[offseti]
					//fmt.Printf("lookup: %v=%v\n", offseti, av.String(true))
					var b byte

					if av.kind == BotValue {
						b = 0
					} else if av.kind == ConcreteValue {
						b = av.value.Bytes32()[31]
					} else if av.kind == StaticValue {
						isStatic = true
					} else if av.kind == TopValue {
						isTop = true
					} else {
						log.Fatal("invalid value")
					}
					barr = append(barr, b)
				}

				if isTop {
					stack1.Push(AbsValueTop(0))
				} else if isStatic {
					stack1.Push(AbsValueStatic())
				} else {
					iv := uint256.NewInt()
					iv.SetBytes(barr)
					stack1.Push(AbsValueConcrete(*iv))
				}
			}

		} else if x.opcode == SHA3 {
			offset := stack1.values[0]
			length := stack1.values[1]

			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			if offset.kind == ConcreteValue && length.kind == ConcreteValue && length.value.IsUint64() {
				data := make([]byte, length.value.Uint64())
				isConcrete := true
				isStatic := true
				for i := 0; i < len(data); i++ {
					b := stack1.memory[uint64(i)]
					if b.kind != ConcreteValue {
						isConcrete = false
					}
					if b.kind != ConcreteValue && b.kind != StaticValue {
						isStatic = false
					}

					if b.kind == ConcreteValue {
						data[i] = b.value.Bytes32()[31]
					}
				}
				if isConcrete {
					buf := common.Hash{}
					hasher := sha3.NewLegacyKeccak256().(keccakState)
					hasher.Write(data)
					hasher.Read(buf[:])
					h := uint256.NewInt()
					h.SetBytes(buf[:])
					stack1.Push(AbsValueConcrete(*h))
				} else if isStatic {
					stack1.Push(AbsValueStatic())
				} else {
					stack1.Push(AbsValueTop(0))
				}
			} else {
				stack1.Push(AbsValueTop(0))
			}
		} else {
			allReadsStatic := true
			for _, i := range x.sem.stackReadIndices {
				if !isAtMostStatic(stack1.values[i].kind) {
					allReadsStatic = false
				}
			}

			for i := 0; i < x.sem.numPop; i++ {
				stack1.Pop(0)
			}

			for i := 0; i < x.sem.numPush; i++ {
				if allReadsStatic {
					stack1.Push(AbsValueStatic())
				} else {
					stack1.Push(AbsValueTop(0))
				}
			}
		}

		stack1.updateHash()

		st1.Add(stack1)
	}

	return st1
}

func StorageFlowAnalysis(code []byte, proof *CfgProof) StorageFlowResult {
	prog := toProg(code, proof)

	exit := make(map[*block]map[*block]*astate)
	instr2state := make(map[*instr]*astate)

	worklist := make([]*block, 0)
	for _, blk := range prog.blocks {
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
	var dynamicPcs []int
	for len(worklist) > 0 {

		block := worklist[0]
		worklist = worklist[1:]

		var st *astate
		if len(block.prevs) == 0 {
			st = initState()
		} else {
			st = emptyState()
			for _, prev := range block.prevs {
				st = Lub(st, exit[prev][block])
			}
		}

		for i := 0; i < len(block.instrs); i++ {
			instr := block.instrs[i]
			instr2state[instr] = st
			if isDynamicAccess(st, instr) {
				dynamicPcs = append(dynamicPcs, instr.pc)
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
						} else if elm0.kind == TopValue {
							result.Error = true
							result.ErrorJumpToTop = true
						}
					}
				}

				st = apply(prog, filtered, block.lastInstr())

				if !Eq(st, exit[block][succ]) {
					exit[block][succ] = st
					worklist = append(worklist, succ)
				}
			}
		}

		iterCount++

		if iterCount % 70 == 0 {
			//prog.print(instr2state)
			break
		}
	}

	prog.print(instr2state)

	if len(dynamicPcs) > 0 {
		fmt.Printf("dynamic pcs: %v\n", dynamicPcs)
	} else if result.Error {

	} else {
		result.IsStaticStateAccess = true
	}

	return result
}

func isDynamicAccess(st *astate, instr * instr) bool {
	if instr.opcode == SLOAD {
		for _, stack := range st.stackset {
			if len(stack.values) > 0 {
				if !isAtMostStatic(stack.values[0].kind) {
					return true
				}
			}
		}
	}

	return false
}
