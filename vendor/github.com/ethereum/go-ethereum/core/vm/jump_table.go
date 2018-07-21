// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/params"
)

type (
	executionFunc       func(pc *uint64, env *EVM, contract *Contract, memory *Memory, stack *Stack) ([]byte, error)
	gasFunc             func(params.GasTable, *EVM, *Contract, *Stack, *Memory, uint64) (uint64, error) // last parameter is the requested memory size as a uint64
	StackValidationFunc func(*Stack) error
	MemorySizeFunc func(*Stack) *big.Int
)

var ErrGasUintOverflow = errors.New("gas uint64 overflow")

type Operation struct {
	// op is the Operation function
	Execute executionFunc
	// GasCost is the gas function and Returns the gas required for execution
	GasCost gasFunc
	// ValidateStack validates the stack (size) for the Operation
	ValidateStack StackValidationFunc
	// MemorySize Returns the memory size required for the Operation
	MemorySize MemorySizeFunc

	Halts   bool // indicates whether the Operation should halt further execution
	Jumps   bool // indicates whether the program counter should not increment
	Writes  bool // determines whether this a state modifying Operation
	Valid   bool // indication whether the retrieved Operation is Valid and known
	Reverts bool // determines whether the Operation Reverts state (implicitly Halts)
	Returns bool // determines whether the operations sets the return data content
}

var (
	FrontierInstructionSet       = NewFrontierInstructionSet()
	HomesteadInstructionSet      = NewHomesteadInstructionSet()
	ByzantiumInstructionSet      = NewByzantiumInstructionSet()
	ConstantinopleInstructionSet = NewConstantinopleInstructionSet()
)

// NewConstantinopleInstructionSet Returns the frontier, homestead
// byzantium and contantinople instructions.
func NewConstantinopleInstructionSet() [256]Operation {
	// instructions that can be executed during the byzantium phase.
	instructionSet := NewByzantiumInstructionSet()
	instructionSet[SHL] = Operation{
		Execute:       opSHL,
		GasCost:       constGasFunc(GasFastestStep),
		ValidateStack: MakeStackFunc(2, 1),
		Valid:         true,
	}
	instructionSet[SHR] = Operation{
		Execute:       opSHR,
		GasCost:       constGasFunc(GasFastestStep),
		ValidateStack: MakeStackFunc(2, 1),
		Valid:         true,
	}
	instructionSet[SAR] = Operation{
		Execute:       opSAR,
		GasCost:       constGasFunc(GasFastestStep),
		ValidateStack: MakeStackFunc(2, 1),
		Valid:         true,
	}
	return instructionSet
}

// NewByzantiumInstructionSet Returns the frontier, homestead and
// byzantium instructions.
func NewByzantiumInstructionSet() [256]Operation {
	// instructions that can be executed during the homestead phase.
	instructionSet := NewHomesteadInstructionSet()
	instructionSet[STATICCALL] = Operation{
		Execute:       opStaticCall,
		GasCost:       gasStaticCall,
		ValidateStack: MakeStackFunc(6, 1),
		MemorySize:    memoryStaticCall,
		Valid:         true,
		Returns:       true,
	}
	instructionSet[RETURNDATASIZE] = Operation{
		Execute:       opReturnDataSize,
		GasCost:       constGasFunc(GasQuickStep),
		ValidateStack: MakeStackFunc(0, 1),
		Valid:         true,
	}
	instructionSet[RETURNDATACOPY] = Operation{
		Execute:       opReturnDataCopy,
		GasCost:       gasReturnDataCopy,
		ValidateStack: MakeStackFunc(3, 0),
		MemorySize:    memoryReturnDataCopy,
		Valid:         true,
	}
	instructionSet[REVERT] = Operation{
		Execute:       opRevert,
		GasCost:       gasRevert,
		ValidateStack: MakeStackFunc(2, 0),
		MemorySize:    memoryRevert,
		Valid:         true,
		Reverts:       true,
		Returns:       true,
	}
	return instructionSet
}

// NewHomesteadInstructionSet Returns the frontier and homestead
// instructions that can be executed during the homestead phase.
func NewHomesteadInstructionSet() [256]Operation {
	instructionSet := NewFrontierInstructionSet()
	instructionSet[DELEGATECALL] = Operation{
		Execute:       opDelegateCall,
		GasCost:       gasDelegateCall,
		ValidateStack: MakeStackFunc(6, 1),
		MemorySize:    memoryDelegateCall,
		Valid:         true,
		Returns:       true,
	}
	return instructionSet
}

// NewFrontierInstructionSet Returns the frontier instructions
// that can be executed during the frontier phase.
func NewFrontierInstructionSet() [256]Operation {
	return [256]Operation{
		STOP: {
			Execute:       opStop,
			GasCost:       constGasFunc(0),
			ValidateStack: MakeStackFunc(0, 0),
			Halts:         true,
			Valid:         true,
		},
		ADD: {
			Execute:       opAdd,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		MUL: {
			Execute:       opMul,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SUB: {
			Execute:       opSub,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		DIV: {
			Execute:       opDiv,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SDIV: {
			Execute:       opSdiv,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		MOD: {
			Execute:       opMod,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SMOD: {
			Execute:       opSmod,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		ADDMOD: {
			Execute:       opAddmod,
			GasCost:       constGasFunc(GasMidStep),
			ValidateStack: MakeStackFunc(3, 1),
			Valid:         true,
		},
		MULMOD: {
			Execute:       opMulmod,
			GasCost:       constGasFunc(GasMidStep),
			ValidateStack: MakeStackFunc(3, 1),
			Valid:         true,
		},
		EXP: {
			Execute:       opExp,
			GasCost:       gasExp,
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SIGNEXTEND: {
			Execute:       opSignExtend,
			GasCost:       constGasFunc(GasFastStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		LT: {
			Execute:       opLt,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		GT: {
			Execute:       opGt,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SLT: {
			Execute:       opSlt,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SGT: {
			Execute:       opSgt,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		EQ: {
			Execute:       opEq,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		ISZERO: {
			Execute:       opIszero,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		AND: {
			Execute:       opAnd,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		XOR: {
			Execute:       opXor,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		OR: {
			Execute:       opOr,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		NOT: {
			Execute:       opNot,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		BYTE: {
			Execute:       opByte,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(2, 1),
			Valid:         true,
		},
		SHA3: {
			Execute:       opSha3,
			GasCost:       gasSha3,
			ValidateStack: MakeStackFunc(2, 1),
			MemorySize:    memorySha3,
			Valid:         true,
		},
		ADDRESS: {
			Execute:       opAddress,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		BALANCE: {
			Execute:       opBalance,
			GasCost:       gasBalance,
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		ORIGIN: {
			Execute:       opOrigin,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		CALLER: {
			Execute:       opCaller,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		CALLVALUE: {
			Execute:       opCallValue,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		CALLDATALOAD: {
			Execute:       opCallDataLoad,
			GasCost:       constGasFunc(GasFastestStep),
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		CALLDATASIZE: {
			Execute:       opCallDataSize,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		CALLDATACOPY: {
			Execute:       opCallDataCopy,
			GasCost:       gasCallDataCopy,
			ValidateStack: MakeStackFunc(3, 0),
			MemorySize:    memoryCallDataCopy,
			Valid:         true,
		},
		CODESIZE: {
			Execute:       opCodeSize,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		CODECOPY: {
			Execute:       opCodeCopy,
			GasCost:       gasCodeCopy,
			ValidateStack: MakeStackFunc(3, 0),
			MemorySize:    memoryCodeCopy,
			Valid:         true,
		},
		GASPRICE: {
			Execute:       opGasprice,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		EXTCODESIZE: {
			Execute:       opExtCodeSize,
			GasCost:       gasExtCodeSize,
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		EXTCODECOPY: {
			Execute:       opExtCodeCopy,
			GasCost:       gasExtCodeCopy,
			ValidateStack: MakeStackFunc(4, 0),
			MemorySize:    memoryExtCodeCopy,
			Valid:         true,
		},
		BLOCKHASH: {
			Execute:       opBlockhash,
			GasCost:       constGasFunc(GasExtStep),
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		COINBASE: {
			Execute:       opCoinbase,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		TIMESTAMP: {
			Execute:       opTimestamp,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		NUMBER: {
			Execute:       opNumber,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		DIFFICULTY: {
			Execute:       opDifficulty,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		GASLIMIT: {
			Execute:       opGasLimit,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		POP: {
			Execute:       opPop,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(1, 0),
			Valid:         true,
		},
		MLOAD: {
			Execute:       opMload,
			GasCost:       gasMLoad,
			ValidateStack: MakeStackFunc(1, 1),
			MemorySize:    memoryMLoad,
			Valid:         true,
		},
		MSTORE: {
			Execute:       opMstore,
			GasCost:       gasMStore,
			ValidateStack: MakeStackFunc(2, 0),
			MemorySize:    memoryMStore,
			Valid:         true,
		},
		MSTORE8: {
			Execute:       opMstore8,
			GasCost:       gasMStore8,
			MemorySize:    memoryMStore8,
			ValidateStack: MakeStackFunc(2, 0),

			Valid: true,
		},
		SLOAD: {
			Execute:       opSload,
			GasCost:       gasSLoad,
			ValidateStack: MakeStackFunc(1, 1),
			Valid:         true,
		},
		SSTORE: {
			Execute:       opSstore,
			GasCost:       gasSStore,
			ValidateStack: MakeStackFunc(2, 0),
			Valid:         true,
			Writes:        true,
		},
		JUMP: {
			Execute:       opJump,
			GasCost:       constGasFunc(GasMidStep),
			ValidateStack: MakeStackFunc(1, 0),
			Jumps:         true,
			Valid:         true,
		},
		JUMPI: {
			Execute:       opJumpi,
			GasCost:       constGasFunc(GasSlowStep),
			ValidateStack: MakeStackFunc(2, 0),
			Jumps:         true,
			Valid:         true,
		},
		PC: {
			Execute:       opPc,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		MSIZE: {
			Execute:       opMsize,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		GAS: {
			Execute:       opGas,
			GasCost:       constGasFunc(GasQuickStep),
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		JUMPDEST: {
			Execute:       opJumpdest,
			GasCost:       constGasFunc(params.JumpdestGas),
			ValidateStack: MakeStackFunc(0, 0),
			Valid:         true,
		},
		PUSH1: {
			Execute:       makePush(1, 1),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH2: {
			Execute:       makePush(2, 2),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH3: {
			Execute:       makePush(3, 3),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH4: {
			Execute:       makePush(4, 4),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH5: {
			Execute:       makePush(5, 5),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH6: {
			Execute:       makePush(6, 6),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH7: {
			Execute:       makePush(7, 7),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH8: {
			Execute:       makePush(8, 8),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH9: {
			Execute:       makePush(9, 9),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH10: {
			Execute:       makePush(10, 10),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH11: {
			Execute:       makePush(11, 11),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH12: {
			Execute:       makePush(12, 12),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH13: {
			Execute:       makePush(13, 13),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH14: {
			Execute:       makePush(14, 14),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH15: {
			Execute:       makePush(15, 15),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH16: {
			Execute:       makePush(16, 16),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH17: {
			Execute:       makePush(17, 17),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH18: {
			Execute:       makePush(18, 18),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH19: {
			Execute:       makePush(19, 19),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH20: {
			Execute:       makePush(20, 20),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH21: {
			Execute:       makePush(21, 21),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH22: {
			Execute:       makePush(22, 22),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH23: {
			Execute:       makePush(23, 23),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH24: {
			Execute:       makePush(24, 24),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH25: {
			Execute:       makePush(25, 25),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH26: {
			Execute:       makePush(26, 26),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH27: {
			Execute:       makePush(27, 27),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH28: {
			Execute:       makePush(28, 28),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH29: {
			Execute:       makePush(29, 29),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH30: {
			Execute:       makePush(30, 30),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH31: {
			Execute:       makePush(31, 31),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		PUSH32: {
			Execute:       makePush(32, 32),
			GasCost:       gasPush,
			ValidateStack: MakeStackFunc(0, 1),
			Valid:         true,
		},
		DUP1: {
			Execute:       makeDup(1),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(1),
			Valid:         true,
		},
		DUP2: {
			Execute:       makeDup(2),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(2),
			Valid:         true,
		},
		DUP3: {
			Execute:       makeDup(3),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(3),
			Valid:         true,
		},
		DUP4: {
			Execute:       makeDup(4),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(4),
			Valid:         true,
		},
		DUP5: {
			Execute:       makeDup(5),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(5),
			Valid:         true,
		},
		DUP6: {
			Execute:       makeDup(6),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(6),
			Valid:         true,
		},
		DUP7: {
			Execute:       makeDup(7),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(7),
			Valid:         true,
		},
		DUP8: {
			Execute:       makeDup(8),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(8),
			Valid:         true,
		},
		DUP9: {
			Execute:       makeDup(9),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(9),
			Valid:         true,
		},
		DUP10: {
			Execute:       makeDup(10),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(10),
			Valid:         true,
		},
		DUP11: {
			Execute:       makeDup(11),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(11),
			Valid:         true,
		},
		DUP12: {
			Execute:       makeDup(12),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(12),
			Valid:         true,
		},
		DUP13: {
			Execute:       makeDup(13),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(13),
			Valid:         true,
		},
		DUP14: {
			Execute:       makeDup(14),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(14),
			Valid:         true,
		},
		DUP15: {
			Execute:       makeDup(15),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(15),
			Valid:         true,
		},
		DUP16: {
			Execute:       makeDup(16),
			GasCost:       gasDup,
			ValidateStack: MakeDupStackFunc(16),
			Valid:         true,
		},
		SWAP1: {
			Execute:       makeSwap(1),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(2),
			Valid:         true,
		},
		SWAP2: {
			Execute:       makeSwap(2),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(3),
			Valid:         true,
		},
		SWAP3: {
			Execute:       makeSwap(3),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(4),
			Valid:         true,
		},
		SWAP4: {
			Execute:       makeSwap(4),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(5),
			Valid:         true,
		},
		SWAP5: {
			Execute:       makeSwap(5),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(6),
			Valid:         true,
		},
		SWAP6: {
			Execute:       makeSwap(6),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(7),
			Valid:         true,
		},
		SWAP7: {
			Execute:       makeSwap(7),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(8),
			Valid:         true,
		},
		SWAP8: {
			Execute:       makeSwap(8),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(9),
			Valid:         true,
		},
		SWAP9: {
			Execute:       makeSwap(9),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(10),
			Valid:         true,
		},
		SWAP10: {
			Execute:       makeSwap(10),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(11),
			Valid:         true,
		},
		SWAP11: {
			Execute:       makeSwap(11),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(12),
			Valid:         true,
		},
		SWAP12: {
			Execute:       makeSwap(12),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(13),
			Valid:         true,
		},
		SWAP13: {
			Execute:       makeSwap(13),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(14),
			Valid:         true,
		},
		SWAP14: {
			Execute:       makeSwap(14),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(15),
			Valid:         true,
		},
		SWAP15: {
			Execute:       makeSwap(15),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(16),
			Valid:         true,
		},
		SWAP16: {
			Execute:       makeSwap(16),
			GasCost:       gasSwap,
			ValidateStack: MakeSwapStackFunc(17),
			Valid:         true,
		},
		LOG0: {
			Execute:       makeLog(0),
			GasCost:       makeGasLog(0),
			ValidateStack: MakeStackFunc(2, 0),
			MemorySize:    memoryLog,
			Valid:         true,
			Writes:        true,
		},
		LOG1: {
			Execute:       makeLog(1),
			GasCost:       makeGasLog(1),
			ValidateStack: MakeStackFunc(3, 0),
			MemorySize:    memoryLog,
			Valid:         true,
			Writes:        true,
		},
		LOG2: {
			Execute:       makeLog(2),
			GasCost:       makeGasLog(2),
			ValidateStack: MakeStackFunc(4, 0),
			MemorySize:    memoryLog,
			Valid:         true,
			Writes:        true,
		},
		LOG3: {
			Execute:       makeLog(3),
			GasCost:       makeGasLog(3),
			ValidateStack: MakeStackFunc(5, 0),
			MemorySize:    memoryLog,
			Valid:         true,
			Writes:        true,
		},
		LOG4: {
			Execute:       makeLog(4),
			GasCost:       makeGasLog(4),
			ValidateStack: MakeStackFunc(6, 0),
			MemorySize:    memoryLog,
			Valid:         true,
			Writes:        true,
		},
		CREATE: {
			Execute:       opCreate,
			GasCost:       gasCreate,
			ValidateStack: MakeStackFunc(3, 1),
			MemorySize:    memoryCreate,
			Valid:         true,
			Writes:        true,
			Returns:       true,
		},
		CALL: {
			Execute:       opCall,
			GasCost:       gasCall,
			ValidateStack: MakeStackFunc(7, 1),
			MemorySize:    memoryCall,
			Valid:         true,
			Returns:       true,
		},
		CALLCODE: {
			Execute:       opCallCode,
			GasCost:       gasCallCode,
			ValidateStack: MakeStackFunc(7, 1),
			MemorySize:    memoryCall,
			Valid:         true,
			Returns:       true,
		},
		RETURN: {
			Execute:       opReturn,
			GasCost:       gasReturn,
			ValidateStack: MakeStackFunc(2, 0),
			MemorySize:    memoryReturn,
			Halts:         true,
			Valid:         true,
		},
		SELFDESTRUCT: {
			Execute:       opSuicide,
			GasCost:       gasSuicide,
			ValidateStack: MakeStackFunc(1, 0),
			Halts:         true,
			Valid:         true,
			Writes:        true,
		},
	}
}
