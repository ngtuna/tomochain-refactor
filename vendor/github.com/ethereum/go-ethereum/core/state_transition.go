// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"errors"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

var (
	ErrInsufficientBalanceForGas = errors.New("insufficient balance to pay for Gas")
)

/*
The State Transitioning Model

A State transition is a change made when a transaction is applied to the current world State
The State transitioning model does All All the necessary work to work out a valid new State root.

1) Nonce handling
2) Pre pay Gas
3) Create a new State object if the recipient is \0*32
4) GetValue transfer
== If contract creation ==
  4a) Attempt to run transaction Data
  4b) If valid, use result as code for the new State object
== end ==
5) Run Script section
6) Derive new State root
*/
type StateTransition struct {
	Gp         *GasPool
	Msg        Message
	Gas        uint64
	GasPrice   *big.Int
	InitialGas uint64
	Value      *big.Int
	Data       []byte
	State      vm.StateDB
	Evm        *vm.EVM
}

// Message represents a message sent to a contract.
type Message interface {
	From() common.Address
	//FromFrontier() (common.Address, error)
	To() *common.Address

	GasPrice() *big.Int
	Gas() uint64
	Value() *big.Int

	Nonce() uint64
	CheckNonce() bool
	Data() []byte
}

// IntrinsicGas computes the 'intrinsic Gas' for a message with the given Data.
func IntrinsicGas(data []byte, contractCreation, homestead bool) (uint64, error) {
	// Set the starting Gas for the raw transaction
	var gas uint64
	if contractCreation && homestead {
		gas = params.TxGasContractCreation
	} else {
		gas = params.TxGas
	}
	// Bump the required Gas by the amount of transactional Data
	if len(data) > 0 {
		// Zero and non-zero bytes are Priced differently
		var nz uint64
		for _, byt := range data {
			if byt != 0 {
				nz++
			}
		}
		// Make sure we don't exceed uint64 for All Data combinations
		if (math.MaxUint64-gas)/params.TxDataNonZeroGas < nz {
			return 0, vm.ErrOutOfGas
		}
		gas += nz * params.TxDataNonZeroGas

		z := uint64(len(data)) - nz
		if (math.MaxUint64-gas)/params.TxDataZeroGas < z {
			return 0, vm.ErrOutOfGas
		}
		gas += z * params.TxDataZeroGas
	}
	return gas, nil
}

// NewStateTransition initialises and returns a new State transition object.
func NewStateTransition(evm *vm.EVM, msg Message, gp *GasPool) *StateTransition {
	return &StateTransition{
		Gp:       gp,
		Evm:      evm,
		Msg:      msg,
		GasPrice: msg.GasPrice(),
		Value:    msg.Value(),
		Data:     msg.Data(),
		State:    evm.StateDB,
	}
}

// ApplyMessage computes the new State by applying the given message
// against the old State within the environment.
//
// ApplyMessage returns the bytes returned by any EVM execution (if it took place),
// the Gas used (which includes Gas refunds) and an error if it failed. An error always
// indicates a core error meaning that the message would always fail for that particular
// State and would never be accepted within a block.
func ApplyMessage(evm *vm.EVM, msg Message, gp *GasPool) ([]byte, uint64, bool, error) {
	return NewStateTransition(evm, msg, gp).TransitionDb()
}

func (st *StateTransition) from() vm.AccountRef {
	f := st.Msg.From()
	if !st.State.Exist(f) {
		st.State.CreateAccount(f)
	}
	return vm.AccountRef(f)
}

func (st *StateTransition) to() vm.AccountRef {
	if st.Msg == nil {
		return vm.AccountRef{}
	}
	to := st.Msg.To()
	if to == nil {
		return vm.AccountRef{} // contract creation
	}

	reference := vm.AccountRef(*to)
	if !st.State.Exist(*to) {
		st.State.CreateAccount(*to)
	}
	return reference
}

func (st *StateTransition) useGas(amount uint64) error {
	if st.Gas < amount {
		return vm.ErrOutOfGas
	}
	st.Gas -= amount

	return nil
}

func (st *StateTransition) buyGas() error {
	var (
		state  = st.State
		sender = st.from()
	)
	mgval := new(big.Int).Mul(new(big.Int).SetUint64(st.Msg.Gas()), st.GasPrice)
	if state.GetBalance(sender.Address()).Cmp(mgval) < 0 {
		return ErrInsufficientBalanceForGas
	}
	if err := st.Gp.SubGas(st.Msg.Gas()); err != nil {
		return err
	}
	st.Gas += st.Msg.Gas()

	st.InitialGas = st.Msg.Gas()
	state.SubBalance(sender.Address(), mgval)
	return nil
}

func (st *StateTransition) preCheck() error {
	msg := st.Msg
	sender := st.from()

	// Make sure this transaction's nonce is correct
	if msg.CheckNonce() {
		nonce := st.State.GetNonce(sender.Address())
		if nonce < msg.Nonce() {
			return ErrNonceTooHigh
		} else if nonce > msg.Nonce() {
			return ErrNonceTooLow
		}
	}
	return st.buyGas()
}

// TransitionDb will transition the State by applying the current message and
// returning the result including the the used Gas. It returns an error if it
// failed. An error indicates a consensus issue.
func (st *StateTransition) TransitionDb() (ret []byte, usedGas uint64, failed bool, err error) {
	if err = st.preCheck(); err != nil {
		return
	}
	msg := st.Msg
	sender := st.from() // err checked in preCheck

	homestead := st.Evm.ChainConfig().IsHomestead(st.Evm.BlockNumber)
	contractCreation := msg.To() == nil

	// Pay intrinsic Gas
	gas, err := IntrinsicGas(st.Data, contractCreation, homestead)
	if err != nil {
		return nil, 0, false, err
	}
	if err = st.useGas(gas); err != nil {
		return nil, 0, false, err
	}

	var (
		evm = st.Evm
		// vm errors do not effect consensus and are therefor
		// not assigned to err, except for insufficient balance
		// error.
		vmerr error
	)
	if contractCreation {
		ret, _, st.Gas, vmerr = evm.Create(sender, st.Data, st.Gas, st.Value)
	} else {
		// Increment the nonce for the next transaction
		st.State.SetNonce(sender.Address(), st.State.GetNonce(sender.Address())+1)
		ret, st.Gas, vmerr = evm.Call(sender, st.to().Address(), st.Data, st.Gas, st.Value)
	}
	if vmerr != nil {
		log.Debug("VM returned with error", "err", vmerr)
		// The only possible consensus-error would be if there wasn't
		// sufficient balance to make the transfer happen. The first
		// balance transfer may never fail.
		if vmerr == vm.ErrInsufficientBalance {
			return nil, 0, false, vmerr
		}
	}
	st.refundGas()
	st.State.AddBalance(st.Evm.Coinbase, new(big.Int).Mul(new(big.Int).SetUint64(st.gasUsed()), st.GasPrice))

	return ret, st.gasUsed(), vmerr != nil, err
}

func (st *StateTransition) refundGas() {
	// Apply refund counter, capped to half of the used Gas.
	refund := st.gasUsed() / 2
	if refund > st.State.GetRefund() {
		refund = st.State.GetRefund()
	}
	st.Gas += refund

	// Return ETH for remaining Gas, exchanged at the original rate.
	sender := st.from()

	remaining := new(big.Int).Mul(new(big.Int).SetUint64(st.Gas), st.GasPrice)
	st.State.AddBalance(sender.Address(), remaining)

	// Also return remaining Gas to the block Gas counter so it is
	// available for the next transaction.
	st.Gp.AddGas(st.Gas)
}

// gasUsed returns the amount of Gas used up by the State transition.
func (st *StateTransition) gasUsed() uint64 {
	return st.InitialGas - st.Gas
}
