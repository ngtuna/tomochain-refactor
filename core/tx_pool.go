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

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core"

	"github.com/tomochain/tomochain/common"
)

type TomoTxPool struct {
	core.TxPool
}

// validateTx checks whether a transaction is valid according to the consensus
// rules and adheres to some heuristic limits of the local node (price and size).
func (pool *TomoTxPool) validateTx(tx *types.Transaction, local bool) error {
	// Heuristic limit, reject transactions over 32KB to prevent DOS attacks
	if tx.Size() > 32*1024 {
		return core.ErrOversizedData
	}
	// Transactions can't be negative. This may never happen using RLP decoded
	// transactions but may occur if you create a transaction using the RPC.
	if tx.Value().Sign() < 0 {
		return core.ErrNegativeValue
	}
	// Ensure the transaction doesn't exceed the current block limit gas.
	if pool.CurrentMaxGas < tx.Gas() {
		return core.ErrGasLimit
	}
	// Make sure the transaction is signed properly
	from, err := types.Sender(pool.Signer, tx)
	if err != nil {
		return core.ErrInvalidSender
	}
	// Drop non-local transactions under our own minimal accepted gas price
	local = local || pool.Locals.Contains(from) // account may be local even if the transaction arrived from the network
	if !local && pool.GasPrice.Cmp(tx.GasPrice()) > 0 {
		return core.ErrUnderpriced
	}
	// Ensure the transaction adheres to nonce ordering
	if pool.CurrentState.GetNonce(from) > tx.Nonce() {
		return core.ErrNonceTooLow
	}
	// Transactor should have enough funds to cover the costs
	// cost == V + GP * GL
	if pool.CurrentState.GetBalance(from).Cmp(tx.Cost()) < 0 {
		return core.ErrInsufficientFunds
	}
	if tx.To() != nil && tx.To().String() != common.BlockSigners {
		intrGas, err := core.IntrinsicGas(tx.Data(), tx.To() == nil, pool.Homestead)
		if err != nil {
			return err
		}
		// Exclude check smart contract sign address.
		if tx.Gas() < intrGas {
			return core.ErrIntrinsicGas
		}
	}
	return nil
}
