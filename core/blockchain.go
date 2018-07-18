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

// Package core implements the Ethereum consensus protocol.
package core

import (
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/core"
)

var (
	Checkpoint = make(chan int)
)

type TomoBlockChain struct {
	core.BlockChain
}

// insertChain will execute the actual chain insertion and event aggregation. The
// only reason this method exists as a separate one is to make locking cleaner
// with deferred statements.
func (bc *TomoBlockChain) insertChain(chain types.Blocks) (int, []interface{}, []*types.Log, error) {
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		if chain[i].NumberU64() != chain[i-1].NumberU64()+1 || chain[i].ParentHash() != chain[i-1].Hash() {
			// Chain broke ancestry, log a messge (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", chain[i].Number(), "hash", chain[i].Hash(),
				"parent", chain[i].ParentHash(), "prevnumber", chain[i-1].Number(), "prevhash", chain[i-1].Hash())

			return 0, nil, nil, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, chain[i-1].NumberU64(),
				chain[i-1].Hash().Bytes()[:4], i, chain[i].NumberU64(), chain[i].Hash().Bytes()[:4], chain[i].ParentHash().Bytes()[:4])
		}
	}
	// Pre-checks passed, start the full block imports
	bc.Wg.Add(1)
	defer bc.Wg.Done()

	bc.Chainmu.Lock()
	defer bc.Chainmu.Unlock()

	// A queued approach to delivering events. This is generally
	// faster than direct delivery and requires much less mutex
	// acquiring.
	var (
		stats         = core.InsertStats{StartTime: mclock.Now()}
		events        = make([]interface{}, 0, len(chain))
		lastCanon     *types.Block
		coalescedLogs []*types.Log
	)
	// Start the parallel header verifier
	headers := make([]*types.Header, len(chain))
	seals := make([]bool, len(chain))

	for i, block := range chain {
		headers[i] = block.Header()
		seals[i] = true
	}
	abort, results := bc.Engine.VerifyHeaders(bc, headers, seals)
	defer close(abort)

	// Iterate over the blocks and insert when the verifier permits
	for i, block := range chain {
		// If the chain is terminating, stop processing blocks
		if atomic.LoadInt32(&bc.ProcInterrupt) == 1 {
			log.Debug("Premature abort during blocks processing")
			break
		}
		// If the header is a banned one, straight out abort
		if core.BadHashes[block.Hash()] {
			bc.ReportBlock(block, nil, core.ErrBlacklistedHash)
			return i, events, coalescedLogs, core.ErrBlacklistedHash
		}
		// Wait for the block's verification to complete
		bstart := time.Now()

		err := <-results
		if err == nil {
			err = bc.GetValidator().ValidateBody(block)
		}
		switch {
		case err == core.ErrKnownBlock:
			// Block and state both already known. However if the current block is below
			// this number we did a rollback and we should reimport it nonetheless.
			if bc.GetCurrentBlock().NumberU64() >= block.NumberU64() {
				stats.Ignored++
				continue
			}

		case err == consensus.ErrFutureBlock:
			// Allow up to MaxFuture second in the future blocks. If this limit is exceeded
			// the chain is discarded and processed at a later time if given.
			max := big.NewInt(time.Now().Unix() + core.MaxTimeFutureBlocks)
			if block.Time().Cmp(max) > 0 {
				return i, events, coalescedLogs, fmt.Errorf("future block: %v > %v", block.Time(), max)
			}
			bc.FutureBlocks.Add(block.Hash(), block)
			stats.Queued++
			continue

		case err == consensus.ErrUnknownAncestor && bc.FutureBlocks.Contains(block.ParentHash()):
			bc.FutureBlocks.Add(block.Hash(), block)
			stats.Queued++
			continue

		case err == consensus.ErrPrunedAncestor:
			// Block competing with the canonical chain, store in the db, but don't process
			// until the competitor TD goes above the canonical TD
			currentBlock := bc.GetCurrentBlock()
			localTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
			externTd := new(big.Int).Add(bc.GetTd(block.ParentHash(), block.NumberU64()-1), block.Difficulty())
			if localTd.Cmp(externTd) > 0 {
				if err = bc.WriteBlockWithoutState(block, externTd); err != nil {
					return i, events, coalescedLogs, err
				}
				continue
			}
			// Competitor chain beat canonical, gather all blocks from the common ancestor
			var winner []*types.Block

			parent := bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
			for !bc.HasState(parent.Root()) {
				winner = append(winner, parent)
				parent = bc.GetBlock(parent.ParentHash(), parent.NumberU64()-1)
			}
			for j := 0; j < len(winner)/2; j++ {
				winner[j], winner[len(winner)-1-j] = winner[len(winner)-1-j], winner[j]
			}
			// Import all the pruned blocks to make the state available
			bc.Chainmu.Unlock()
			_, evs, logs, err := bc.insertChain(winner)
			bc.Chainmu.Lock()
			events, coalescedLogs = evs, logs

			if err != nil {
				return i, events, coalescedLogs, err
			}

		case err != nil:
			bc.ReportBlock(block, nil, err)
			return i, events, coalescedLogs, err
		}
		// Create a new statedb using the parent block and report an
		// error if it fails.
		var parent *types.Block
		if i == 0 {
			parent = bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
		} else {
			parent = chain[i-1]
		}
		state, err := state.New(parent.Root(), bc.StateCache)
		if err != nil {
			return i, events, coalescedLogs, err
		}
		// Process block using the parent state as reference point.
		receipts, logs, usedGas, err := bc.Processor.Process(block, state, bc.VmConfig)
		if err != nil {
			bc.ReportBlock(block, receipts, err)
			return i, events, coalescedLogs, err
		}
		// Validate the state using the default validator
		err = bc.GetValidator().ValidateState(block, parent, state, receipts, usedGas)
		if err != nil {
			bc.ReportBlock(block, receipts, err)
			return i, events, coalescedLogs, err
		}
		proctime := time.Since(bstart)

		// Write the block to the chain and get the status.
		status, err := bc.WriteBlockWithState(block, receipts, state)
		if err != nil {
			return i, events, coalescedLogs, err
		}
		switch status {
		case core.CanonStatTy:
			log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(), "uncles", len(block.Uncles()),
				"txs", len(block.Transactions()), "gas", block.GasUsed(), "elapsed", common.PrettyDuration(time.Since(bstart)))

			coalescedLogs = append(coalescedLogs, logs...)
			core.BlockInsertTimer.UpdateSince(bstart)
			events = append(events, core.ChainEvent{block, block.Hash(), logs})
			lastCanon = block

			// Only count canonical blocks for GC processing time
			bc.Gcproc += proctime

		case core.SideStatTy:
			log.Debug("Inserted forked block", "number", block.Number(), "hash", block.Hash(), "diff", block.Difficulty(), "elapsed",
				common.PrettyDuration(time.Since(bstart)), "txs", len(block.Transactions()), "gas", block.GasUsed(), "uncles", len(block.Uncles()))

			core.BlockInsertTimer.UpdateSince(bstart)
			events = append(events, core.ChainSideEvent{block})
		}
		stats.Processed++
		stats.UsedGas += usedGas
		stats.Report(chain, i, bc.StateCache.TrieDB().Size())
		if i == len(chain)-1 {
			if (bc.ChainConfig.Clique != nil) && (chain[i].NumberU64()%bc.ChainConfig.Clique.Epoch) == 0 {
				Checkpoint <- 1
			}
		}
	}
	// Append a single chain head event if we've progressed the chain
	if lastCanon != nil && bc.GetCurrentBlock().Hash() == lastCanon.Hash() {
		events = append(events, core.ChainHeadEvent{lastCanon})
	}
	return 0, events, coalescedLogs, nil
}
