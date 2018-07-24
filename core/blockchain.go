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
	mrand "math/rand"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/tomochain/tomochain/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/core"
	"gopkg.in/karalabe/cookiejar.v2/collections/prque"
	"github.com/hashicorp/golang-lru"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/ethdb"
	"sync"
	"github.com/ethereum/go-ethereum/event"
	"io"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/rlp"
	"errors"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	Checkpoint = make(chan int)
)

type TomoBlockChain struct {
	ChainConfig *params.ChainConfig // Chain & network configuration
	CacheConfig *core.CacheConfig        // Cache configuration for pruning

	Db     ethdb.Database // Low level persistent database to store final content in
	Triegc *prque.Prque   // Priority Queue mapping block numbers to tries to gc
	Gcproc time.Duration  // Accumulates canonical block processing for trie dumping

	Hc            *HeaderChain
	RmLogsFeed    event.Feed
	ChainFeed     event.Feed
	ChainSideFeed event.Feed
	ChainHeadFeed event.Feed
	LogsFeed      event.Feed
	Scope         event.SubscriptionScope
	GenesisBlock  *types.Block

	Mu      sync.RWMutex // global mutex for locking Chain operations
	Chainmu sync.RWMutex // blockchain insertion lock
	Procmu  sync.RWMutex // block Processor lock

	Checkpoint       int          // Checkpoint counts towards the new Checkpoint
	CurrentBlock     atomic.Value // Current head of the block Chain
	CurrentFastBlock atomic.Value // Current head of the fast-sync Chain (may be above the block Chain!)

	StateCache   state.Database // State database to reuse between imports (Contains State cache)
	BodyCache    *lru.Cache     // Cache for the most recent block bodies
	BodyRLPCache *lru.Cache     // Cache for the most recent block bodies in RLP encoded format
	BlockCache   *lru.Cache     // Cache for the most recent entire blocks
	FutureBlocks *lru.Cache     // future blocks are blocks added for later processing

	Quit    chan struct{} // blockchain Quit channel
	Running int32         // Running must be called atomically
	// ProcInterrupt must be atomically called
	ProcInterrupt int32          // interrupt signaler for block processing
	Wg            sync.WaitGroup // Chain processing wait group for shutting down

	Engine    consensus.Engine
	Processor core.Processor // block Processor interface
	Validator core.Validator // block and State Validator interface
	VmConfig  vm.Config

	BadBlocks *lru.Cache // Bad block cache

}

func NewBlockChain(db ethdb.Database, cacheConfig *core.CacheConfig, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig vm.Config) (*TomoBlockChain, error) {
	if cacheConfig == nil {
		cacheConfig = &core.CacheConfig{
			TrieNodeLimit: 256 * 1024 * 1024,
			TrieTimeLimit: 5 * time.Minute,
		}
	}
	bodyCache, _ := lru.New(core.BodyCacheLimit)
	bodyRLPCache, _ := lru.New(core.BodyCacheLimit)
	blockCache, _ := lru.New(core.BlockCacheLimit)
	futureBlocks, _ := lru.New(core.MaxFutureBlocks)
	badBlocks, _ := lru.New(core.BadBlockLimit)

	bc := &TomoBlockChain{
		ChainConfig:  chainConfig,
		CacheConfig:  cacheConfig,
		Db:           db,
		Triegc:       prque.New(),
		StateCache:   state.NewDatabase(db),
		Quit:         make(chan struct{}),
		BodyCache:    bodyCache,
		BodyRLPCache: bodyRLPCache,
		BlockCache:   blockCache,
		FutureBlocks: futureBlocks,
		Engine:       engine,
		VmConfig:     vmConfig,
		BadBlocks:    badBlocks,
	}
	bc.SetValidator(NewBlockValidator(chainConfig, bc, engine))
	bc.SetProcessor(NewStateProcessor(chainConfig, bc, engine))

	var err error
	bc.Hc, err = NewHeaderChain(db, chainConfig, engine, bc.GetProcInterrupt)
	if err != nil {
		return nil, err
	}
	bc.GenesisBlock = bc.GetBlockByNumber(0)
	if bc.GenesisBlock == nil {
		return nil, core.ErrNoGenesis
	}
	if err := bc.LoadLastState(); err != nil {
		return nil, err
	}
	// Check the current State of the block hashes and make sure that we do not have any of the bad blocks in our Chain
	for hash := range core.BadHashes {
		if header := bc.GetHeaderByHash(hash); header != nil {
			// get the canonical block corresponding to the offending header's number
			headerByNumber := bc.GetHeaderByNumber(header.Number.Uint64())
			// make sure the headerByNumber (if present) is in our current canonical Chain
			if headerByNumber != nil && headerByNumber.Hash() == header.Hash() {
				log.Error("Found bad hash, rewinding Chain", "number", header.Number, "hash", header.ParentHash)
				bc.SetHead(header.Number.Uint64() - 1)
				log.Error("Chain rewind was successful, resuming normal operation")
			}
		}
	}
	// Take ownership of this particular State
	go bc.Update()
	return bc, nil
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




func (bc *TomoBlockChain) GetProcInterrupt() bool {
	return atomic.LoadInt32(&bc.ProcInterrupt) == 1
}

// LoadLastState loads the last known Chain State from the database. This method
// assumes that the Chain manager mutex is held.
func (bc *TomoBlockChain) LoadLastState() error {
	// Restore the last known head block
	head := core.GetHeadBlockHash(bc.Db)
	if head == (common.Hash{}) {
		// Corrupt or empty database, init from scratch
		log.Warn("Empty database, resetting Chain")
		return bc.Reset()
	}
	// Make sure the entire head block is available
	currentBlock := bc.GetBlockByHash(head)
	if currentBlock == nil {
		// Corrupt or empty database, init from scratch
		log.Warn("GetHead block missing, resetting Chain", "hash", head)
		return bc.Reset()
	}
	// Make sure the State associated with the block is available
	if _, err := state.New(currentBlock.Root(), bc.StateCache); err != nil {
		// Dangling block without a State associated, init from scratch
		log.Warn("GetHead State missing, repairing Chain", "number", currentBlock.Number(), "hash", currentBlock.Hash())
		if err := bc.repair(&currentBlock); err != nil {
			return err
		}
	}
	// Everything seems to be fine, set as the head block
	bc.CurrentBlock.Store(currentBlock)

	// Restore the last known head header
	currentHeader := currentBlock.Header()
	if head := core.GetHeadHeaderHash(bc.Db); head != (common.Hash{}) {
		if header := bc.GetHeaderByHash(head); header != nil {
			currentHeader = header
		}
	}
	bc.Hc.SetCurrentHeader(currentHeader)

	// Restore the last known head fast block
	bc.CurrentFastBlock.Store(currentBlock)
	if head := core.GetHeadFastBlockHash(bc.Db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.CurrentFastBlock.Store(block)
		}
	}

	// Issue a status log for the user
	currentFastBlock := bc.GetCurrentFastBlock()

	headerTd := bc.GetTd(currentHeader.Hash(), currentHeader.Number.Uint64())
	blockTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
	fastTd := bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64())

	log.Info("Loaded most recent local header", "number", currentHeader.Number, "hash", currentHeader.Hash(), "td", headerTd)
	log.Info("Loaded most recent local full block", "number", currentBlock.Number(), "hash", currentBlock.Hash(), "td", blockTd)
	log.Info("Loaded most recent local fast block", "number", currentFastBlock.Number(), "hash", currentFastBlock.Hash(), "td", fastTd)

	return nil
}

// SetHead rewinds the local Chain to a new head. In the case of headers, everything
// above the new head will be deleted and the new one set. In the case of blocks
// though, the head may be further rewound if block bodies are missing (non-archive
// nodes after a fast sync).
func (bc *TomoBlockChain) SetHead(head uint64) error {
	log.Warn("Rewinding blockchain", "target", head)

	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	// Rewind the header Chain, deleting All block bodies until then
	delFn := func(hash common.Hash, num uint64) {
		core.DeleteBody(bc.Db, hash, num)
	}
	bc.Hc.SetHead(head, delFn)
	currentHeader := bc.Hc.CurrentHeader()

	// Clear out any stale content from the caches
	bc.BodyCache.Purge()
	bc.BodyRLPCache.Purge()
	bc.BlockCache.Purge()
	bc.FutureBlocks.Purge()

	// Rewind the block Chain, ensuring we don't end up with a stateless head block
	if currentBlock := bc.GetCurrentBlock(); currentBlock != nil && currentHeader.Number.Uint64() < currentBlock.NumberU64() {
		bc.CurrentBlock.Store(bc.GetBlock(currentHeader.Hash(), currentHeader.Number.Uint64()))
	}
	if currentBlock := bc.GetCurrentBlock(); currentBlock != nil {
		if _, err := state.New(currentBlock.Root(), bc.StateCache); err != nil {
			// Rewound State missing, rolled back to before pivot, reset to genesis
			bc.CurrentBlock.Store(bc.GenesisBlock)
		}
	}
	// Rewind the fast block in a simpleton way to the target head
	if currentFastBlock := bc.GetCurrentFastBlock(); currentFastBlock != nil && currentHeader.Number.Uint64() < currentFastBlock.NumberU64() {
		bc.CurrentFastBlock.Store(bc.GetBlock(currentHeader.Hash(), currentHeader.Number.Uint64()))
	}
	// If either blocks reached nil, reset to the genesis State
	if currentBlock := bc.GetCurrentBlock(); currentBlock == nil {
		bc.CurrentBlock.Store(bc.GenesisBlock)
	}
	if currentFastBlock := bc.GetCurrentFastBlock(); currentFastBlock == nil {
		bc.CurrentFastBlock.Store(bc.GenesisBlock)
	}
	currentBlock := bc.GetCurrentBlock()
	currentFastBlock := bc.GetCurrentFastBlock()
	if err := core.WriteHeadBlockHash(bc.Db, currentBlock.Hash()); err != nil {
		log.Crit("Failed to reset head full block", "err", err)
	}
	if err := core.WriteHeadFastBlockHash(bc.Db, currentFastBlock.Hash()); err != nil {
		log.Crit("Failed to reset head fast block", "err", err)
	}
	return bc.LoadLastState()
}

// FastSyncCommitHead sets the current head block to the one defined by the hash
// irrelevant what the Chain contents were prior.
func (bc *TomoBlockChain) FastSyncCommitHead(hash common.Hash) error {
	// Make sure that both the block as well at its State trie exists
	block := bc.GetBlockByHash(hash)
	if block == nil {
		return fmt.Errorf("non existent block [%x…]", hash[:4])
	}
	if _, err := trie.NewSecure(block.Root(), bc.StateCache.TrieDB(), 0); err != nil {
		return err
	}
	// If All checks out, manually set the head block
	bc.Mu.Lock()
	bc.CurrentBlock.Store(block)
	bc.Mu.Unlock()

	log.Info("Committed new head block", "number", block.Number(), "hash", hash)
	return nil
}

// GasLimit returns the Gas limit of the current HEAD block.
func (bc *TomoBlockChain) GasLimit() uint64 {
	return bc.GetCurrentBlock().GasLimit()
}

// GetCurrentBlock retrieves the current head block of the canonical Chain. The
// block is retrieved from the blockchain's internal cache.
func (bc *TomoBlockChain) GetCurrentBlock() *types.Block {
	return bc.CurrentBlock.Load().(*types.Block)
}

// GetCurrentFastBlock retrieves the current fast-sync head block of the canonical
// Chain. The block is retrieved from the blockchain's internal cache.
func (bc *TomoBlockChain) GetCurrentFastBlock() *types.Block {
	return bc.CurrentFastBlock.Load().(*types.Block)
}

// SetProcessor sets the Processor required for making State modifications.
func (bc *TomoBlockChain) SetProcessor(processor core.Processor) {
	bc.Procmu.Lock()
	defer bc.Procmu.Unlock()
	bc.Processor = processor
}

// SetValidator sets the Validator which is used to validate incoming blocks.
func (bc *TomoBlockChain) SetValidator(validator core.Validator) {
	bc.Procmu.Lock()
	defer bc.Procmu.Unlock()
	bc.Validator = validator
}

// GetValidator returns the current Validator.
func (bc *TomoBlockChain) GetValidator() core.Validator {
	bc.Procmu.RLock()
	defer bc.Procmu.RUnlock()
	return bc.Validator
}

// GetProcessor returns the current Processor.
func (bc *TomoBlockChain) GetProcessor() core.Processor {
	bc.Procmu.RLock()
	defer bc.Procmu.RUnlock()
	return bc.Processor
}

// State returns a new mutable State based on the current HEAD block.
func (bc *TomoBlockChain) State() (*state.StateDB, error) {
	return bc.StateAt(bc.GetCurrentBlock().Root())
}

// StateAt returns a new mutable State based on a particular point in time.
func (bc *TomoBlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return state.New(root, bc.StateCache)
}

// Reset purges the entire blockchain, restoring it to its genesis State.
func (bc *TomoBlockChain) Reset() error {
	return bc.ResetWithGenesisBlock(bc.GenesisBlock)
}

// ResetWithGenesisBlock purges the entire blockchain, restoring it to the
// specified genesis State.
func (bc *TomoBlockChain) ResetWithGenesisBlock(genesis *types.Block) error {
	// Dump the entire block Chain and purge the caches
	if err := bc.SetHead(0); err != nil {
		return err
	}
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	// Prepare the genesis block and reinitialise the Chain
	if err := bc.Hc.WriteTd(genesis.Hash(), genesis.NumberU64(), genesis.Difficulty()); err != nil {
		log.Crit("Failed to write genesis block TD", "err", err)
	}
	if err := core.WriteBlock(bc.Db, genesis); err != nil {
		log.Crit("Failed to write genesis block", "err", err)
	}
	bc.GenesisBlock = genesis
	bc.insert(bc.GenesisBlock)
	bc.CurrentBlock.Store(bc.GenesisBlock)
	bc.Hc.SetGenesis(bc.GenesisBlock.Header())
	bc.Hc.SetCurrentHeader(bc.GenesisBlock.Header())
	bc.CurrentFastBlock.Store(bc.GenesisBlock)

	return nil
}

// repair tries to repair the current blockchain by rolling back the current block
// until one with associated State is found. This is needed to fix incomplete Db
// writes caused either by crashes/power outages, or simply non-committed tries.
//
// This method only rolls back the current block. The current header and current
// fast block are left intact.
func (bc *TomoBlockChain) repair(head **types.Block) error {
	for {
		// Abort if we've rewound to a head block that does have associated State
		if _, err := state.New((*head).Root(), bc.StateCache); err == nil {
			log.Info("Rewound blockchain to past State", "number", (*head).Number(), "hash", (*head).Hash())
			return nil
		}
		// Otherwise rewind one block and recheck State availability there
		(*head) = bc.GetBlock((*head).ParentHash(), (*head).NumberU64()-1)
	}
}

// Export writes the active Chain to the given writer.
func (bc *TomoBlockChain) Export(w io.Writer) error {
	return bc.ExportN(w, uint64(0), bc.GetCurrentBlock().NumberU64())
}

// ExportN writes a subset of the active Chain to the given writer.
func (bc *TomoBlockChain) ExportN(w io.Writer, first uint64, last uint64) error {
	bc.Mu.RLock()
	defer bc.Mu.RUnlock()

	if first > last {
		return fmt.Errorf("export failed: first (%d) is greater than last (%d)", first, last)
	}
	log.Info("Exporting batch of blocks", "count", last-first+1)

	for nr := first; nr <= last; nr++ {
		block := bc.GetBlockByNumber(nr)
		if block == nil {
			return fmt.Errorf("export failed on #%d: not found", nr)
		}

		if err := block.EncodeRLP(w); err != nil {
			return err
		}
	}

	return nil
}

// insert injects a new head block into the current block Chain. This method
// assumes that the block is indeed a true head. It will also reset the head
// header and the head fast sync block to this very same block if they are older
// or if they are on a different side Chain.
//
// Note, this function assumes that the `Mu` mutex is held!
func (bc *TomoBlockChain) insert(block *types.Block) {
	// If the block is on a side Chain or an unknown one, force other heads onto it too
	updateHeads := core.GetCanonicalHash(bc.Db, block.NumberU64()) != block.Hash()

	// Add the block to the canonical Chain number scheme and mark as the head
	if err := core.WriteCanonicalHash(bc.Db, block.Hash(), block.NumberU64()); err != nil {
		log.Crit("Failed to insert block number", "err", err)
	}
	if err := core.WriteHeadBlockHash(bc.Db, block.Hash()); err != nil {
		log.Crit("Failed to insert head block hash", "err", err)
	}
	bc.CurrentBlock.Store(block)

	// If the block is better than our head or is on a different Chain, force Update heads
	if updateHeads {
		bc.Hc.SetCurrentHeader(block.Header())

		if err := core.WriteHeadFastBlockHash(bc.Db, block.Hash()); err != nil {
			log.Crit("Failed to insert head fast block hash", "err", err)
		}
		bc.CurrentFastBlock.Store(block)
	}
}

// Genesis retrieves the Chain's genesis block.
func (bc *TomoBlockChain) Genesis() *types.Block {
	return bc.GenesisBlock
}

// GetBody retrieves a block body (transactions and uncles) from the database by
// hash, caching it if found.
func (bc *TomoBlockChain) GetBody(hash common.Hash) *types.Body {
	// Short circuit if the body's already in the cache, retrieve otherwise
	if cached, ok := bc.BodyCache.Get(hash); ok {
		body := cached.(*types.Body)
		return body
	}
	body := core.GetBody(bc.Db, hash, bc.Hc.GetBlockNumber(hash))
	if body == nil {
		return nil
	}
	// Cache the found body for next time and return
	bc.BodyCache.Add(hash, body)
	return body
}

// GetBodyRLP retrieves a block body in RLP encoding from the database by hash,
// caching it if found.
func (bc *TomoBlockChain) GetBodyRLP(hash common.Hash) rlp.RawValue {
	// Short circuit if the body's already in the cache, retrieve otherwise
	if cached, ok := bc.BodyRLPCache.Get(hash); ok {
		return cached.(rlp.RawValue)
	}
	body := core.GetBodyRLP(bc.Db, hash, bc.Hc.GetBlockNumber(hash))
	if len(body) == 0 {
		return nil
	}
	// Cache the found body for next time and return
	bc.BodyRLPCache.Add(hash, body)
	return body
}

// HasBlock checks if a block is fully present in the database or not.
func (bc *TomoBlockChain) HasBlock(hash common.Hash, number uint64) bool {
	if bc.BlockCache.Contains(hash) {
		return true
	}
	ok, _ := bc.Db.Has(core.BlockBodyKey(hash, number))
	return ok
}

// HasState checks if State trie is fully present in the database or not.
func (bc *TomoBlockChain) HasState(hash common.Hash) bool {
	_, err := bc.StateCache.OpenTrie(hash)
	return err == nil
}

// HasBlockAndState checks if a block and associated State trie is fully present
// in the database or not, caching it if present.
func (bc *TomoBlockChain) HasBlockAndState(hash common.Hash, number uint64) bool {
	// Check first that the block itself is known
	block := bc.GetBlock(hash, number)
	if block == nil {
		return false
	}
	return bc.HasState(block.Root())
}

// GetBlock retrieves a block from the database by hash and number,
// caching it if found.
func (bc *TomoBlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	// Short circuit if the block's already in the cache, retrieve otherwise
	if block, ok := bc.BlockCache.Get(hash); ok {
		return block.(*types.Block)
	}
	block := core.GetBlock(bc.Db, hash, number)
	if block == nil {
		return nil
	}
	// Cache the found block for next time and return
	bc.BlockCache.Add(block.Hash(), block)
	return block
}

// GetBlockByHash retrieves a block from the database by hash, caching it if found.
func (bc *TomoBlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	return bc.GetBlock(hash, bc.Hc.GetBlockNumber(hash))
}

// GetBlockByNumber retrieves a block from the database by number, caching it
// (associated with its hash) if found.
func (bc *TomoBlockChain) GetBlockByNumber(number uint64) *types.Block {
	hash := core.GetCanonicalHash(bc.Db, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return bc.GetBlock(hash, number)
}

// GetReceiptsByHash retrieves the receipts for All transactions in a given block.
func (bc *TomoBlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	return core.GetBlockReceipts(bc.Db, hash, core.GetBlockNumber(bc.Db, hash))
}

// GetBlocksFromHash returns the block corresponding to hash and up to n-1 ancestors.
// [deprecated by eth/62]
func (bc *TomoBlockChain) GetBlocksFromHash(hash common.Hash, n int) (blocks []*types.Block) {
	number := bc.Hc.GetBlockNumber(hash)
	for i := 0; i < n; i++ {
		block := bc.GetBlock(hash, number)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		hash = block.ParentHash()
		number--
	}
	return
}

// GetUnclesInChain retrieves All the uncles from a given block backwards until
// a specific distance is reached.
func (bc *TomoBlockChain) GetUnclesInChain(block *types.Block, length int) []*types.Header {
	uncles := []*types.Header{}
	for i := 0; block != nil && i < length; i++ {
		uncles = append(uncles, block.Uncles()...)
		block = bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
	}
	return uncles
}

// TrieNode retrieves a blob of Data associated with a trie node (or code hash)
// either from ephemeral in-memory cache, or from persistent storage.
func (bc *TomoBlockChain) TrieNode(hash common.Hash) ([]byte, error) {
	return bc.StateCache.TrieDB().Node(hash)
}

// Stop stops the blockchain service. If any imports are currently in progress
// it will abort them using the ProcInterrupt.
func (bc *TomoBlockChain) Stop() {
	if !atomic.CompareAndSwapInt32(&bc.Running, 0, 1) {
		return
	}
	// Unsubscribe All subscriptions registered from blockchain
	bc.Scope.Close()
	close(bc.Quit)
	atomic.StoreInt32(&bc.ProcInterrupt, 1)

	bc.Wg.Wait()

	// Ensure the State of a recent block is also stored to disk before exiting.
	// We're writing three different states to catch different restart scenarios:
	//  - HEAD:     So we don't need to reprocess any blocks in the general case
	//  - HEAD-1:   So we don't do large reorgs if our HEAD becomes an uncle
	//  - HEAD-127: So we have a hard limit on the number of blocks reexecuted
	if !bc.CacheConfig.Disabled {
		triedb := bc.StateCache.TrieDB()

		for _, offset := range []uint64{0, 1, core.TriesInMemory - 1} {
			if number := bc.GetCurrentBlock().NumberU64(); number > offset {
				recent := bc.GetBlockByNumber(number - offset)

				log.Info("Writing cached State to disk", "block", recent.Number(), "hash", recent.Hash(), "root", recent.Root())
				if err := triedb.Commit(recent.Root(), true); err != nil {
					log.Error("Failed to commit recent State trie", "err", err)
				}
			}
		}
		for !bc.Triegc.Empty() {
			triedb.Dereference(bc.Triegc.PopItem().(common.Hash), common.Hash{})
		}
		if size := triedb.Size(); size != 0 {
			log.Error("Dangling trie nodes after full cleanup")
		}
	}
	log.Info("Blockchain manager stopped")
}

func (bc *TomoBlockChain) procFutureBlocks() {
	blocks := make([]*types.Block, 0, bc.FutureBlocks.Len())
	for _, hash := range bc.FutureBlocks.Keys() {
		if block, exist := bc.FutureBlocks.Peek(hash); exist {
			blocks = append(blocks, block.(*types.Block))
		}
	}
	if len(blocks) > 0 {
		types.BlockBy(types.Number).Sort(blocks)

		// Insert one by one as Chain insertion needs contiguous ancestry between blocks
		for i := range blocks {
			bc.InsertChain(blocks[i : i+1])
		}
	}
}

// Rollback is designed to remove a Chain of links from the database that aren't
// certain enough to be valid.
func (bc *TomoBlockChain) Rollback(chain []common.Hash) {
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	for i := len(chain) - 1; i >= 0; i-- {
		hash := chain[i]

		currentHeader := bc.Hc.CurrentHeader()
		if currentHeader.Hash() == hash {
			bc.Hc.SetCurrentHeader(bc.GetHeader(currentHeader.ParentHash, currentHeader.Number.Uint64()-1))
		}
		if currentFastBlock := bc.GetCurrentFastBlock(); currentFastBlock.Hash() == hash {
			newFastBlock := bc.GetBlock(currentFastBlock.ParentHash(), currentFastBlock.NumberU64()-1)
			bc.CurrentFastBlock.Store(newFastBlock)
			core.WriteHeadFastBlockHash(bc.Db, newFastBlock.Hash())
		}
		if currentBlock := bc.GetCurrentBlock(); currentBlock.Hash() == hash {
			newBlock := bc.GetBlock(currentBlock.ParentHash(), currentBlock.NumberU64()-1)
			bc.CurrentBlock.Store(newBlock)
			core.WriteHeadBlockHash(bc.Db, newBlock.Hash())
		}
	}
}

// SetReceiptsData computes All the non-consensus fields of the receipts
func SetReceiptsData(config *params.ChainConfig, block *types.Block, receipts types.Receipts) error {
	signer := types.MakeSigner(config, block.Number())

	transactions, logIndex := block.Transactions(), uint(0)
	if len(transactions) != len(receipts) {
		return errors.New("transaction and receipt count mismatch")
	}

	for j := 0; j < len(receipts); j++ {
		// The transaction hash can be retrieved from the transaction itself
		receipts[j].TxHash = transactions[j].Hash()

		// The contract address can be derived from the transaction itself
		if transactions[j].To() == nil {
			// Deriving the Signer is expensive, only do if it's actually needed
			from, _ := types.Sender(signer, transactions[j])
			receipts[j].ContractAddress = crypto.CreateAddress(from, transactions[j].Nonce())
		}
		// The used Gas can be calculated based on previous receipts
		if j == 0 {
			receipts[j].GasUsed = receipts[j].CumulativeGasUsed
		} else {
			receipts[j].GasUsed = receipts[j].CumulativeGasUsed - receipts[j-1].CumulativeGasUsed
		}
		// The derived log fields can simply be set from the block and transaction
		for k := 0; k < len(receipts[j].Logs); k++ {
			receipts[j].Logs[k].BlockNumber = block.NumberU64()
			receipts[j].Logs[k].BlockHash = block.Hash()
			receipts[j].Logs[k].TxHash = receipts[j].TxHash
			receipts[j].Logs[k].TxIndex = uint(j)
			receipts[j].Logs[k].Index = logIndex
			logIndex++
		}
	}
	return nil
}

// InsertReceiptChain attempts to complete an already existing header Chain with
// transaction and receipt Data.
func (bc *TomoBlockChain) InsertReceiptChain(blockChain types.Blocks, receiptChain []types.Receipts) (int, error) {
	bc.Wg.Add(1)
	defer bc.Wg.Done()

	// Do a sanity check that the provided Chain is actually ordered and linked
	for i := 1; i < len(blockChain); i++ {
		if blockChain[i].NumberU64() != blockChain[i-1].NumberU64()+1 || blockChain[i].ParentHash() != blockChain[i-1].Hash() {
			log.Error("Non contiguous receipt insert", "number", blockChain[i].Number(), "hash", blockChain[i].Hash(), "parent", blockChain[i].ParentHash(),
				"prevnumber", blockChain[i-1].Number(), "prevhash", blockChain[i-1].Hash())
			return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, blockChain[i-1].NumberU64(),
				blockChain[i-1].Hash().Bytes()[:4], i, blockChain[i].NumberU64(), blockChain[i].Hash().Bytes()[:4], blockChain[i].ParentHash().Bytes()[:4])
		}
	}

	var (
		stats = struct{ processed, ignored int32 }{}
		start = time.Now()
		bytes = 0
		batch = bc.Db.NewBatch()
	)
	for i, block := range blockChain {
		receipts := receiptChain[i]
		// Short circuit insertion if shutting down or processing failed
		if atomic.LoadInt32(&bc.ProcInterrupt) == 1 {
			return 0, nil
		}
		// Short circuit if the owner header is unknown
		if !bc.HasHeader(block.Hash(), block.NumberU64()) {
			return i, fmt.Errorf("containing header #%d [%x…] unknown", block.Number(), block.Hash().Bytes()[:4])
		}
		// Skip if the entire Data is already known
		if bc.HasBlock(block.Hash(), block.NumberU64()) {
			stats.ignored++
			continue
		}
		// Compute All the non-consensus fields of the receipts
		if err := SetReceiptsData(bc.ChainConfig, block, receipts); err != nil {
			return i, fmt.Errorf("failed to set receipts Data: %v", err)
		}
		// Write All the Data out into the database
		if err := core.WriteBody(batch, block.Hash(), block.NumberU64(), block.Body()); err != nil {
			return i, fmt.Errorf("failed to write block body: %v", err)
		}
		if err := core.WriteBlockReceipts(batch, block.Hash(), block.NumberU64(), receipts); err != nil {
			return i, fmt.Errorf("failed to write block receipts: %v", err)
		}
		if err := core.WriteTxLookupEntries(batch, block); err != nil {
			return i, fmt.Errorf("failed to write lookup metadata: %v", err)
		}
		stats.processed++

		if batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return 0, err
			}
			bytes += batch.ValueSize()
			batch.Reset()
		}
	}
	if batch.ValueSize() > 0 {
		bytes += batch.ValueSize()
		if err := batch.Write(); err != nil {
			return 0, err
		}
	}

	// Update the head fast sync block if better
	bc.Mu.Lock()
	head := blockChain[len(blockChain)-1]
	if td := bc.GetTd(head.Hash(), head.NumberU64()); td != nil { // Rewind may have occurred, skip in that case
		currentFastBlock := bc.GetCurrentFastBlock()
		if bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64()).Cmp(td) < 0 {
			if err := core.WriteHeadFastBlockHash(bc.Db, head.Hash()); err != nil {
				log.Crit("Failed to Update head fast block hash", "err", err)
			}
			bc.CurrentFastBlock.Store(head)
		}
	}
	bc.Mu.Unlock()

	log.Info("Imported new block receipts",
		"count", stats.processed,
		"elapsed", common.PrettyDuration(time.Since(start)),
		"number", head.Number(),
		"hash", head.Hash(),
		"size", common.StorageSize(bytes),
		"Ignored", stats.ignored)
	return 0, nil
}

// WriteBlockWithoutState writes only the block and its metadata to the database,
// but does not write any State. This is used to construct competing side forks
// up to the point where they exceed the canonical total difficulty.
func (bc *TomoBlockChain) WriteBlockWithoutState(block *types.Block, td *big.Int) (err error) {
	bc.Wg.Add(1)
	defer bc.Wg.Done()

	if err := bc.Hc.WriteTd(block.Hash(), block.NumberU64(), td); err != nil {
		return err
	}
	if err := core.WriteBlock(bc.Db, block); err != nil {
		return err
	}
	return nil
}

// WriteBlockWithState writes the block and All associated State to the database.
func (bc *TomoBlockChain) WriteBlockWithState(block *types.Block, receipts []*types.Receipt, state *state.StateDB) (status core.WriteStatus, err error) {
	bc.Wg.Add(1)
	defer bc.Wg.Done()

	// Calculate the total difficulty of the block
	ptd := bc.GetTd(block.ParentHash(), block.NumberU64()-1)
	if ptd == nil {
		return core.NonStatTy, consensus.ErrUnknownAncestor
	}
	// Make sure no inconsistent State is leaked during insertion
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	currentBlock := bc.GetCurrentBlock()
	localTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
	externTd := new(big.Int).Add(block.Difficulty(), ptd)

	// Irrelevant of the canonical status, write the block itself to the database
	if err := bc.Hc.WriteTd(block.Hash(), block.NumberU64(), externTd); err != nil {
		return core.NonStatTy, err
	}
	// Write other block Data using a batch.
	batch := bc.Db.NewBatch()
	if err := core.WriteBlock(batch, block); err != nil {
		return core.NonStatTy, err
	}
	root, err := state.Commit(bc.ChainConfig.IsEIP158(block.Number()))
	if err != nil {
		return core.NonStatTy, err
	}
	triedb := bc.StateCache.TrieDB()

	// If we're Running an archive node, always flush
	if bc.CacheConfig.Disabled {
		if err := triedb.Commit(root, false); err != nil {
			return core.NonStatTy, err
		}
	} else {
		// Full but not archive node, do proper garbage collection
		triedb.Reference(root, common.Hash{}) // metadata reference to keep trie alive
		bc.Triegc.Push(root, -float32(block.NumberU64()))

		if current := block.NumberU64(); current > core.TriesInMemory {
			// Find the next State trie we need to commit
			header := bc.GetHeaderByNumber(current - core.TriesInMemory)
			chosen := header.Number.Uint64()

			// Only write to disk if we exceeded our memory allowance *and* also have at
			// least a given number of tries gapped.
			var (
				size  = triedb.Size()
				limit = common.StorageSize(bc.CacheConfig.TrieNodeLimit) * 1024 * 1024
			)
			if size > limit || bc.Gcproc > bc.CacheConfig.TrieTimeLimit {
				// If we're exceeding limits but haven't reached a large enough memory gap,
				// warn the user that the system is becoming unstable.
				if chosen < core.LastWrite+core.TriesInMemory {
					switch {
					case size >= 2*limit:
						log.Warn("State memory usage too high, committing", "size", size, "limit", limit, "optimum", float64(chosen-core.LastWrite)/core.TriesInMemory)
					case bc.Gcproc >= 2*bc.CacheConfig.TrieTimeLimit:
						log.Info("State in memory for too long, committing", "time", bc.Gcproc, "allowance", bc.CacheConfig.TrieTimeLimit, "optimum", float64(chosen-core.LastWrite)/core.TriesInMemory)
					}
				}
				// If optimum or critical limits reached, write to disk
				if chosen >= core.LastWrite+core.TriesInMemory || size >= 2*limit || bc.Gcproc >= 2*bc.CacheConfig.TrieTimeLimit {
					triedb.Commit(header.Root, true)
					core.LastWrite = chosen
					bc.Gcproc = 0
				}
			}
			// Garbage collect anything below our required write retention
			for !bc.Triegc.Empty() {
				root, number := bc.Triegc.Pop()
				if uint64(-number) > chosen {
					bc.Triegc.Push(root, number)
					break
				}
				triedb.Dereference(root.(common.Hash), common.Hash{})
			}
		}
	}
	if err := core.WriteBlockReceipts(batch, block.Hash(), block.NumberU64(), receipts); err != nil {
		return core.NonStatTy, err
	}
	// If the total difficulty is higher than our known, add it to the canonical Chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	reorg := externTd.Cmp(localTd) > 0
	currentBlock = bc.GetCurrentBlock()
	if !reorg && externTd.Cmp(localTd) == 0 {
		// Split same-difficulty blocks by number, then at random
		reorg = block.NumberU64() < currentBlock.NumberU64() || (block.NumberU64() == currentBlock.NumberU64() && mrand.Float64() < 0.5)
	}
	if reorg {
		// Reorganise the Chain if the parent is not the head block
		if block.ParentHash() != currentBlock.Hash() {
			if err := bc.reorg(currentBlock, block); err != nil {
				return core.NonStatTy, err
			}
		}
		// Write the positional metadata for transaction and receipt lookups
		if err := core.WriteTxLookupEntries(batch, block); err != nil {
			return core.NonStatTy, err
		}
		// Write hash preimages
		if err := core.WritePreimages(bc.Db, block.NumberU64(), state.Preimages()); err != nil {
			return core.NonStatTy, err
		}
		status = core.CanonStatTy
	} else {
		status = core.SideStatTy
	}
	if err := batch.Write(); err != nil {
		return core.NonStatTy, err
	}

	// Set new head.
	if status == core.CanonStatTy {
		bc.insert(block)
	}
	bc.FutureBlocks.Remove(block.Hash())
	return status, nil
}

// InsertChain attempts to insert the given batch of blocks in to the canonical
// Chain or, otherwise, create a fork. If an error is returned it will return
// the index number of the failing block as well an error describing what went
// wrong.
//
// After insertion is done, All accumulated events will be fired.
func (bc *TomoBlockChain) InsertChain(chain types.Blocks) (int, error) {
	n, events, logs, err := bc.insertChain(chain)
	bc.PostChainEvents(events, logs)
	return n, err
}


// reorgs takes two blocks, an old Chain and a new Chain and will reconstruct the blocks and inserts them
// to be part of the new canonical Chain and accumulates potential missing transactions and post an
// event about them
func (bc *TomoBlockChain) reorg(oldBlock, newBlock *types.Block) error {
	var (
		newChain    types.Blocks
		oldChain    types.Blocks
		commonBlock *types.Block
		deletedTxs  types.Transactions
		deletedLogs []*types.Log
		// collectLogs collects the logs that were generated during the
		// processing of the block that corresponds with the given hash.
		// These logs are later announced as deleted.
		collectLogs = func(h common.Hash) {
			// Coalesce logs and set 'Removed'.
			receipts := core.GetBlockReceipts(bc.Db, h, bc.Hc.GetBlockNumber(h))
			for _, receipt := range receipts {
				for _, log := range receipt.Logs {
					del := *log
					del.Removed = true
					deletedLogs = append(deletedLogs, &del)
				}
			}
		}
	)

	// first reduce whoever is higher bound
	if oldBlock.NumberU64() > newBlock.NumberU64() {
		// reduce old Chain
		for ; oldBlock != nil && oldBlock.NumberU64() != newBlock.NumberU64(); oldBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1) {
			oldChain = append(oldChain, oldBlock)
			deletedTxs = append(deletedTxs, oldBlock.Transactions()...)

			collectLogs(oldBlock.Hash())
		}
	} else {
		// reduce new Chain and append new Chain blocks for inserting later on
		for ; newBlock != nil && newBlock.NumberU64() != oldBlock.NumberU64(); newBlock = bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1) {
			newChain = append(newChain, newBlock)
		}
	}
	if oldBlock == nil {
		return fmt.Errorf("Invalid old Chain")
	}
	if newBlock == nil {
		return fmt.Errorf("Invalid new Chain")
	}

	for {
		if oldBlock.Hash() == newBlock.Hash() {
			commonBlock = oldBlock
			break
		}

		oldChain = append(oldChain, oldBlock)
		newChain = append(newChain, newBlock)
		deletedTxs = append(deletedTxs, oldBlock.Transactions()...)
		collectLogs(oldBlock.Hash())

		oldBlock, newBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1), bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1)
		if oldBlock == nil {
			return fmt.Errorf("Invalid old Chain")
		}
		if newBlock == nil {
			return fmt.Errorf("Invalid new Chain")
		}
	}
	// Ensure the user sees large reorgs
	if len(oldChain) > 0 && len(newChain) > 0 {
		logFn := log.Debug
		if len(oldChain) > 63 {
			logFn = log.Warn
		}
		logFn("Chain split detected", "number", commonBlock.Number(), "hash", commonBlock.Hash(),
			"drop", len(oldChain), "dropfrom", oldChain[0].Hash(), "add", len(newChain), "addfrom", newChain[0].Hash())
	} else {
		log.Error("Impossible reorg, please file an issue", "oldnum", oldBlock.Number(), "oldhash", oldBlock.Hash(), "newnum", newBlock.Number(), "newhash", newBlock.Hash())
	}
	// Insert the new Chain, taking care of the proper incremental order
	var addedTxs types.Transactions
	for i := len(newChain) - 1; i >= 0; i-- {
		// insert the block in the canonical way, re-writing history
		bc.insert(newChain[i])
		// write lookup entries for hash based transaction/receipt searches
		if err := core.WriteTxLookupEntries(bc.Db, newChain[i]); err != nil {
			return err
		}
		addedTxs = append(addedTxs, newChain[i].Transactions()...)
	}
	// calculate the difference between deleted and added transactions
	diff := types.TxDifference(deletedTxs, addedTxs)
	// When transactions get deleted from the database that means the
	// receipts that were created in the fork must also be deleted
	for _, tx := range diff {
		core.DeleteTxLookupEntry(bc.Db, tx.Hash())
	}
	if len(deletedLogs) > 0 {
		go bc.RmLogsFeed.Send(core.RemovedLogsEvent{deletedLogs})
	}
	if len(oldChain) > 0 {
		go func() {
			for _, block := range oldChain {
				bc.ChainSideFeed.Send(core.ChainSideEvent{Block: block})
			}
		}()
	}

	return nil
}

// PostChainEvents iterates over the events generated by a Chain insertion and
// posts them into the event feed.
// TODO: Should not expose PostChainEvents. The Chain events should be posted in WriteBlock.
func (bc *TomoBlockChain) PostChainEvents(events []interface{}, logs []*types.Log) {
	// post event logs for further processing
	if logs != nil {
		bc.LogsFeed.Send(logs)
	}
	for _, event := range events {
		switch ev := event.(type) {
		case core.ChainEvent:
			bc.ChainFeed.Send(ev)

		case core.ChainHeadEvent:
			bc.ChainHeadFeed.Send(ev)

		case core.ChainSideEvent:
			bc.ChainSideFeed.Send(ev)
		}
	}
}

func (bc *TomoBlockChain) Update() {
	futureTimer := time.NewTicker(5 * time.Second)
	defer futureTimer.Stop()
	for {
		select {
		case <-futureTimer.C:
			bc.procFutureBlocks()
		case <-bc.Quit:
			return
		}
	}
}

// GetBadBlocks returns a list of the last 'bad blocks' that the client has seen on the network
func (bc *TomoBlockChain) GetBadBlocks() ([]core.BadBlockArgs, error) {
	headers := make([]core.BadBlockArgs, 0, bc.BadBlocks.Len())
	for _, hash := range bc.BadBlocks.Keys() {
		if hdr, exist := bc.BadBlocks.Peek(hash); exist {
			header := hdr.(*types.Header)
			headers = append(headers, core.BadBlockArgs{header.Hash(), header})
		}
	}
	return headers, nil
}

// addBadBlock adds a bad block to the bad-block LRU cache
func (bc *TomoBlockChain) addBadBlock(block *types.Block) {
	bc.BadBlocks.Add(block.Header().Hash(), block.Header())
}

// ReportBlock logs a bad block error.
func (bc *TomoBlockChain) ReportBlock(block *types.Block, receipts types.Receipts, err error) {
	bc.addBadBlock(block)

	var receiptString string
	for _, receipt := range receipts {
		receiptString += fmt.Sprintf("\t%v\n", receipt)
	}
	log.Error(fmt.Sprintf(`
########## BAD BLOCK #########
Chain Config: %v

Number: %v
Hash: 0x%x
%v

Error: %v
##############################
`, bc.ChainConfig, block.Number(), block.Hash(), receiptString, err))
}

// InsertHeaderChain attempts to insert the given header Chain in to the local
// Chain, possibly creating a reorg. If an error is returned, it will return the
// index number of the failing header as well an error describing what went wrong.
//
// The verify parameter can be used to fine tune whether nonce verification
// should be done or not. The reason behind the optional check is because some
// of the header retrieval mechanisms already need to verify nonces, as well as
// because nonces can be verified sparsely, not needing to check each.
func (bc *TomoBlockChain) InsertHeaderChain(chain []*types.Header, checkFreq int) (int, error) {
	start := time.Now()
	if i, err := bc.Hc.ValidateHeaderChain(chain, checkFreq); err != nil {
		return i, err
	}

	// Make sure only one thread manipulates the Chain at once
	bc.Chainmu.Lock()
	defer bc.Chainmu.Unlock()

	bc.Wg.Add(1)
	defer bc.Wg.Done()

	whFunc := func(header *types.Header) error {
		bc.Mu.Lock()
		defer bc.Mu.Unlock()

		_, err := bc.Hc.WriteHeader(header)
		return err
	}

	return bc.Hc.InsertHeaderChain(chain, whFunc, start)
}

// writeHeader writes a header into the local Chain, given that its parent is
// already known. If the total difficulty of the newly inserted header becomes
// greater than the current known TD, the canonical Chain is re-routed.
//
// Note: This method is not concurrent-safe with inserting blocks simultaneously
// into the Chain, as side effects caused by reorganisations cannot be emulated
// without the real blocks. Hence, writing headers directly should only be done
// in two scenarios: pure-header mode of operation (light clients), or properly
// separated header/block phases (non-archive clients).
func (bc *TomoBlockChain) writeHeader(header *types.Header) error {
	bc.Wg.Add(1)
	defer bc.Wg.Done()

	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	_, err := bc.Hc.WriteHeader(header)
	return err
}

// CurrentHeader retrieves the current head header of the canonical Chain. The
// header is retrieved from the HeaderChain's internal cache.
func (bc *TomoBlockChain) CurrentHeader() *types.Header {
	return bc.Hc.CurrentHeader()
}

// GetTd retrieves a block's total difficulty in the canonical Chain from the
// database by hash and number, caching it if found.
func (bc *TomoBlockChain) GetTd(hash common.Hash, number uint64) *big.Int {
	return bc.Hc.GetTd(hash, number)
}

// GetTdByHash retrieves a block's total difficulty in the canonical Chain from the
// database by hash, caching it if found.
func (bc *TomoBlockChain) GetTdByHash(hash common.Hash) *big.Int {
	return bc.Hc.GetTdByHash(hash)
}

// GetHeader retrieves a block header from the database by hash and number,
// caching it if found.
func (bc *TomoBlockChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	return bc.Hc.GetHeader(hash, number)
}

// GetHeaderByHash retrieves a block header from the database by hash, caching it if
// found.
func (bc *TomoBlockChain) GetHeaderByHash(hash common.Hash) *types.Header {
	return bc.Hc.GetHeaderByHash(hash)
}

// HasHeader checks if a block header is present in the database or not, caching
// it if present.
func (bc *TomoBlockChain) HasHeader(hash common.Hash, number uint64) bool {
	return bc.Hc.HasHeader(hash, number)
}

// GetBlockHashesFromHash retrieves a number of block hashes starting at a given
// hash, fetching towards the genesis block.
func (bc *TomoBlockChain) GetBlockHashesFromHash(hash common.Hash, max uint64) []common.Hash {
	return bc.Hc.GetBlockHashesFromHash(hash, max)
}

// GetHeaderByNumber retrieves a block header from the database by number,
// caching it (associated with its hash) if found.
func (bc *TomoBlockChain) GetHeaderByNumber(number uint64) *types.Header {
	return bc.Hc.GetHeaderByNumber(number)
}

// Config retrieves the blockchain's Chain configuration.
func (bc *TomoBlockChain) Config() *params.ChainConfig { return bc.ChainConfig }

// GetEngine retrieves the blockchain's consensus GetEngine.
func (bc *TomoBlockChain) GetEngine() consensus.Engine { return bc.Engine }

// SubscribeRemovedLogsEvent registers a subscription of RemovedLogsEvent.
func (bc *TomoBlockChain) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
	return bc.Scope.Track(bc.RmLogsFeed.Subscribe(ch))
}

// SubscribeChainEvent registers a subscription of ChainEvent.
func (bc *TomoBlockChain) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	return bc.Scope.Track(bc.ChainFeed.Subscribe(ch))
}

// SubscribeChainHeadEvent registers a subscription of ChainHeadEvent.
func (bc *TomoBlockChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return bc.Scope.Track(bc.ChainHeadFeed.Subscribe(ch))
}

// SubscribeChainSideEvent registers a subscription of ChainSideEvent.
func (bc *TomoBlockChain) SubscribeChainSideEvent(ch chan<- core.ChainSideEvent) event.Subscription {
	return bc.Scope.Track(bc.ChainSideFeed.Subscribe(ch))
}

// SubscribeLogsEvent registers a subscription of []*types.Log.
func (bc *TomoBlockChain) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return bc.Scope.Track(bc.LogsFeed.Subscribe(ch))
}

