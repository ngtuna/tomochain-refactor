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
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/karalabe/cookiejar.v2/collections/prque"
)

const (
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10
	// rmTxChanSize is the size of channel listening to RemovedTransactionEvent.
	rmTxChanSize = 10
)

var (
	// ErrInvalidSender is returned if the transaction Contains an invalid signature.
	ErrInvalidSender = errors.New("invalid sender")

	// ErrNonceTooLow is returned if the nonce of a transaction is lower than the
	// one present in the local Chain.
	ErrNonceTooLow = errors.New("nonce too low")

	// ErrUnderpriced is returned if a transaction's Gas price is below the minimum
	// configured for the transaction pool.
	ErrUnderpriced = errors.New("transaction underpriced")

	// ErrReplaceUnderpriced is returned if a transaction is attempted to be replaced
	// with a different one without the required price bump.
	ErrReplaceUnderpriced = errors.New("replacement transaction underpriced")

	// ErrInsufficientFunds is returned if the total cost of executing a transaction
	// is higher than the balance of the user's account.
	ErrInsufficientFunds = errors.New("insufficient funds for Gas * price + GetValue")

	// ErrIntrinsicGas is returned if the transaction is specified to use less Gas
	// than required to start the invocation.
	ErrIntrinsicGas = errors.New("intrinsic Gas too low")

	// ErrGasLimit is returned if a transaction's requested Gas limit exceeds the
	// maximum allowance of the current block.
	ErrGasLimit = errors.New("exceeds block Gas limit")

	// ErrNegativeValue is a sanity error to ensure noone is able to specify a
	// transaction with a negative GetValue.
	ErrNegativeValue = errors.New("negative GetValue")

	// ErrOversizedData is returned if the input Data of a transaction is greater
	// than some meaningful limit a user might use. This is not a consensus error
	// making the transaction invalid, rather a DOS protection.
	ErrOversizedData = errors.New("oversized Data")
)

var (
	evictionInterval    = time.Minute     // Time interval to check for evictable transactions
	statsReportInterval = 8 * time.Second // Time interval to Report transaction pool stats
)

var (
	// Metrics for the Pending pool
	pendingDiscardCounter   = metrics.NewRegisteredCounter("txpool/Pending/discard", nil)
	pendingReplaceCounter   = metrics.NewRegisteredCounter("txpool/Pending/replace", nil)
	pendingRateLimitCounter = metrics.NewRegisteredCounter("txpool/Pending/ratelimit", nil) // Dropped due to rate limiting
	pendingNofundsCounter   = metrics.NewRegisteredCounter("txpool/Pending/nofunds", nil)   // Dropped due to out-of-funds

	// Metrics for the Queued pool
	queuedDiscardCounter   = metrics.NewRegisteredCounter("txpool/Queued/discard", nil)
	queuedReplaceCounter   = metrics.NewRegisteredCounter("txpool/Queued/replace", nil)
	queuedRateLimitCounter = metrics.NewRegisteredCounter("txpool/Queued/ratelimit", nil) // Dropped due to rate limiting
	queuedNofundsCounter   = metrics.NewRegisteredCounter("txpool/Queued/nofunds", nil)   // Dropped due to out-of-funds

	// General tx metrics
	invalidTxCounter     = metrics.NewRegisteredCounter("txpool/invalid", nil)
	underpricedTxCounter = metrics.NewRegisteredCounter("txpool/underpriced", nil)
)

// TxStatus is the current status of a transaction as seen by the pool.
type TxStatus uint

const (
	TxStatusUnknown TxStatus = iota
	TxStatusQueued
	TxStatusPending
	TxStatusIncluded
)

// blockChain provides the State of blockchain and current Gas limit to do
// some pre checks in tx pool and event subscribers.
type blockChain interface {
	GetCurrentBlock() *types.Block
	GetBlock(hash common.Hash, number uint64) *types.Block
	StateAt(root common.Hash) (*state.StateDB, error)

	SubscribeChainHeadEvent(ch chan<- ChainHeadEvent) event.Subscription
}

// TxPoolConfig are the configuration parameters of the transaction pool.
type TxPoolConfig struct {
	NoLocals  bool          // Whether local transaction handling should be disabled
	Journal   string        // Journal of local transactions to survive node restarts
	Rejournal time.Duration // Time interval to regenerate the local transaction Journal

	PriceLimit uint64 // Minimum Gas price to enforce for acceptance into the pool
	PriceBump  uint64 // Minimum price bump percentage to replace an already existing transaction (nonce)

	AccountSlots uint64 // Minimum number of executable transaction slots guaranteed per account
	GlobalSlots  uint64 // Maximum number of executable transaction slots for All accounts
	AccountQueue uint64 // Maximum number of non-executable transaction slots permitted per account
	GlobalQueue  uint64 // Maximum number of non-executable transaction slots for All accounts

	Lifetime time.Duration // Maximum amount of time non-executable transaction are Queued
}

// DefaultTxPoolConfig Contains the default configurations for the transaction
// pool.
var DefaultTxPoolConfig = TxPoolConfig{
	Journal:   "transactions.rlp",
	Rejournal: time.Hour,

	PriceLimit: 1,
	PriceBump:  10,

	AccountSlots: 16,
	GlobalSlots:  4096,
	AccountQueue: 64,
	GlobalQueue:  1024,

	Lifetime: 3 * time.Hour,
}

// sanitize checks the provided user configurations and changes anything that's
// unreasonable or unworkable.
func (config *TxPoolConfig) sanitize() TxPoolConfig {
	conf := *config
	if conf.Rejournal < time.Second {
		log.Warn("Sanitizing invalid txpool Journal time", "provided", conf.Rejournal, "updated", time.Second)
		conf.Rejournal = time.Second
	}
	if conf.PriceLimit < 1 {
		log.Warn("Sanitizing invalid txpool price limit", "provided", conf.PriceLimit, "updated", DefaultTxPoolConfig.PriceLimit)
		conf.PriceLimit = DefaultTxPoolConfig.PriceLimit
	}
	if conf.PriceBump < 1 {
		log.Warn("Sanitizing invalid txpool price bump", "provided", conf.PriceBump, "updated", DefaultTxPoolConfig.PriceBump)
		conf.PriceBump = DefaultTxPoolConfig.PriceBump
	}
	return conf
}

// GetTxPool Contains All currently known transactions. Transactions
// enter the pool when they are received from the network or submitted
// locally. They exit the pool when they are included in the blockchain.
//
// The pool separates processable transactions (which can be applied to the
// current State) and future transactions. Transactions move between those
// two states over time as they are received and Processed.
type TxPool struct {
	Config       TxPoolConfig
	Chainconfig  *params.ChainConfig
	Chain        blockChain
	GasPrice     *big.Int
	TxFeed       event.Feed
	Scope        event.SubscriptionScope
	ChainHeadCh  chan ChainHeadEvent
	ChainHeadSub event.Subscription
	Signer       types.Signer
	Mu           sync.RWMutex

	CurrentState  *state.StateDB      // Current State in the blockchain head
	PendingState  *state.ManagedState // GetPending State tracking virtual nonces
	CurrentMaxGas uint64              // Current Gas limit for transaction caps

	Locals  *accountSet // Set of local transaction to exempt from eviction rules
	Journal *txJournal  // Journal of local transaction to back up to disk

	Pending map[common.Address]*txList         // All currently processable transactions
	Queue   map[common.Address]*txList         // Queued but non-processable transactions
	Beats   map[common.Address]time.Time       // Last heartbeat from each known account
	All     map[common.Hash]*types.Transaction // All transactions to allow lookups
	Priced  *txPricedList                      // All transactions sorted by price

	Wg sync.WaitGroup // for shutdown sync

	Homestead bool
}

// NewTxPool creates a new transaction pool to gather, sort and filter inbound
// transactions from the network.
func NewTxPool(config TxPoolConfig, chainconfig *params.ChainConfig, chain blockChain) *TxPool {
	// Sanitize the input to ensure no vulnerable Gas prices are set
	config = (&config).sanitize()

	// Create the transaction pool with its initial settings
	pool := &TxPool{
		Config:      config,
		Chainconfig: chainconfig,
		Chain:       chain,
		Signer:      types.NewEIP155Signer(chainconfig.ChainId),
		Pending:     make(map[common.Address]*txList),
		Queue:       make(map[common.Address]*txList),
		Beats:       make(map[common.Address]time.Time),
		All:         make(map[common.Hash]*types.Transaction),
		ChainHeadCh: make(chan ChainHeadEvent, chainHeadChanSize),
		GasPrice:    new(big.Int).SetUint64(config.PriceLimit),
	}
	pool.Locals = newAccountSet(pool.Signer)
	pool.Priced = newTxPricedList(&pool.All)
	pool.reset(nil, chain.GetCurrentBlock().Header())

	// If local transactions and journaling is enabled, load from disk
	if !config.NoLocals && config.Journal != "" {
		pool.Journal = newTxJournal(config.Journal)

		if err := pool.Journal.load(pool.AddLocal); err != nil {
			log.Warn("Failed to load transaction Journal", "err", err)
		}
		if err := pool.Journal.rotate(pool.local()); err != nil {
			log.Warn("Failed to rotate transaction Journal", "err", err)
		}
	}
	// Subscribe events from blockchain
	pool.ChainHeadSub = pool.Chain.SubscribeChainHeadEvent(pool.ChainHeadCh)

	// Start the event loop and return
	pool.Wg.Add(1)
	go pool.loop()

	return pool
}

// loop is the transaction pool's main event loop, waiting for and reacting to
// outside blockchain events as well as for various reporting and transaction
// eviction events.
func (pool *TxPool) loop() {
	defer pool.Wg.Done()

	// Start the stats reporting and transaction eviction tickers
	var prevPending, prevQueued, prevStales int

	report := time.NewTicker(statsReportInterval)
	defer report.Stop()

	evict := time.NewTicker(evictionInterval)
	defer evict.Stop()

	journal := time.NewTicker(pool.Config.Rejournal)
	defer journal.Stop()

	// Track the previous head headers for transaction reorgs
	head := pool.Chain.GetCurrentBlock()

	// Keep waiting for and reacting to the various events
	for {
		select {
		// Handle ChainHeadEvent
		case ev := <-pool.ChainHeadCh:
			if ev.Block != nil {
				pool.Mu.Lock()
				if pool.Chainconfig.IsHomestead(ev.Block.Number()) {
					pool.Homestead = true
				}
				pool.reset(head.Header(), ev.Block.Header())
				head = ev.Block

				pool.Mu.Unlock()
			}
		// Be unsubscribed due to system stopped
		case <-pool.ChainHeadSub.Err():
			return

		// Handle stats reporting ticks
		case <-report.C:
			pool.Mu.RLock()
			pending, queued := pool.stats()
			stales := pool.Priced.stales
			pool.Mu.RUnlock()

			if pending != prevPending || queued != prevQueued || stales != prevStales {
				log.Debug("Transaction pool status Report", "executable", pending, "Queued", queued, "stales", stales)
				prevPending, prevQueued, prevStales = pending, queued, stales
			}

		// Handle inactive account transaction eviction
		case <-evict.C:
			pool.Mu.Lock()
			for addr := range pool.Queue {
				// Skip local transactions from the eviction mechanism
				if pool.Locals.Contains(addr) {
					continue
				}
				// Any non-Locals old enough should be removed
				if time.Since(pool.Beats[addr]) > pool.Config.Lifetime {
					for _, tx := range pool.Queue[addr].Flatten() {
						pool.removeTx(tx.Hash())
					}
				}
			}
			pool.Mu.Unlock()

		// Handle local transaction Journal rotation
		case <-journal.C:
			if pool.Journal != nil {
				pool.Mu.Lock()
				if err := pool.Journal.rotate(pool.local()); err != nil {
					log.Warn("Failed to rotate local tx Journal", "err", err)
				}
				pool.Mu.Unlock()
			}
		}
	}
}

// lockedReset is a wrapper around reset to allow calling it in a thread safe
// manner. This method is only ever used in the tester!
func (pool *TxPool) lockedReset(oldHead, newHead *types.Header) {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	pool.reset(oldHead, newHead)
}

// reset retrieves the current State of the blockchain and ensures the content
// of the transaction pool is valid with regard to the Chain State.
func (pool *TxPool) reset(oldHead, newHead *types.Header) {
	// If we're reorging an old State, reinject All dropped transactions
	var reinject types.Transactions

	if oldHead != nil && oldHead.Hash() != newHead.ParentHash {
		// If the reorg is too deep, avoid doing it (will happen during fast sync)
		oldNum := oldHead.Number.Uint64()
		newNum := newHead.Number.Uint64()

		if depth := uint64(math.Abs(float64(oldNum) - float64(newNum))); depth > 64 {
			log.Debug("Skipping deep transaction reorg", "depth", depth)
		} else {
			// Reorg seems shallow enough to pull in All transactions into memory
			var discarded, included types.Transactions

			var (
				rem = pool.Chain.GetBlock(oldHead.Hash(), oldHead.Number.Uint64())
				add = pool.Chain.GetBlock(newHead.Hash(), newHead.Number.Uint64())
			)
			for rem.NumberU64() > add.NumberU64() {
				discarded = append(discarded, rem.Transactions()...)
				if rem = pool.Chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
					log.Error("Unrooted old Chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
					return
				}
			}
			for add.NumberU64() > rem.NumberU64() {
				included = append(included, add.Transactions()...)
				if add = pool.Chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
					log.Error("Unrooted new Chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
					return
				}
			}
			for rem.Hash() != add.Hash() {
				discarded = append(discarded, rem.Transactions()...)
				if rem = pool.Chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
					log.Error("Unrooted old Chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
					return
				}
				included = append(included, add.Transactions()...)
				if add = pool.Chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
					log.Error("Unrooted new Chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
					return
				}
			}
			reinject = types.TxDifference(discarded, included)
		}
	}
	// Initialize the internal State to the current head
	if newHead == nil {
		newHead = pool.Chain.GetCurrentBlock().Header() // Special case during testing
	}
	statedb, err := pool.Chain.StateAt(newHead.Root)
	if err != nil {
		log.Error("Failed to reset txpool State", "err", err)
		return
	}
	pool.CurrentState = statedb
	pool.PendingState = state.ManageState(statedb)
	pool.CurrentMaxGas = newHead.GasLimit

	// Inject any transactions discarded due to reorgs
	log.Debug("Reinjecting stale transactions", "count", len(reinject))
	pool.addTxsLocked(reinject, false)

	// validate the pool of Pending transactions, this will remove
	// any transactions that have been included in the block or
	// have been invalidated because of another transaction (e.g.
	// higher Gas price)
	pool.demoteUnexecutables()

	// Update All accounts to the latest known Pending nonce
	for addr, list := range pool.Pending {
		txs := list.Flatten() // Heavy but will be cached and is needed by the miner anyway
		pool.PendingState.SetNonce(addr, txs[len(txs)-1].Nonce()+1)
	}
	// Check the Queue and move transactions over to the Pending if possible
	// or remove those that have become invalid
	pool.promoteExecutables(nil)
}

// Stop terminates the transaction pool.
func (pool *TxPool) Stop() {
	// Unsubscribe All subscriptions registered from txpool
	pool.Scope.Close()

	// Unsubscribe subscriptions registered from blockchain
	pool.ChainHeadSub.Unsubscribe()
	pool.Wg.Wait()

	if pool.Journal != nil {
		pool.Journal.close()
	}
	log.Info("Transaction pool stopped")
}

// SubscribeTxPreEvent registers a subscription of TxPreEvent and
// starts sending event to the given channel.
func (pool *TxPool) SubscribeTxPreEvent(ch chan<- TxPreEvent) event.Subscription {
	return pool.Scope.Track(pool.TxFeed.Subscribe(ch))
}

// GetGasPrice returns the current Gas price enforced by the transaction pool.
func (pool *TxPool) GetGasPrice() *big.Int {
	pool.Mu.RLock()
	defer pool.Mu.RUnlock()

	return new(big.Int).Set(pool.GasPrice)
}

// SetGasPrice updates the minimum price required by the transaction pool for a
// new transaction, and drops All transactions below this threshold.
func (pool *TxPool) SetGasPrice(price *big.Int) {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	pool.GasPrice = price
	for _, tx := range pool.Priced.Cap(price, pool.Locals) {
		pool.removeTx(tx.Hash())
	}
	log.Info("Transaction pool price threshold updated", "price", price)
}

// State returns the virtual managed State of the transaction pool.
func (pool *TxPool) State() *state.ManagedState {
	pool.Mu.RLock()
	defer pool.Mu.RUnlock()

	return pool.PendingState
}

// Stats retrieves the current pool stats, namely the number of Pending and the
// number of Queued (non-executable) transactions.
func (pool *TxPool) Stats() (int, int) {
	pool.Mu.RLock()
	defer pool.Mu.RUnlock()

	return pool.stats()
}

// stats retrieves the current pool stats, namely the number of Pending and the
// number of Queued (non-executable) transactions.
func (pool *TxPool) stats() (int, int) {
	pending := 0
	for _, list := range pool.Pending {
		pending += list.Len()
	}
	queued := 0
	for _, list := range pool.Queue {
		queued += list.Len()
	}
	return pending, queued
}

// Content retrieves the Data content of the transaction pool, returning All the
// Pending as well as Queued transactions, grouped by account and sorted by nonce.
func (pool *TxPool) Content() (map[common.Address]types.Transactions, map[common.Address]types.Transactions) {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	pending := make(map[common.Address]types.Transactions)
	for addr, list := range pool.Pending {
		pending[addr] = list.Flatten()
	}
	queued := make(map[common.Address]types.Transactions)
	for addr, list := range pool.Queue {
		queued[addr] = list.Flatten()
	}
	return pending, queued
}

// GetPending retrieves All currently processable transactions, groupped by origin
// account and sorted by nonce. The returned transaction set is a copy and can be
// freely modified by calling code.
func (pool *TxPool) GetPending() (map[common.Address]types.Transactions, error) {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	pending := make(map[common.Address]types.Transactions)
	for addr, list := range pool.Pending {
		pending[addr] = list.Flatten()
	}
	return pending, nil
}

// local retrieves All currently known local transactions, groupped by origin
// account and sorted by nonce. The returned transaction set is a copy and can be
// freely modified by calling code.
func (pool *TxPool) local() map[common.Address]types.Transactions {
	txs := make(map[common.Address]types.Transactions)
	for addr := range pool.Locals.accounts {
		if pending := pool.Pending[addr]; pending != nil {
			txs[addr] = append(txs[addr], pending.Flatten()...)
		}
		if queued := pool.Queue[addr]; queued != nil {
			txs[addr] = append(txs[addr], queued.Flatten()...)
		}
	}
	return txs
}

// validateTx checks whether a transaction is valid according to the consensus
// rules and adheres to some heuristic limits of the local node (price and size).
func (pool *TxPool) validateTx(tx *types.Transaction, local bool) error {
	// Heuristic limit, reject transactions over 32KB to prevent DOS attacks
	if tx.Size() > 32*1024 {
		return ErrOversizedData
	}
	// Transactions can't be negative. This may never happen using RLP decoded
	// transactions but may occur if you create a transaction using the RPC.
	if tx.Value().Sign() < 0 {
		return ErrNegativeValue
	}
	// Ensure the transaction doesn't exceed the current block limit Gas.
	if pool.CurrentMaxGas < tx.Gas() {
		return ErrGasLimit
	}
	// Make sure the transaction is signed properly
	from, err := types.Sender(pool.Signer, tx)
	if err != nil {
		return ErrInvalidSender
	}
	// Drop non-local transactions under our own minimal accepted Gas price
	local = local || pool.Locals.Contains(from) // account may be local even if the transaction arrived from the network
	if !local && pool.GasPrice.Cmp(tx.GasPrice()) > 0 {
		return ErrUnderpriced
	}
	// Ensure the transaction adheres to nonce ordering
	if pool.CurrentState.GetNonce(from) > tx.Nonce() {
		return ErrNonceTooLow
	}
	// Transactor should have enough funds to cover the costs
	// cost == V + GP * GL
	if pool.CurrentState.GetBalance(from).Cmp(tx.Cost()) < 0 {
		return ErrInsufficientFunds
	}
	intrGas, err := IntrinsicGas(tx.Data(), tx.To() == nil, pool.Homestead)
	if err != nil {
		return err
	}
	if tx.Gas() < intrGas {
		return ErrIntrinsicGas
	}
	return nil
}

// add validates a transaction and inserts it into the non-executable Queue for
// later Pending promotion and execution. If the transaction is a replacement for
// an already Pending or Queued one, it overwrites the previous and returns this
// so outer code doesn't uselessly call promote.
//
// If a newly added transaction is marked as local, its sending account will be
// whitelisted, preventing any associated transaction from being dropped out of
// the pool due to pricing constraints.
func (pool *TxPool) add(tx *types.Transaction, local bool) (bool, error) {
	// If the transaction is already known, discard it
	hash := tx.Hash()
	if pool.All[hash] != nil {
		log.Trace("Discarding already known transaction", "hash", hash)
		return false, fmt.Errorf("known transaction: %x", hash)
	}
	// If the transaction fails basic validation, discard it
	if err := pool.validateTx(tx, local); err != nil {
		log.Trace("Discarding invalid transaction", "hash", hash, "err", err)
		invalidTxCounter.Inc(1)
		return false, err
	}
	// If the transaction pool is full, discard underpriced transactions
	if uint64(len(pool.All)) >= pool.Config.GlobalSlots+pool.Config.GlobalQueue {
		// If the new transaction is underpriced, don't accept it
		if pool.Priced.Underpriced(tx, pool.Locals) {
			log.Trace("Discarding underpriced transaction", "hash", hash, "price", tx.GasPrice())
			underpricedTxCounter.Inc(1)
			return false, ErrUnderpriced
		}
		// New transaction is better than our worse ones, make room for it
		drop := pool.Priced.Discard(len(pool.All)-int(pool.Config.GlobalSlots+pool.Config.GlobalQueue-1), pool.Locals)
		for _, tx := range drop {
			log.Trace("Discarding freshly underpriced transaction", "hash", tx.Hash(), "price", tx.GasPrice())
			underpricedTxCounter.Inc(1)
			pool.removeTx(tx.Hash())
		}
	}
	// If the transaction is replacing an already Pending one, do directly
	from, _ := types.Sender(pool.Signer, tx) // already validated
	if list := pool.Pending[from]; list != nil && list.Overlaps(tx) {
		// Nonce already Pending, check if required price bump is met
		inserted, old := list.Add(tx, pool.Config.PriceBump)
		if !inserted {
			pendingDiscardCounter.Inc(1)
			return false, ErrReplaceUnderpriced
		}
		// New transaction is better, replace old one
		if old != nil {
			delete(pool.All, old.Hash())
			pool.Priced.Removed()
			pendingReplaceCounter.Inc(1)
		}
		pool.All[tx.Hash()] = tx
		pool.Priced.Put(tx)
		pool.journalTx(from, tx)

		log.Trace("Pooled new executable transaction", "hash", hash, "from", from, "to", tx.To())

		// We've directly injected a replacement transaction, notify subsystems
		go pool.TxFeed.Send(TxPreEvent{tx})

		return old != nil, nil
	}
	// New transaction isn't replacing a Pending one, push into Queue
	replace, err := pool.enqueueTx(hash, tx)
	if err != nil {
		return false, err
	}
	// Mark local addresses and Journal local transactions
	if local {
		pool.Locals.add(from)
	}
	pool.journalTx(from, tx)

	log.Trace("Pooled new future transaction", "hash", hash, "from", from, "to", tx.To())
	return replace, nil
}

// enqueueTx inserts a new transaction into the non-executable transaction Queue.
//
// Note, this method assumes the pool lock is held!
func (pool *TxPool) enqueueTx(hash common.Hash, tx *types.Transaction) (bool, error) {
	// Try to insert the transaction into the future Queue
	from, _ := types.Sender(pool.Signer, tx) // already validated
	if pool.Queue[from] == nil {
		pool.Queue[from] = newTxList(false)
	}
	inserted, old := pool.Queue[from].Add(tx, pool.Config.PriceBump)
	if !inserted {
		// An older transaction was better, discard this
		queuedDiscardCounter.Inc(1)
		return false, ErrReplaceUnderpriced
	}
	// Discard any previous transaction and mark this
	if old != nil {
		delete(pool.All, old.Hash())
		pool.Priced.Removed()
		queuedReplaceCounter.Inc(1)
	}
	pool.All[hash] = tx
	pool.Priced.Put(tx)
	return old != nil, nil
}

// journalTx adds the specified transaction to the local disk Journal if it is
// deemed to have been sent from a local account.
func (pool *TxPool) journalTx(from common.Address, tx *types.Transaction) {
	// Only Journal if it's enabled and the transaction is local
	if pool.Journal == nil || !pool.Locals.Contains(from) {
		return
	}
	if err := pool.Journal.insert(tx); err != nil {
		log.Warn("Failed to Journal local transaction", "err", err)
	}
}

// promoteTx adds a transaction to the Pending (processable) list of transactions.
//
// Note, this method assumes the pool lock is held!
func (pool *TxPool) promoteTx(addr common.Address, hash common.Hash, tx *types.Transaction) {
	// Try to insert the transaction into the Pending Queue
	if pool.Pending[addr] == nil {
		pool.Pending[addr] = newTxList(true)
	}
	list := pool.Pending[addr]

	inserted, old := list.Add(tx, pool.Config.PriceBump)
	if !inserted {
		// An older transaction was better, discard this
		delete(pool.All, hash)
		pool.Priced.Removed()

		pendingDiscardCounter.Inc(1)
		return
	}
	// Otherwise discard any previous transaction and mark this
	if old != nil {
		delete(pool.All, old.Hash())
		pool.Priced.Removed()

		pendingReplaceCounter.Inc(1)
	}
	// Failsafe to work around direct Pending inserts (tests)
	if pool.All[hash] == nil {
		pool.All[hash] = tx
		pool.Priced.Put(tx)
	}
	// Set the potentially new Pending nonce and notify any subsystems of the new tx
	pool.Beats[addr] = time.Now()
	pool.PendingState.SetNonce(addr, tx.Nonce()+1)

	go pool.TxFeed.Send(TxPreEvent{tx})
}

// AddLocal enqueues a single transaction into the pool if it is valid, marking
// the sender as a local one in the mean time, ensuring it goes around the local
// pricing constraints.
func (pool *TxPool) AddLocal(tx *types.Transaction) error {
	return pool.addTx(tx, !pool.Config.NoLocals)
}

// AddRemote enqueues a single transaction into the pool if it is valid. If the
// sender is not among the locally tracked ones, full pricing constraints will
// apply.
func (pool *TxPool) AddRemote(tx *types.Transaction) error {
	return pool.addTx(tx, false)
}

// AddLocals enqueues a batch of transactions into the pool if they are valid,
// marking the senders as a local ones in the mean time, ensuring they go around
// the local pricing constraints.
func (pool *TxPool) AddLocals(txs []*types.Transaction) []error {
	return pool.addTxs(txs, !pool.Config.NoLocals)
}

// AddRemotes enqueues a batch of transactions into the pool if they are valid.
// If the senders are not among the locally tracked ones, full pricing constraints
// will apply.
func (pool *TxPool) AddRemotes(txs []*types.Transaction) []error {
	return pool.addTxs(txs, false)
}

// addTx enqueues a single transaction into the pool if it is valid.
func (pool *TxPool) addTx(tx *types.Transaction, local bool) error {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	// Try to inject the transaction and Update any State
	replace, err := pool.add(tx, local)
	if err != nil {
		return err
	}
	// If we added a new transaction, run promotion checks and return
	if !replace {
		from, _ := types.Sender(pool.Signer, tx) // already validated
		pool.promoteExecutables([]common.Address{from})
	}
	return nil
}

// addTxs attempts to Queue a batch of transactions if they are valid.
func (pool *TxPool) addTxs(txs []*types.Transaction, local bool) []error {
	pool.Mu.Lock()
	defer pool.Mu.Unlock()

	return pool.addTxsLocked(txs, local)
}

// addTxsLocked attempts to Queue a batch of transactions if they are valid,
// whilst assuming the transaction pool lock is already held.
func (pool *TxPool) addTxsLocked(txs []*types.Transaction, local bool) []error {
	// Add the batch of transaction, tracking the accepted ones
	dirty := make(map[common.Address]struct{})
	errs := make([]error, len(txs))

	for i, tx := range txs {
		var replace bool
		if replace, errs[i] = pool.add(tx, local); errs[i] == nil {
			if !replace {
				from, _ := types.Sender(pool.Signer, tx) // already validated
				dirty[from] = struct{}{}
			}
		}
	}
	// Only reprocess the internal State if something was actually added
	if len(dirty) > 0 {
		addrs := make([]common.Address, 0, len(dirty))
		for addr := range dirty {
			addrs = append(addrs, addr)
		}
		pool.promoteExecutables(addrs)
	}
	return errs
}

// Status returns the status (unknown/Pending/Queued) of a batch of transactions
// identified by their hashes.
func (pool *TxPool) Status(hashes []common.Hash) []TxStatus {
	pool.Mu.RLock()
	defer pool.Mu.RUnlock()

	status := make([]TxStatus, len(hashes))
	for i, hash := range hashes {
		if tx := pool.All[hash]; tx != nil {
			from, _ := types.Sender(pool.Signer, tx) // already validated
			if pool.Pending[from] != nil && pool.Pending[from].txs.items[tx.Nonce()] != nil {
				status[i] = TxStatusPending
			} else {
				status[i] = TxStatusQueued
			}
		}
	}
	return status
}

// Get returns a transaction if it is contained in the pool
// and nil otherwise.
func (pool *TxPool) Get(hash common.Hash) *types.Transaction {
	pool.Mu.RLock()
	defer pool.Mu.RUnlock()

	return pool.All[hash]
}

// removeTx removes a single transaction from the Queue, moving All subsequent
// transactions back to the future Queue.
func (pool *TxPool) removeTx(hash common.Hash) {
	// Fetch the transaction we wish to delete
	tx, ok := pool.All[hash]
	if !ok {
		return
	}
	addr, _ := types.Sender(pool.Signer, tx) // already validated during insertion

	// Remove it from the list of known transactions
	delete(pool.All, hash)
	pool.Priced.Removed()

	// Remove the transaction from the Pending lists and reset the account nonce
	if pending := pool.Pending[addr]; pending != nil {
		if removed, invalids := pending.Remove(tx); removed {
			// If no more Pending transactions are left, remove the list
			if pending.Empty() {
				delete(pool.Pending, addr)
				delete(pool.Beats, addr)
			}
			// Postpone any invalidated transactions
			for _, tx := range invalids {
				pool.enqueueTx(tx.Hash(), tx)
			}
			// Update the account nonce if needed
			if nonce := tx.Nonce(); pool.PendingState.GetNonce(addr) > nonce {
				pool.PendingState.SetNonce(addr, nonce)
			}
			return
		}
	}
	// Transaction is in the future Queue
	if future := pool.Queue[addr]; future != nil {
		future.Remove(tx)
		if future.Empty() {
			delete(pool.Queue, addr)
		}
	}
}

// promoteExecutables moves transactions that have become processable from the
// future Queue to the set of Pending transactions. During this process, All
// invalidated transactions (low nonce, low balance) are deleted.
func (pool *TxPool) promoteExecutables(accounts []common.Address) {
	// Gather All the accounts potentially needing updates
	if accounts == nil {
		accounts = make([]common.Address, 0, len(pool.Queue))
		for addr := range pool.Queue {
			accounts = append(accounts, addr)
		}
	}
	// Iterate over All accounts and promote any executable transactions
	for _, addr := range accounts {
		list := pool.Queue[addr]
		if list == nil {
			continue // Just in case someone calls with a non existing account
		}
		// Drop All transactions that are deemed too old (low nonce)
		for _, tx := range list.Forward(pool.CurrentState.GetNonce(addr)) {
			hash := tx.Hash()
			log.Trace("Removed old Queued transaction", "hash", hash)
			delete(pool.All, hash)
			pool.Priced.Removed()
		}
		// Drop All transactions that are too costly (low balance or out of Gas)
		drops, _ := list.Filter(pool.CurrentState.GetBalance(addr), pool.CurrentMaxGas)
		for _, tx := range drops {
			hash := tx.Hash()
			log.Trace("Removed unpayable Queued transaction", "hash", hash)
			delete(pool.All, hash)
			pool.Priced.Removed()
			queuedNofundsCounter.Inc(1)
		}
		// Gather All executable transactions and promote them
		for _, tx := range list.Ready(pool.PendingState.GetNonce(addr)) {
			hash := tx.Hash()
			log.Trace("Promoting Queued transaction", "hash", hash)
			pool.promoteTx(addr, hash, tx)
		}
		// Drop All transactions over the allowed limit
		if !pool.Locals.Contains(addr) {
			for _, tx := range list.Cap(int(pool.Config.AccountQueue)) {
				hash := tx.Hash()
				delete(pool.All, hash)
				pool.Priced.Removed()
				queuedRateLimitCounter.Inc(1)
				log.Trace("Removed cap-exceeding Queued transaction", "hash", hash)
			}
		}
		// Delete the entire Queue entry if it became empty.
		if list.Empty() {
			delete(pool.Queue, addr)
		}
	}
	// If the Pending limit is overflown, start equalizing allowances
	pending := uint64(0)
	for _, list := range pool.Pending {
		pending += uint64(list.Len())
	}
	if pending > pool.Config.GlobalSlots {
		pendingBeforeCap := pending
		// Assemble a spam order to penalize large transactors first
		spammers := prque.New()
		for addr, list := range pool.Pending {
			// Only evict transactions from high rollers
			if !pool.Locals.Contains(addr) && uint64(list.Len()) > pool.Config.AccountSlots {
				spammers.Push(addr, float32(list.Len()))
			}
		}
		// Gradually drop transactions from offenders
		offenders := []common.Address{}
		for pending > pool.Config.GlobalSlots && !spammers.Empty() {
			// Retrieve the next offender if not local address
			offender, _ := spammers.Pop()
			offenders = append(offenders, offender.(common.Address))

			// Equalize balances until All the same or below threshold
			if len(offenders) > 1 {
				// Calculate the equalization threshold for All current offenders
				threshold := pool.Pending[offender.(common.Address)].Len()

				// Iteratively reduce All offenders until below limit or threshold reached
				for pending > pool.Config.GlobalSlots && pool.Pending[offenders[len(offenders)-2]].Len() > threshold {
					for i := 0; i < len(offenders)-1; i++ {
						list := pool.Pending[offenders[i]]
						for _, tx := range list.Cap(list.Len() - 1) {
							// Drop the transaction from the global pools too
							hash := tx.Hash()
							delete(pool.All, hash)
							pool.Priced.Removed()

							// Update the account nonce to the dropped transaction
							if nonce := tx.Nonce(); pool.PendingState.GetNonce(offenders[i]) > nonce {
								pool.PendingState.SetNonce(offenders[i], nonce)
							}
							log.Trace("Removed fairness-exceeding Pending transaction", "hash", hash)
						}
						pending--
					}
				}
			}
		}
		// If still above threshold, reduce to limit or min allowance
		if pending > pool.Config.GlobalSlots && len(offenders) > 0 {
			for pending > pool.Config.GlobalSlots && uint64(pool.Pending[offenders[len(offenders)-1]].Len()) > pool.Config.AccountSlots {
				for _, addr := range offenders {
					list := pool.Pending[addr]
					for _, tx := range list.Cap(list.Len() - 1) {
						// Drop the transaction from the global pools too
						hash := tx.Hash()
						delete(pool.All, hash)
						pool.Priced.Removed()

						// Update the account nonce to the dropped transaction
						if nonce := tx.Nonce(); pool.PendingState.GetNonce(addr) > nonce {
							pool.PendingState.SetNonce(addr, nonce)
						}
						log.Trace("Removed fairness-exceeding Pending transaction", "hash", hash)
					}
					pending--
				}
			}
		}
		pendingRateLimitCounter.Inc(int64(pendingBeforeCap - pending))
	}
	// If we've Queued more transactions than the hard limit, drop oldest ones
	queued := uint64(0)
	for _, list := range pool.Queue {
		queued += uint64(list.Len())
	}
	if queued > pool.Config.GlobalQueue {
		// Sort All accounts with Queued transactions by heartbeat
		addresses := make(addresssByHeartbeat, 0, len(pool.Queue))
		for addr := range pool.Queue {
			if !pool.Locals.Contains(addr) { // don't drop Locals
				addresses = append(addresses, addressByHeartbeat{addr, pool.Beats[addr]})
			}
		}
		sort.Sort(addresses)

		// Drop transactions until the total is below the limit or only Locals remain
		for drop := queued - pool.Config.GlobalQueue; drop > 0 && len(addresses) > 0; {
			addr := addresses[len(addresses)-1]
			list := pool.Queue[addr.address]

			addresses = addresses[:len(addresses)-1]

			// Drop All transactions if they are less than the overflow
			if size := uint64(list.Len()); size <= drop {
				for _, tx := range list.Flatten() {
					pool.removeTx(tx.Hash())
				}
				drop -= size
				queuedRateLimitCounter.Inc(int64(size))
				continue
			}
			// Otherwise drop only last few transactions
			txs := list.Flatten()
			for i := len(txs) - 1; i >= 0 && drop > 0; i-- {
				pool.removeTx(txs[i].Hash())
				drop--
				queuedRateLimitCounter.Inc(1)
			}
		}
	}
}

// demoteUnexecutables removes invalid and Processed transactions from the pools
// executable/Pending Queue and any subsequent transactions that become unexecutable
// are moved back into the future Queue.
func (pool *TxPool) demoteUnexecutables() {
	// Iterate over All accounts and demote any non-executable transactions
	for addr, list := range pool.Pending {
		nonce := pool.CurrentState.GetNonce(addr)

		// Drop All transactions that are deemed too old (low nonce)
		for _, tx := range list.Forward(nonce) {
			hash := tx.Hash()
			log.Trace("Removed old Pending transaction", "hash", hash)
			delete(pool.All, hash)
			pool.Priced.Removed()
		}
		// Drop All transactions that are too costly (low balance or out of Gas), and Queue any invalids back for later
		drops, invalids := list.Filter(pool.CurrentState.GetBalance(addr), pool.CurrentMaxGas)
		for _, tx := range drops {
			hash := tx.Hash()
			log.Trace("Removed unpayable Pending transaction", "hash", hash)
			delete(pool.All, hash)
			pool.Priced.Removed()
			pendingNofundsCounter.Inc(1)
		}
		for _, tx := range invalids {
			hash := tx.Hash()
			log.Trace("Demoting Pending transaction", "hash", hash)
			pool.enqueueTx(hash, tx)
		}
		// If there's a gap in front, warn (should never happen) and postpone All transactions
		if list.Len() > 0 && list.txs.Get(nonce) == nil {
			for _, tx := range list.Cap(0) {
				hash := tx.Hash()
				log.Error("Demoting invalidated transaction", "hash", hash)
				pool.enqueueTx(hash, tx)
			}
		}
		// Delete the entire Queue entry if it became empty.
		if list.Empty() {
			delete(pool.Pending, addr)
			delete(pool.Beats, addr)
		}
	}
}

// addressByHeartbeat is an account address tagged with its last activity timestamp.
type addressByHeartbeat struct {
	address   common.Address
	heartbeat time.Time
}

type addresssByHeartbeat []addressByHeartbeat

func (a addresssByHeartbeat) Len() int           { return len(a) }
func (a addresssByHeartbeat) Less(i, j int) bool { return a[i].heartbeat.Before(a[j].heartbeat) }
func (a addresssByHeartbeat) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// accountSet is simply a set of addresses to check for existence, and a Signer
// capable of deriving addresses from transactions.
type accountSet struct {
	accounts map[common.Address]struct{}
	signer   types.Signer
}

// newAccountSet creates a new address set with an associated Signer for sender
// derivations.
func newAccountSet(signer types.Signer) *accountSet {
	return &accountSet{
		accounts: make(map[common.Address]struct{}),
		signer:   signer,
	}
}

// Contains checks if a given address is contained within the set.
func (as *accountSet) Contains(addr common.Address) bool {
	_, exist := as.accounts[addr]
	return exist
}

// containsTx checks if the sender of a given tx is within the set. If the sender
// cannot be derived, this method returns false.
func (as *accountSet) containsTx(tx *types.Transaction) bool {
	if addr, err := types.Sender(as.signer, tx); err == nil {
		return as.Contains(addr)
	}
	return false
}

// add inserts a new address into the set to track.
func (as *accountSet) add(addr common.Address) {
	as.accounts[addr] = struct{}{}
}
