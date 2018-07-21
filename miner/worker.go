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

package miner

import (
	"bytes"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/miner"

	"github.com/tomochain/tomochain/contracts"
	"github.com/tomochain/tomochain/consensus"
	"github.com/tomochain/tomochain/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"sync"
	"github.com/ethereum/go-ethereum/ethdb"
	"gopkg.in/fatih/set.v0"

	tomoCore "github.com/tomochain/tomochain/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
)

// Agent can register themself with the Worker
type Agent interface {
	Work() chan<- *Work
	SetReturnCh(chan<- *Result)
	Stop()
	Start()
	GetHashRate() int64
}
type Work struct {
	Config *params.ChainConfig
	Signer types.Signer

	State     *state.StateDB // apply State changes here
	Ancestors *set.Set       // ancestor set (used for checking uncle parent validity)
	Family    *set.Set       // Family set (used for checking uncle invalidity)
	Uncles    *set.Set       // uncle set
	Tcount    int            // tx count in cycle

	Block *types.Block // the new block

	Header   *types.Header
	Txs      []*types.Transaction
	Receipts []*types.Receipt

	CreatedAt time.Time
}

type Result struct {
	Work  *Work
	Block *types.Block
}

// Worker is the main object which takes care of applying messages to the new state
type Worker struct {
	Config *params.ChainConfig
	Engine consensus.Engine

	Mu sync.Mutex

	// Update loop
	Mux          *event.TypeMux
	TxCh         chan core.TxPreEvent
	TxSub        event.Subscription
	ChainHeadCh  chan core.ChainHeadEvent
	ChainHeadSub event.Subscription
	ChainSideCh  chan core.ChainSideEvent
	ChainSideSub event.Subscription
	Wg           sync.WaitGroup

	Agents map[Agent]struct{}
	Recv   chan *Result

	Eth     Backend
	Chain   *tomoCore.TomoBlockChain
	Proc    core.Validator
	ChainDb ethdb.Database

	Coinbase common.Address
	Extra    []byte

	CurrentMu sync.Mutex
	Current   *Work

	UncleMu        sync.Mutex
	PossibleUncles map[common.Hash]*types.Block

	Unconfirmed *miner.UnconfirmedBlocks // set of locally mined blocks pending canonicalness confirmations

	// atomic status counters
	Mining int32
	AtWork int32
}

func NewWorker(config *params.ChainConfig, engine consensus.Engine, coinbase common.Address, eth Backend, mux *event.TypeMux) *Worker {
	worker := &Worker{
		Config:         config,
		Engine:         engine,
		Eth:            eth,
		Mux:            mux,
		TxCh:           make(chan core.TxPreEvent, miner.TxChanSize),
		ChainHeadCh:    make(chan core.ChainHeadEvent, miner.ChainHeadChanSize),
		ChainSideCh:    make(chan core.ChainSideEvent, miner.ChainSideChanSize),
		ChainDb:        eth.GetChainDb(),
		Recv:           make(chan *Result, miner.ResultQueueSize),
		Chain:          eth.BlockChain(),
		Proc:           eth.BlockChain().GetValidator(),
		PossibleUncles: make(map[common.Hash]*types.Block),
		Coinbase:       coinbase,
		Agents:         make(map[Agent]struct{}),
		Unconfirmed:    miner.NewUnconfirmedBlocks(eth.BlockChain(), miner.MiningLogAtDepth),
	}
	// Subscribe TxPreEvent for tx pool
	worker.TxSub = eth.GetTxPool().SubscribeTxPreEvent(worker.TxCh)
	// Subscribe events for blockchain
	worker.ChainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.ChainHeadCh)
	worker.ChainSideSub = eth.BlockChain().SubscribeChainSideEvent(worker.ChainSideCh)
	go worker.Update()

	go worker.wait()
	worker.commitNewWork()

	return worker
}

func (self *Worker) wait() {
	for {
		mustCommitNewWork := true
		for result := range self.Recv {
			atomic.AddInt32(&self.AtWork, -1)

			if result == nil {
				continue
			}
			block := result.Block
			work := result.Work

			// Update the block hash in all logs since it is now available and not when the
			// receipt/log of individual transactions were created.
			for _, r := range work.Receipts {
				for _, l := range r.Logs {
					l.BlockHash = block.Hash()
				}
			}
			for _, log := range work.State.Logs() {
				log.BlockHash = block.Hash()
			}
			stat, err := self.Chain.WriteBlockWithState(block, work.Receipts, work.State)
			if err != nil {
				log.Error("Failed writing block to chain", "err", err)
				continue
			}
			// check if canon block and write transactions
			if stat == core.CanonStatTy {
				// implicit by posting ChainHeadEvent
				mustCommitNewWork = false
			}
			// Broadcast the block and announce chain insertion event
			self.Mux.Post(core.NewMinedBlockEvent{Block: block})
			var (
				events []interface{}
				logs   = work.State.Logs()
			)
			events = append(events, core.ChainEvent{Block: block, Hash: block.Hash(), Logs: logs})
			if stat == core.CanonStatTy {
				events = append(events, core.ChainHeadEvent{Block: block})
			}
			self.Chain.PostChainEvents(events, logs)

			// Insert the block into the set of pending ones to wait for confirmations
			self.Unconfirmed.Insert(block.NumberU64(), block.Hash())

			if mustCommitNewWork {
				self.commitNewWork()
			}

			if self.Config.Clique != nil {
				c := self.Engine.(*clique.Clique)
				snap, err := c.GetSnapshot(self.Chain, block.Header())
				if err != nil {
					log.Error("Fail to get snapshot for sign tx signer.")
					return
				}
				if _, authorized := snap.Signers[self.Coinbase]; !authorized {
					log.Error("Coinbase address not in snapshot signers.")
					return
				}
				// Send tx sign to smart contract blockSigners.
				if err := contracts.CreateTransactionSign(self.Config, self.Eth.GetTxPool(), self.Eth.GetAccountManager(), block); err != nil {
					log.Error("Fail to create tx sign for signer", "error", "err")
				}
			}
		}
	}
}

func (self *Worker) commitNewWork() {
	self.Mu.Lock()
	defer self.Mu.Unlock()
	self.UncleMu.Lock()
	defer self.UncleMu.Unlock()
	self.CurrentMu.Lock()
	defer self.CurrentMu.Unlock()

	tstart := time.Now()
	parent := self.Chain.GetCurrentBlock()

	// Only try to commit new work if we are mining
	if atomic.LoadInt32(&self.Mining) == 1 {
		// check if we are right after parent's coinbase in the list
		// only go with Clique
		if self.Config.Clique != nil {
			c := self.Engine.(*clique.Clique)
			snap, err := c.GetSnapshot(self.Chain, parent.Header())
			if err != nil {
				log.Error("Failed when trying to commit new work", "err", err)
				return
			}
			ok, err := clique.YourTurn(snap, parent.Header(), self.Coinbase)
			if err != nil {
				log.Error("Failed when trying to commit new work", "err", err)
				return
			}
			if !ok {
				log.Info("Not our turn to commit block. Wait for next time")
				return
			}
		}
	}

	tstamp := tstart.Unix()
	if parent.Time().Cmp(new(big.Int).SetInt64(tstamp)) >= 0 {
		tstamp = parent.Time().Int64() + 1
	}
	// this will ensure we're not going off too far in the future
	if now := time.Now().Unix(); tstamp > now+1 {
		wait := time.Duration(tstamp-now) * time.Second
		log.Info("Mining too far in the future", "wait", common.PrettyDuration(wait))
		time.Sleep(wait)
	}

	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent),
		Extra:      self.Extra,
		Time:       big.NewInt(tstamp),
	}
	// Only set the coinbase if we are mining (avoid spurious block rewards)
	if atomic.LoadInt32(&self.Mining) == 1 {
		header.Coinbase = self.Coinbase
	}
	if err := self.Engine.Prepare(self.Chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return
	}
	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := self.Config.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if self.Config.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := self.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		return
	}
	// Create the current work task and check any fork transitions needed
	work := self.Current
	if self.Config.DAOForkSupport && self.Config.DAOForkBlock != nil && self.Config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(work.State)
	}
	pending, err := self.Eth.GetTxPool().GetPending()
	if err != nil {
		log.Error("Failed to fetch pending transactions", "err", err)
		return
	}
	txs := types.NewTransactionsByPriceAndNonce(self.Current.Signer, pending)
	work.CommitTransactions(self.Mux, txs, self.Chain, self.Coinbase)

	// compute uncles for the new block.
	var (
		uncles    []*types.Header
		badUncles []common.Hash
	)
	for hash, uncle := range self.PossibleUncles {
		if len(uncles) == 2 {
			break
		}
		if err := self.commitUncle(work, uncle.Header()); err != nil {
			log.Trace("Bad uncle found and will be removed", "hash", hash)
			log.Trace(fmt.Sprint(uncle))

			badUncles = append(badUncles, hash)
		} else {
			log.Debug("Committing new uncle to block", "hash", hash)
			uncles = append(uncles, uncle.Header())
		}
	}
	for _, hash := range badUncles {
		delete(self.PossibleUncles, hash)
	}
	// Create the new block to seal with the consensus engine
	if work.Block, err = self.Engine.Finalize(self.Chain, header, work.State, work.Txs, uncles, work.Receipts); err != nil {
		log.Error("Failed to finalize block for sealing", "err", err)
		return
	}
	// We only care about logging if we're actually mining.
	if atomic.LoadInt32(&self.Mining) == 1 {
		log.Info("Commit new mining work", "number", work.Block.Number(), "txs", work.Tcount, "uncles", len(uncles), "elapsed", common.PrettyDuration(time.Since(tstart)))
		self.Unconfirmed.Shift(work.Block.NumberU64() - 1)
	}
	if (work.Config.Clique != nil) && (work.Block.NumberU64()%work.Config.Clique.Epoch) == 0 {
		tomoCore.Checkpoint <- 1
	}
	self.push(work)
}

func (self *Worker) setEtherbase(addr common.Address) {
	self.Mu.Lock()
	defer self.Mu.Unlock()
	self.Coinbase = addr
}

func (self *Worker) setExtra(extra []byte) {
	self.Mu.Lock()
	defer self.Mu.Unlock()
	self.Extra = extra
}

func (self *Worker) pending() (*types.Block, *state.StateDB) {
	self.CurrentMu.Lock()
	defer self.CurrentMu.Unlock()

	if atomic.LoadInt32(&self.Mining) == 0 {
		return types.NewBlock(
			self.Current.Header,
			self.Current.Txs,
			nil,
			self.Current.Receipts,
		), self.Current.State.Copy()
	}
	return self.Current.Block, self.Current.State.Copy()
}

func (self *Worker) pendingBlock() *types.Block {
	self.CurrentMu.Lock()
	defer self.CurrentMu.Unlock()

	if atomic.LoadInt32(&self.Mining) == 0 {
		return types.NewBlock(
			self.Current.Header,
			self.Current.Txs,
			nil,
			self.Current.Receipts,
		)
	}
	return self.Current.Block
}

func (self *Worker) start() {
	self.Mu.Lock()
	defer self.Mu.Unlock()

	atomic.StoreInt32(&self.Mining, 1)

	// spin up Agents
	for agent := range self.Agents {
		agent.Start()
	}
}

func (self *Worker) stop() {
	self.Wg.Wait()

	self.Mu.Lock()
	defer self.Mu.Unlock()
	if atomic.LoadInt32(&self.Mining) == 1 {
		for agent := range self.Agents {
			agent.Stop()
		}
	}
	atomic.StoreInt32(&self.Mining, 0)
	atomic.StoreInt32(&self.AtWork, 0)
}

func (self *Worker) register(agent Agent) {
	self.Mu.Lock()
	defer self.Mu.Unlock()
	self.Agents[agent] = struct{}{}
	agent.SetReturnCh(self.Recv)
}

func (self *Worker) unregister(agent Agent) {
	self.Mu.Lock()
	defer self.Mu.Unlock()
	delete(self.Agents, agent)
	agent.Stop()
}

func (self *Worker) Update() {
	defer self.TxSub.Unsubscribe()
	defer self.ChainHeadSub.Unsubscribe()
	defer self.ChainSideSub.Unsubscribe()

	for {
		// A real event arrived, process interesting content
		select {
		// Handle ChainHeadEvent
		case <-self.ChainHeadCh:
			self.commitNewWork()

			// Handle ChainSideEvent
		case ev := <-self.ChainSideCh:
			self.UncleMu.Lock()
			self.PossibleUncles[ev.Block.Hash()] = ev.Block
			self.UncleMu.Unlock()

			// Handle TxPreEvent
		case ev := <-self.TxCh:
			// Apply transaction to the pending State if we're not Mining
			if atomic.LoadInt32(&self.Mining) == 0 {
				self.CurrentMu.Lock()
				acc, _ := types.Sender(self.Current.Signer, ev.Tx)
				txs := map[common.Address]types.Transactions{acc: {ev.Tx}}
				txset := types.NewTransactionsByPriceAndNonce(self.Current.Signer, txs)

				self.Current.CommitTransactions(self.Mux, txset, self.Chain, self.Coinbase)
				self.CurrentMu.Unlock()
			} else {
				// If we're Mining, but nothing is being processed, wake on new transactions
				if self.Config.Clique != nil && self.Config.Clique.Period == 0 {
					self.commitNewWork()
				}
			}

			// System stopped
		case <-self.TxSub.Err():
			return
		case <-self.ChainHeadSub.Err():
			return
		case <-self.ChainSideSub.Err():
			return
		}
	}
}

// push sends a new work task to currently live miner Agents.
func (self *Worker) push(work *Work) {
	if atomic.LoadInt32(&self.Mining) != 1 {
		return
	}
	for agent := range self.Agents {
		atomic.AddInt32(&self.AtWork, 1)
		if ch := agent.Work(); ch != nil {
			ch <- work
		}
	}
}

// makeCurrent creates a new environment for the Current cycle.
func (self *Worker) makeCurrent(parent *types.Block, header *types.Header) error {
	state, err := self.Chain.StateAt(parent.Root())
	if err != nil {
		return err
	}
	work := &Work{
		Config:    self.Config,
		Signer:    types.NewEIP155Signer(self.Config.ChainId),
		State:     state,
		Ancestors: set.New(),
		Family:    set.New(),
		Uncles:    set.New(),
		Header:    header,
		CreatedAt: time.Now(),
	}

	// when 08 is processed Ancestors contain 07 (quick block)
	for _, ancestor := range self.Chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			work.Family.Add(uncle.Hash())
		}
		work.Family.Add(ancestor.Hash())
		work.Ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	work.Tcount = 0
	self.Current = work
	return nil
}

func (self *Worker) commitUncle(work *Work, uncle *types.Header) error {
	hash := uncle.Hash()
	if work.Uncles.Has(hash) {
		return fmt.Errorf("uncle not unique")
	}
	if !work.Ancestors.Has(uncle.ParentHash) {
		return fmt.Errorf("uncle's parent unknown (%x)", uncle.ParentHash[0:4])
	}
	if work.Family.Has(hash) {
		return fmt.Errorf("uncle already in Family (%x)", hash)
	}
	work.Uncles.Add(uncle.Hash())
	return nil
}




func (env *Work) CommitTransactions(mux *event.TypeMux, txs *types.TransactionsByPriceAndNonce, bc *tomoCore.TomoBlockChain, coinbase common.Address) {
	gp := new(core.GasPool).AddGas(env.Header.GasLimit)

	var coalescedLogs []*types.Log

	for {
		// If we don't have enough gas for any further transactions then we're done
		if gp.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "gp", gp)
			break
		}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 Signer regardless of the Current hf.
		from, _ := types.Sender(env.Signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !env.Config.IsEIP155(env.Header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", env.Config.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		env.State.Prepare(tx.Hash(), common.Hash{}, env.Tcount)

		err, logs := env.commitTransaction(tx, bc, coinbase, gp)
		switch err {
		case core.ErrGasLimitReached:
			// Pop the Current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for Current block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			env.Tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}

	if len(coalescedLogs) > 0 || env.Tcount > 0 {
		// make a copy, the State caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		go func(logs []*types.Log, tcount int) {
			if len(logs) > 0 {
				mux.Post(core.PendingLogsEvent{Logs: logs})
			}
			if tcount > 0 {
				mux.Post(core.PendingStateEvent{})
			}
		}(cpy, env.Tcount)
	}
}

func (env *Work) commitTransaction(tx *types.Transaction, bc *tomoCore.TomoBlockChain, coinbase common.Address, gp *core.GasPool) (error, []*types.Log) {
	snap := env.State.Snapshot()

	receipt, _, err := tomoCore.ApplyTransaction(env.Config, bc, &coinbase, gp, env.State, env.Header, tx, &env.Header.GasUsed, vm.Config{})
	if err != nil {
		env.State.RevertToSnapshot(snap)
		return err, nil
	}
	env.Txs = append(env.Txs, tx)
	env.Receipts = append(env.Receipts, receipt)

	return nil, receipt.Logs
}

