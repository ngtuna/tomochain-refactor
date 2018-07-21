// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either Version 3 of the License, or
// (at your option) any later Version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
)

const (
	ForceSyncCycle      = 10 * time.Second // Time interval to force syncs, even if few Peers are available
	MinDesiredPeerCount = 5                // Amount of Peers desired to start syncing

	// This is the target size for the packs of transactions sent by txsyncLoop.
	// A pack can get larger than this if a single transactions exceeds this size.
	TxsyncPackSize = 100 * 1024
)

type Txsync struct {
	P   *Peer
	Txs []*types.Transaction
}

// syncTransactions starts sending all currently pending transactions to the given Peer.
func (pm *ProtocolManager) syncTransactions(p *Peer) {
	var txs types.Transactions
	pending, _ := pm.Txpool.GetPending()
	for _, batch := range pending {
		txs = append(txs, batch...)
	}
	if len(txs) == 0 {
		return
	}
	select {
	case pm.TxsyncCh <- &Txsync{p, txs}:
	case <-pm.QuitSync:
	}
}

// txsyncLoop takes care of the initial transaction sync for each new
// connection. When a new Peer appears, we relay all currently pending
// transactions. In order to minimise egress bandwidth usage, we send
// the transactions in small packs to one Peer at a time.
func (pm *ProtocolManager) txsyncLoop() {
	var (
		pending = make(map[discover.NodeID]*Txsync)
		sending = false               // whether a send is active
		pack    = new(Txsync)         // the pack that is being sent
		done    = make(chan error, 1) // result of the send
	)

	// send starts a sending a pack of transactions from the sync.
	send := func(s *Txsync) {
		// Fill pack with transactions up to the target size.
		size := common.StorageSize(0)
		pack.P = s.P
		pack.Txs = pack.Txs[:0]
		for i := 0; i < len(s.Txs) && size < TxsyncPackSize; i++ {
			pack.Txs = append(pack.Txs, s.Txs[i])
			size += s.Txs[i].Size()
		}
		// Remove the transactions that will be sent.
		s.Txs = s.Txs[:copy(s.Txs, s.Txs[len(pack.Txs):])]
		if len(s.Txs) == 0 {
			delete(pending, s.P.ID())
		}
		// Send the pack in the background.
		s.P.Log().Trace("Sending batch of transactions", "count", len(pack.Txs), "bytes", size)
		sending = true
		go func() { done <- pack.P.SendTransactions(pack.Txs) }()
	}

	// pick chooses the next pending sync.
	pick := func() *Txsync {
		if len(pending) == 0 {
			return nil
		}
		n := rand.Intn(len(pending)) + 1
		for _, s := range pending {
			if n--; n == 0 {
				return s
			}
		}
		return nil
	}

	for {
		select {
		case s := <-pm.TxsyncCh:
			pending[s.P.ID()] = s
			if !sending {
				send(s)
			}
		case err := <-done:
			sending = false
			// Stop tracking Peers that cause send failures.
			if err != nil {
				pack.P.Log().Debug("Transaction send failed", "err", err)
				delete(pending, pack.P.ID())
			}
			// Schedule the next send.
			if s := pick(); s != nil {
				send(s)
			}
		case <-pm.QuitSync:
			return
		}
	}
}

// syncer is responsible for periodically synchronising with the network, both
// downloading hashes and blocks as well as handling the announcement handler.
func (pm *ProtocolManager) syncer() {
	// Start and ensure cleanup of sync mechanisms
	pm.Fetcher.Start()
	defer pm.Fetcher.Stop()
	defer pm.Downloader.Terminate()

	// Wait for different events to fire synchronisation operations
	forceSync := time.NewTicker(ForceSyncCycle)
	defer forceSync.Stop()

	for {
		select {
		case <-pm.NewPeerCh:
			// Make sure we have Peers to select from, then sync
			if pm.Peers.Len() < MinDesiredPeerCount {
				break
			}
			go pm.Synchronise(pm.Peers.BestPeer())

		case <-forceSync.C:
			// Force a sync even if not enough Peers are present
			go pm.Synchronise(pm.Peers.BestPeer())

		case <-pm.NoMorePeers:
			return
		}
	}
}

// Synchronise tries to sync up our local Block chain with a remote Peer.
func (pm *ProtocolManager) Synchronise(peer *Peer) {
	// Short circuit if no Peers are available
	if peer == nil {
		return
	}
	// Make sure the Peer's TD is higher than our own
	currentBlock := pm.Blockchain.GetCurrentBlock()
	td := pm.Blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())

	pHead, pTd := peer.GetHead()
	if pTd.Cmp(td) <= 0 {
		return
	}
	// Otherwise try to sync with the Downloader
	mode := downloader.FullSync
	if atomic.LoadUint32(&pm.FastSync) == 1 {
		// Fast sync was explicitly requested, and explicitly granted
		mode = downloader.FastSync
	} else if currentBlock.NumberU64() == 0 && pm.Blockchain.GetCurrentFastBlock().NumberU64() > 0 {
		// The database seems empty as the current Block is the genesis. Yet the fast
		// Block is ahead, so fast sync was enabled for this node at a certain point.
		// The only scenario where this can happen is if the user manually (or via a
		// bad Block) rolled back a fast sync node below the sync point. In this case
		// however it's safe to reenable fast sync.
		atomic.StoreUint32(&pm.FastSync, 1)
		mode = downloader.FastSync
	}

	if mode == downloader.FastSync {
		// Make sure the Peer's total difficulty we are synchronizing is higher.
		if pm.Blockchain.GetTdByHash(pm.Blockchain.GetCurrentFastBlock().Hash()).Cmp(pTd) >= 0 {
			return
		}
	}

	// Run the sync cycle, and disable fast sync if we've went past the pivot Block
	if err := pm.Downloader.Synchronise(peer.Id, pHead, pTd, mode); err != nil {
		return
	}
	if atomic.LoadUint32(&pm.FastSync) == 1 {
		log.Info("Fast sync complete, auto disabling")
		atomic.StoreUint32(&pm.FastSync, 0)
	}
	atomic.StoreUint32(&pm.AcceptTxs, 1) // Mark initial sync done
	if head := pm.Blockchain.GetCurrentBlock(); head.NumberU64() > 0 {
		// We've completed a sync cycle, notify all Peers of new state. This path is
		// essential in star-topology networks where a gateway node needs to notify
		// all its out-of-date Peers of the availability of a new Block. This failure
		// scenario will most often crop up in private and hackathon networks with
		// degenerate connectivity, but it should be healthy for the mainnet too to
		// more reliably update Peers or the local TD state.
		go pm.BroadcastBlock(head, false)
	}
}
