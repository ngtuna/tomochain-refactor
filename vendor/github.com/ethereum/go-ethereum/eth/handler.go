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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/fetcher"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	SoftResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	EstHeaderRlpSize  = 500             // Approximate size of an RLP encoded Block header

	// TxChanSize is the size of channel listening to TxPreEvent.
	// The number is referenced from the size of tx pool.
	TxChanSize = 4096
)

var (
	DaoChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the DAO handshake challenge
)

// ErrIncompatibleConfig is returned if the requested protocols and configs are
// not compatible (low protocol Version restrictions and high requirements).
var ErrIncompatibleConfig = errors.New("incompatible configuration")

func ErrResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type ProtocolManager struct {
	NetworkId uint64

	FastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	AcceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	Txpool      TxPool
	Blockchain  *core.BlockChain
	Chainconfig *params.ChainConfig
	MaxPeers    int

	Downloader *downloader.Downloader
	Fetcher    *fetcher.Fetcher
	Peers      *PeerSet

	SubProtocols []p2p.Protocol

	EventMux      *event.TypeMux
	TxCh          chan core.TxPreEvent
	TxSub         event.Subscription
	MinedBlockSub *event.TypeMuxSubscription

	// channels for Fetcher, syncer, txsyncLoop
	NewPeerCh   chan *Peer
	TxsyncCh    chan *Txsync
	QuitSync    chan struct{}
	NoMorePeers chan struct{}

	// wait group is used for graceful shutdowns during downloading
	// and processing
	Wg sync.WaitGroup
}

// NewProtocolManager returns a new ethereum sub protocol manager. The Ethereum sub protocol manages Peers capable
// with the ethereum network.
func NewProtocolManager(config *params.ChainConfig, mode downloader.SyncMode, networkId uint64, mux *event.TypeMux, txpool TxPool, engine consensus.Engine, blockchain *core.BlockChain, chaindb ethdb.Database) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		NetworkId:   networkId,
		EventMux:    mux,
		Txpool:      txpool,
		Blockchain:  blockchain,
		Chainconfig: config,
		Peers:       NewPeerSet(),
		NewPeerCh:   make(chan *Peer),
		NoMorePeers: make(chan struct{}),
		TxsyncCh:    make(chan *Txsync),
		QuitSync:    make(chan struct{}),
	}
	// Figure out whether to allow fast sync or not
	if mode == downloader.FastSync && blockchain.GetCurrentBlock().NumberU64() > 0 {
		log.Warn("Blockchain not empty, fast sync disabled")
		mode = downloader.FullSync
	}
	if mode == downloader.FastSync {
		manager.FastSync = uint32(1)
	}
	// Initiate a sub-protocol for every implemented Version we can Handle
	manager.SubProtocols = make([]p2p.Protocol, 0, len(ProtocolVersions))
	for i, version := range ProtocolVersions {
		// Skip protocol Version if incompatible with the mode of operation
		if mode == downloader.FastSync && version < Eth63 {
			continue
		}
		// Compatible; initialise the sub-protocol
		version := version // Closure for the run
		manager.SubProtocols = append(manager.SubProtocols, p2p.Protocol{
			Name:    ProtocolName,
			Version: version,
			Length:  ProtocolLengths[i],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				peer := manager.NewPeer(int(version), p, rw)
				select {
				case manager.NewPeerCh <- peer:
					manager.Wg.Add(1)
					defer manager.Wg.Done()
					return manager.Handle(peer)
				case <-manager.QuitSync:
					return p2p.DiscQuitting
				}
			},
			NodeInfo: func() interface{} {
				return manager.NodeInfo()
			},
			PeerInfo: func(id discover.NodeID) interface{} {
				if p := manager.Peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
					return p.Info()
				}
				return nil
			},
		})
	}
	if len(manager.SubProtocols) == 0 {
		return nil, ErrIncompatibleConfig
	}
	// Construct the different synchronisation mechanisms
	manager.Downloader = downloader.New(mode, chaindb, manager.EventMux, blockchain, nil, manager.RemovePeer)

	validator := func(header *types.Header) error {
		return engine.VerifyHeader(blockchain, header, true)
	}
	heighter := func() uint64 {
		return blockchain.GetCurrentBlock().NumberU64()
	}
	inserter := func(blocks types.Blocks) (int, error) {
		// If fast sync is running, deny importing weird blocks
		if atomic.LoadUint32(&manager.FastSync) == 1 {
			log.Warn("Discarded bad propagated Block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
			return 0, nil
		}
		atomic.StoreUint32(&manager.AcceptTxs, 1) // Mark initial sync done on any Fetcher import
		return manager.Blockchain.InsertChain(blocks)
	}
	manager.Fetcher = fetcher.New(blockchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, inserter, manager.RemovePeer)

	return manager, nil
}

func (pm *ProtocolManager) RemovePeer(id string) {
	// Short circuit if the Peer was already removed
	peer := pm.Peers.Peer(id)
	if peer == nil {
		return
	}
	log.Debug("Removing Ethereum Peer", "Peer", id)

	// Unregister the Peer from the Downloader and Ethereum Peer set
	pm.Downloader.UnregisterPeer(id)
	if err := pm.Peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "Peer", id, "err", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.MaxPeers = maxPeers

	// broadcast transactions
	pm.TxCh = make(chan core.TxPreEvent, TxChanSize)
	pm.TxSub = pm.Txpool.SubscribeTxPreEvent(pm.TxCh)
	go pm.txBroadcastLoop()

	// broadcast mined blocks
	pm.MinedBlockSub = pm.EventMux.Subscribe(core.NewMinedBlockEvent{})
	go pm.minedBroadcastLoop()

	// start sync handlers
	go pm.syncer()
	go pm.txsyncLoop()
}

func (pm *ProtocolManager) Stop() {
	log.Info("Stopping Ethereum protocol")

	pm.TxSub.Unsubscribe()         // quits txBroadcastLoop
	pm.MinedBlockSub.Unsubscribe() // quits blockBroadcastLoop

	// Quit the sync loop.
	// After this send has completed, no new Peers will be accepted.
	pm.NoMorePeers <- struct{}{}

	// Quit Fetcher, txsyncLoop.
	close(pm.QuitSync)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the Peer set.
	// sessions which are already established but not added to pm.Peers yet
	// will exit when they try to register.
	pm.Peers.Close()

	// Wait for all Peer handler goroutines and the loops to come down.
	pm.Wg.Wait()

	log.Info("Ethereum protocol stopped")
}

func (pm *ProtocolManager) NewPeer(pv int, p *p2p.Peer, rw p2p.MsgReadWriter) *Peer {
	return NewPeer(pv, p, NewMeteredMsgWriter(rw))
}

// Handle is the callback invoked to manage the life cycle of an eth Peer. When
// this function terminates, the Peer is disconnected.
func (pm *ProtocolManager) Handle(p *Peer) error {
	// Ignore MaxPeers if this is a trusted Peer
	if pm.Peers.Len() >= pm.MaxPeers && !p.Peer.Info().Network.Trusted {
		return p2p.DiscTooManyPeers
	}
	p.Log().Debug("Ethereum Peer connected", "name", p.Name())

	// Execute the Ethereum handshake
	var (
		genesis = pm.Blockchain.Genesis()
		head    = pm.Blockchain.CurrentHeader()
		hash    = head.Hash()
		number  = head.Number.Uint64()
		td      = pm.Blockchain.GetTd(hash, number)
	)
	if err := p.Handshake(pm.NetworkId, td, hash, genesis.Hash()); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	if rw, ok := p.Rw.(*MeteredMsgReadWriter); ok {
		rw.Init(p.Version)
	}
	// Register the Peer locally
	if err := pm.Peers.Register(p); err != nil {
		p.Log().Error("Ethereum Peer registration failed", "err", err)
		return err
	}
	defer pm.RemovePeer(p.Id)

	// Register the Peer in the Downloader. If the Downloader considers it banned, we disconnect
	if err := pm.Downloader.RegisterPeer(p.Id, p.Version, p); err != nil {
		return err
	}
	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	pm.syncTransactions(p)

	// If we're DAO hard-fork aware, validate any remote Peer with regard to the hard-fork
	if daoBlock := pm.Chainconfig.DAOForkBlock; daoBlock != nil {
		// Request the Peer's DAO fork header for extra-data validation
		if err := p.RequestHeadersByNumber(daoBlock.Uint64(), 1, 0, false); err != nil {
			return err
		}
		// Start a timer to disconnect if the Peer doesn't reply in time
		p.ForkDrop = time.AfterFunc(DaoChallengeTimeout, func() {
			p.Log().Debug("Timed out DAO fork-check, dropping")
			pm.RemovePeer(p.Id)
		})
		// Make sure it's cleaned up if the Peer dies off
		defer func() {
			if p.ForkDrop != nil {
				p.ForkDrop.Stop()
				p.ForkDrop = nil
			}
		}()
	}
	// main loop. Handle incoming messages.
	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Debug("Ethereum message handling failed", "err", err)
			return err
		}
	}
}

// handleMsg is invoked whenever an inbound message is received from a remote
// Peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *Peer) error {
	// Read the next message from the remote Peer, and ensure it's fully consumed
	msg, err := p.Rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return ErrResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return ErrResp(ErrExtraStatusMsg, "uncontrolled status message")

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		// Decode the complex header query
		var query GetBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return ErrResp(ErrDecode, "%v: %v", msg, err)
		}
		hashMode := query.Origin.Hash != (common.Hash{})

		// Gather headers until the fetch or network limits is reached
		var (
			bytes   common.StorageSize
			headers []*types.Header
			unknown bool
		)
		for !unknown && len(headers) < int(query.Amount) && bytes < SoftResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			var origin *types.Header
			if hashMode {
				origin = pm.Blockchain.GetHeaderByHash(query.Origin.Hash)
			} else {
				origin = pm.Blockchain.GetHeaderByNumber(query.Origin.Number)
			}
			if origin == nil {
				break
			}
			number := origin.Number.Uint64()
			headers = append(headers, origin)
			bytes += EstHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case query.Origin.Hash != (common.Hash{}) && query.Reverse:
				// Hash based traversal towards the genesis Block
				for i := 0; i < int(query.Skip)+1; i++ {
					if header := pm.Blockchain.GetHeader(query.Origin.Hash, number); header != nil {
						query.Origin.Hash = header.ParentHash
						number--
					} else {
						unknown = true
						break
					}
				}
			case query.Origin.Hash != (common.Hash{}) && !query.Reverse:
				// Hash based traversal towards the leaf Block
				var (
					current = origin.Number.Uint64()
					next    = current + query.Skip + 1
				)
				if next <= current {
					infos, _ := json.MarshalIndent(p.Peer.Info(), "", "  ")
					p.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
					unknown = true
				} else {
					if header := pm.Blockchain.GetHeaderByNumber(next); header != nil {
						if pm.Blockchain.GetBlockHashesFromHash(header.Hash(), query.Skip+1)[query.Skip] == query.Origin.Hash {
							query.Origin.Hash = header.Hash()
						} else {
							unknown = true
						}
					} else {
						unknown = true
					}
				}
			case query.Reverse:
				// Number based traversal towards the genesis Block
				if query.Origin.Number >= query.Skip+1 {
					query.Origin.Number -= query.Skip + 1
				} else {
					unknown = true
				}

			case !query.Reverse:
				// Number based traversal towards the leaf Block
				query.Origin.Number += query.Skip + 1
			}
		}
		return p.SendBlockHeaders(headers)

	case msg.Code == BlockHeadersMsg:
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			return ErrResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// If no headers were received, but we're expending a DAO fork check, maybe it's that
		if len(headers) == 0 && p.ForkDrop != nil {
			// Possibly an empty reply to the fork header checks, sanity check TDs
			verifyDAO := true

			// If we already have a DAO header, we can check the Peer's TD against it. If
			// the Peer's ahead of this, it too must have a reply to the DAO check
			if daoHeader := pm.Blockchain.GetHeaderByNumber(pm.Chainconfig.DAOForkBlock.Uint64()); daoHeader != nil {
				if _, td := p.GetHead(); td.Cmp(pm.Blockchain.GetTd(daoHeader.Hash(), daoHeader.Number.Uint64())) >= 0 {
					verifyDAO = false
				}
			}
			// If we're seemingly on the same chain, disable the drop timer
			if verifyDAO {
				p.Log().Debug("Seems to be on the same side of the DAO fork")
				p.ForkDrop.Stop()
				p.ForkDrop = nil
				return nil
			}
		}
		// Filter out any explicitly requested headers, deliver the rest to the Downloader
		filter := len(headers) == 1
		if filter {
			// If it's a potential DAO fork check, validate against the rules
			if p.ForkDrop != nil && pm.Chainconfig.DAOForkBlock.Cmp(headers[0].Number) == 0 {
				// Disable the fork drop timer
				p.ForkDrop.Stop()
				p.ForkDrop = nil

				// Validate the header and either drop the Peer or continue
				if err := misc.VerifyDAOHeaderExtraData(pm.Chainconfig, headers[0]); err != nil {
					p.Log().Debug("Verified to be on the other side of the DAO fork, dropping")
					return err
				}
				p.Log().Debug("Verified to be on the same side of the DAO fork")
				return nil
			}
			// Irrelevant of the fork checks, send the header to the Fetcher just in case
			headers = pm.Fetcher.FilterHeaders(p.Id, headers, time.Now())
		}
		if len(headers) > 0 || !filter {
			err := pm.Downloader.DeliverHeaders(p.Id, headers)
			if err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather blocks until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  int
			bodies []rlp.RawValue
		)
		for bytes < SoftResponseLimit && len(bodies) < downloader.MaxBlockFetch {
			// Retrieve the hash of the next Block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return ErrResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested Block body, stopping if enough was found
			if data := pm.Blockchain.GetBodyRLP(hash); len(data) != 0 {
				bodies = append(bodies, data)
				bytes += len(data)
			}
		}
		return p.SendBlockBodiesRLP(bodies)

	case msg.Code == BlockBodiesMsg:
		// A batch of Block bodies arrived to one of our previous requests
		var request BlockBodiesData
		if err := msg.Decode(&request); err != nil {
			return ErrResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver them all to the Downloader for queuing
		trasactions := make([][]*types.Transaction, len(request))
		uncles := make([][]*types.Header, len(request))

		for i, body := range request {
			trasactions[i] = body.Transactions
			uncles[i] = body.Uncles
		}
		// Filter out any explicitly requested bodies, deliver the rest to the Downloader
		filter := len(trasactions) > 0 || len(uncles) > 0
		if filter {
			trasactions, uncles = pm.Fetcher.FilterBodies(p.Id, trasactions, uncles, time.Now())
		}
		if len(trasactions) > 0 || len(uncles) > 0 || !filter {
			err := pm.Downloader.DeliverBodies(p.Id, trasactions, uncles)
			if err != nil {
				log.Debug("Failed to deliver bodies", "err", err)
			}
		}

	case p.Version >= Eth63 && msg.Code == GetNodeDataMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash  common.Hash
			bytes int
			data  [][]byte
		)
		for bytes < SoftResponseLimit && len(data) < downloader.MaxStateFetch {
			// Retrieve the hash of the next state entry
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return ErrResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested state entry, stopping if enough was found
			if entry, err := pm.Blockchain.TrieNode(hash); err == nil {
				data = append(data, entry)
				bytes += len(entry)
			}
		}
		return p.SendNodeData(data)

	case p.Version >= Eth63 && msg.Code == NodeDataMsg:
		// A batch of node state data arrived to one of our previous requests
		var data [][]byte
		if err := msg.Decode(&data); err != nil {
			return ErrResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the Downloader
		if err := pm.Downloader.DeliverNodeData(p.Id, data); err != nil {
			log.Debug("Failed to deliver node state data", "err", err)
		}

	case p.Version >= Eth63 && msg.Code == GetReceiptsMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash     common.Hash
			bytes    int
			receipts []rlp.RawValue
		)
		for bytes < SoftResponseLimit && len(receipts) < downloader.MaxReceiptFetch {
			// Retrieve the hash of the next Block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return ErrResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested Block's receipts, skipping if unknown to us
			results := pm.Blockchain.GetReceiptsByHash(hash)
			if results == nil {
				if header := pm.Blockchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
					continue
				}
			}
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(results); err != nil {
				log.Error("Failed to encode receipt", "err", err)
			} else {
				receipts = append(receipts, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendReceiptsRLP(receipts)

	case p.Version >= Eth63 && msg.Code == ReceiptsMsg:
		// A batch of receipts arrived to one of our previous requests
		var receipts [][]*types.Receipt
		if err := msg.Decode(&receipts); err != nil {
			return ErrResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the Downloader
		if err := pm.Downloader.DeliverReceipts(p.Id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

	case msg.Code == NewBlockHashesMsg:
		var announces NewBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return ErrResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(NewBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.Blockchain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}
		for _, block := range unknown {
			pm.Fetcher.AddNotify(p.Id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies)
		}

	case msg.Code == NewBlockMsg:
		// Retrieve and decode the propagated Block
		var request NewBlockData
		if err := msg.Decode(&request); err != nil {
			return ErrResp(ErrDecode, "%v: %v", msg, err)
		}
		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p

		// Mark the Peer as owning the Block and schedule it for import
		p.MarkBlock(request.Block.Hash())
		pm.Fetcher.Enqueue(p.Id, request.Block)

		// Assuming the Block is importable by the Peer, but possibly not yet done so,
		// calculate the GetHead hash and TD that the Peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
		)
		// Update the Peers total difficulty if better than the previous
		if _, td := p.GetHead(); trueTD.Cmp(td) > 0 {
			p.SetHead(trueHead, trueTD)

			// Schedule a sync if above ours. Note, this will not fire a sync for a gap of
			// a singe Block (as the true TD is below the propagated Block), however this
			// scenario should easily be covered by the Fetcher.
			currentBlock := pm.Blockchain.GetCurrentBlock()
			if trueTD.Cmp(pm.Blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())) > 0 {
				go pm.Synchronise(p)
			}
		}

	case msg.Code == TxMsg:
		// Transactions arrived, make sure we have a valid and fresh chain to Handle them
		if atomic.LoadUint32(&pm.AcceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return ErrResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return ErrResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		pm.Txpool.AddRemotes(txs)

	default:
		return ErrResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

// BroadcastBlock will either propagate a Block to a subset of it's Peers, or
// will only announce it's availability (depending what's requested).
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()
	peers := pm.Peers.PeersWithoutBlock(hash)

	// If propagation is requested, send to a subset of the Peer
	if propagate {
		// Calculate the TD of the Block (it's not imported yet, so Block.Td is not valid)
		var td *big.Int
		if parent := pm.Blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
			td = new(big.Int).Add(block.Difficulty(), pm.Blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
		} else {
			log.Error("Propagating dangling Block", "number", block.Number(), "hash", hash)
			return
		}
		// Send the Block to a subset of our Peers
		transfer := peers[:int(math.Sqrt(float64(len(peers))))]
		for _, peer := range transfer {
			peer.SendNewBlock(block, td)
		}
		log.Trace("Propagated Block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
		return
	}
	// Otherwise if the Block is indeed in out own chain, announce it
	if pm.Blockchain.HasBlock(hash, block.NumberU64()) {
		for _, peer := range peers {
			peer.SendNewBlockHashes([]common.Hash{hash}, []uint64{block.NumberU64()})
		}
		log.Trace("Announced Block", "hash", hash, "recipients", len(peers), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
	}
}

// BroadcastTx will propagate a transaction to all Peers which are not known to
// already have the given transaction.
func (pm *ProtocolManager) BroadcastTx(hash common.Hash, tx *types.Transaction) {
	// Broadcast transaction to a batch of Peers not knowing about it
	peers := pm.Peers.PeersWithoutTx(hash)
	//FIXME include this again: Peers = Peers[:int(math.Sqrt(float64(len(Peers))))]
	for _, peer := range peers {
		peer.SendTransactions(types.Transactions{tx})
	}
	log.Trace("Broadcast transaction", "hash", hash, "recipients", len(peers))
}

// Mined broadcast loop
func (self *ProtocolManager) minedBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range self.MinedBlockSub.Chan() {
		switch ev := obj.Data.(type) {
		case core.NewMinedBlockEvent:
			self.BroadcastBlock(ev.Block, true)  // First propagate Block to Peers
			self.BroadcastBlock(ev.Block, false) // Only then announce to the rest
		}
	}
}

func (self *ProtocolManager) txBroadcastLoop() {
	for {
		select {
		case event := <-self.TxCh:
			self.BroadcastTx(event.Tx.Hash(), event.Tx)

		// Err() channel will be Closed when unsubscribing.
		case <-self.TxSub.Err():
			return
		}
	}
}

// NodeInfo represents a short summary of the Ethereum sub-protocol metadata
// known about the host Peer.
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Frontier, 2=Morden, Ropsten=3, Rinkeby=4)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's Blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis Block
	Config     *params.ChainConfig `json:"Config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"GetHead"`       // SHA3 hash of the host's best owned Block
}

// NodeInfo retrieves some protocol metadata about the running host node.
func (self *ProtocolManager) NodeInfo() *NodeInfo {
	currentBlock := self.Blockchain.GetCurrentBlock()
	return &NodeInfo{
		Network:    self.NetworkId,
		Difficulty: self.Blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64()),
		Genesis:    self.Blockchain.Genesis().Hash(),
		Config:     self.Blockchain.Config(),
		Head:       currentBlock.Hash(),
	}
}
