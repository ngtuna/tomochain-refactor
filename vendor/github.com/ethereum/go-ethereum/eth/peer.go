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
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"gopkg.in/fatih/set.v0"
)

var (
	errClosed            = errors.New("Peer set is Closed")
	errAlreadyRegistered = errors.New("Peer is already registered")
	errNotRegistered     = errors.New("Peer is not registered")
)

const (
	maxKnownTxs      = 32768 // Maximum transactions hashes to keep in the known list (prevent DOS)
	maxKnownBlocks   = 1024  // Maximum Block hashes to keep in the known list (prevent DOS)
	handshakeTimeout = 5 * time.Second
)

// PeerInfo represents a short summary of the Ethereum sub-protocol metadata known
// about a connected Peer.
type PeerInfo struct {
	Version    int      `json:"Version"`    // Ethereum protocol Version negotiated
	Difficulty *big.Int `json:"difficulty"` // Total difficulty of the Peer's Blockchain
	Head       string   `json:"GetHead"`       // SHA3 hash of the Peer's best owned Block
}

type Peer struct {
	Id string

	*p2p.Peer
	Rw p2p.MsgReadWriter

	Version  int         // Protocol Version negotiated
	ForkDrop *time.Timer // Timed connection dropper if forks aren't validated in time

	Head common.Hash
	Td   *big.Int
	Lock sync.RWMutex

	KnownTxs    *set.Set // Set of transaction hashes known to be known by this Peer
	KnownBlocks *set.Set // Set of Block hashes known to be known by this Peer
}

func NewPeer(version int, p *p2p.Peer, rw p2p.MsgReadWriter) *Peer {
	id := p.ID()

	return &Peer{
		Peer:        p,
		Rw:          rw,
		Version:     version,
		Id:          fmt.Sprintf("%x", id[:8]),
		KnownTxs:    set.New(),
		KnownBlocks: set.New(),
	}
}

// Info gathers and returns a collection of metadata known about a Peer.
func (p *Peer) Info() *PeerInfo {
	hash, td := p.GetHead()

	return &PeerInfo{
		Version:    p.Version,
		Difficulty: td,
		Head:       hash.Hex(),
	}
}

// GetHead retrieves a copy of the current GetHead hash and total difficulty of the
// Peer.
func (p *Peer) GetHead() (hash common.Hash, td *big.Int) {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	copy(hash[:], p.Head[:])
	return hash, new(big.Int).Set(p.Td)
}

// SetHead updates the GetHead hash and total difficulty of the Peer.
func (p *Peer) SetHead(hash common.Hash, td *big.Int) {
	p.Lock.Lock()
	defer p.Lock.Unlock()

	copy(p.Head[:], hash[:])
	p.Td.Set(td)
}

// MarkBlock marks a Block as known for the Peer, ensuring that the Block will
// never be propagated to this particular Peer.
func (p *Peer) MarkBlock(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known Block hash
	for p.KnownBlocks.Size() >= maxKnownBlocks {
		p.KnownBlocks.Pop()
	}
	p.KnownBlocks.Add(hash)
}

// MarkTransaction marks a transaction as known for the Peer, ensuring that it
// will never be propagated to this particular Peer.
func (p *Peer) MarkTransaction(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known transaction hash
	for p.KnownTxs.Size() >= maxKnownTxs {
		p.KnownTxs.Pop()
	}
	p.KnownTxs.Add(hash)
}

// SendTransactions sends transactions to the Peer and includes the hashes
// in its transaction hash set for future reference.
func (p *Peer) SendTransactions(txs types.Transactions) error {
	for _, tx := range txs {
		p.KnownTxs.Add(tx.Hash())
	}
	return p2p.Send(p.Rw, TxMsg, txs)
}

// SendNewBlockHashes announces the availability of a number of blocks through
// a hash notification.
func (p *Peer) SendNewBlockHashes(hashes []common.Hash, numbers []uint64) error {
	for _, hash := range hashes {
		p.KnownBlocks.Add(hash)
	}
	request := make(NewBlockHashesData, len(hashes))
	for i := 0; i < len(hashes); i++ {
		request[i].Hash = hashes[i]
		request[i].Number = numbers[i]
	}
	return p2p.Send(p.Rw, NewBlockHashesMsg, request)
}

// SendNewBlock propagates an entire Block to a remote Peer.
func (p *Peer) SendNewBlock(block *types.Block, td *big.Int) error {
	p.KnownBlocks.Add(block.Hash())
	return p2p.Send(p.Rw, NewBlockMsg, []interface{}{block, td})
}

// SendBlockHeaders sends a batch of Block headers to the remote Peer.
func (p *Peer) SendBlockHeaders(headers []*types.Header) error {
	return p2p.Send(p.Rw, BlockHeadersMsg, headers)
}

// SendBlockBodies sends a batch of Block contents to the remote Peer.
func (p *Peer) SendBlockBodies(bodies []*blockBody) error {
	return p2p.Send(p.Rw, BlockBodiesMsg, BlockBodiesData(bodies))
}

// SendBlockBodiesRLP sends a batch of Block contents to the remote Peer from
// an already RLP encoded format.
func (p *Peer) SendBlockBodiesRLP(bodies []rlp.RawValue) error {
	return p2p.Send(p.Rw, BlockBodiesMsg, bodies)
}

// SendNodeDataRLP sends a batch of arbitrary internal data, corresponding to the
// hashes requested.
func (p *Peer) SendNodeData(data [][]byte) error {
	return p2p.Send(p.Rw, NodeDataMsg, data)
}

// SendReceiptsRLP sends a batch of transaction receipts, corresponding to the
// ones requested from an already RLP encoded format.
func (p *Peer) SendReceiptsRLP(receipts []rlp.RawValue) error {
	return p2p.Send(p.Rw, ReceiptsMsg, receipts)
}

// RequestOneHeader is a wrapper around the header query functions to fetch a
// single header. It is used solely by the Fetcher.
func (p *Peer) RequestOneHeader(hash common.Hash) error {
	p.Log().Debug("Fetching single header", "hash", hash)
	return p2p.Send(p.Rw, GetBlockHeadersMsg, &GetBlockHeadersData{Origin: hashOrNumber{Hash: hash}, Amount: uint64(1), Skip: uint64(0), Reverse: false})
}

// RequestHeadersByHash fetches a batch of blocks' headers corresponding to the
// specified header query, based on the hash of an origin Block.
func (p *Peer) RequestHeadersByHash(origin common.Hash, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromhash", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.Rw, GetBlockHeadersMsg, &GetBlockHeadersData{Origin: hashOrNumber{Hash: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// RequestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin Block.
func (p *Peer) RequestHeadersByNumber(origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.Rw, GetBlockHeadersMsg, &GetBlockHeadersData{Origin: hashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// RequestBodies fetches a batch of blocks' bodies corresponding to the hashes
// specified.
func (p *Peer) RequestBodies(hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of Block bodies", "count", len(hashes))
	return p2p.Send(p.Rw, GetBlockBodiesMsg, hashes)
}

// RequestNodeData fetches a batch of arbitrary data from a node's known state
// data, corresponding to the specified hashes.
func (p *Peer) RequestNodeData(hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of state data", "count", len(hashes))
	return p2p.Send(p.Rw, GetNodeDataMsg, hashes)
}

// RequestReceipts fetches a batch of transaction receipts from a remote node.
func (p *Peer) RequestReceipts(hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of receipts", "count", len(hashes))
	return p2p.Send(p.Rw, GetReceiptsMsg, hashes)
}

// Handshake executes the eth protocol handshake, negotiating Version number,
// network IDs, difficulties, GetHead and genesis blocks.
func (p *Peer) Handshake(network uint64, td *big.Int, head common.Hash, genesis common.Hash) error {
	// Send out own handshake in a new thread
	errc := make(chan error, 2)
	var status statusData // safe to read after two values have been received from errc

	go func() {
		errc <- p2p.Send(p.Rw, StatusMsg, &statusData{
			ProtocolVersion: uint32(p.Version),
			NetworkId:       network,
			TD:              td,
			CurrentBlock:    head,
			GenesisBlock:    genesis,
		})
	}()
	go func() {
		errc <- p.readStatus(network, &status, genesis)
	}()
	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
		case <-timeout.C:
			return p2p.DiscReadTimeout
		}
	}
	p.Td, p.Head = status.TD, status.CurrentBlock
	return nil
}

func (p *Peer) readStatus(network uint64, status *statusData, genesis common.Hash) (err error) {
	msg, err := p.Rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Code != StatusMsg {
		return ErrResp(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, StatusMsg)
	}
	if msg.Size > ProtocolMaxMsgSize {
		return ErrResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	if err := msg.Decode(&status); err != nil {
		return ErrResp(ErrDecode, "msg %v: %v", msg, err)
	}
	if status.GenesisBlock != genesis {
		return ErrResp(ErrGenesisBlockMismatch, "%x (!= %x)", status.GenesisBlock[:8], genesis[:8])
	}
	if status.NetworkId != network {
		return ErrResp(ErrNetworkIdMismatch, "%d (!= %d)", status.NetworkId, network)
	}
	if int(status.ProtocolVersion) != p.Version {
		return ErrResp(ErrProtocolVersionMismatch, "%d (!= %d)", status.ProtocolVersion, p.Version)
	}
	return nil
}

// String implements fmt.Stringer.
func (p *Peer) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.Id,
		fmt.Sprintf("eth/%2d", p.Version),
	)
}

// PeerSet represents the collection of active Peers currently participating in
// the Ethereum sub-protocol.
type PeerSet struct {
	Peers  map[string]*Peer
	Lock   sync.RWMutex
	Closed bool
}

// NewPeerSet creates a new Peer set to track the active participants.
func NewPeerSet() *PeerSet {
	return &PeerSet{
		Peers: make(map[string]*Peer),
	}
}

// Register injects a new Peer into the working set, or returns an error if the
// Peer is already known.
func (ps *PeerSet) Register(p *Peer) error {
	ps.Lock.Lock()
	defer ps.Lock.Unlock()

	if ps.Closed {
		return errClosed
	}
	if _, ok := ps.Peers[p.Id]; ok {
		return errAlreadyRegistered
	}
	ps.Peers[p.Id] = p
	return nil
}

// Unregister removes a remote Peer from the active set, disabling any further
// actions to/from that particular entity.
func (ps *PeerSet) Unregister(id string) error {
	ps.Lock.Lock()
	defer ps.Lock.Unlock()

	if _, ok := ps.Peers[id]; !ok {
		return errNotRegistered
	}
	delete(ps.Peers, id)
	return nil
}

// Peer retrieves the registered Peer with the given Id.
func (ps *PeerSet) Peer(id string) *Peer {
	ps.Lock.RLock()
	defer ps.Lock.RUnlock()

	return ps.Peers[id]
}

// Len returns if the current number of Peers in the set.
func (ps *PeerSet) Len() int {
	ps.Lock.RLock()
	defer ps.Lock.RUnlock()

	return len(ps.Peers)
}

// PeersWithoutBlock retrieves a list of Peers that do not have a given Block in
// their set of known hashes.
func (ps *PeerSet) PeersWithoutBlock(hash common.Hash) []*Peer {
	ps.Lock.RLock()
	defer ps.Lock.RUnlock()

	list := make([]*Peer, 0, len(ps.Peers))
	for _, p := range ps.Peers {
		if !p.KnownBlocks.Has(hash) {
			list = append(list, p)
		}
	}
	return list
}

// PeersWithoutTx retrieves a list of Peers that do not have a given transaction
// in their set of known hashes.
func (ps *PeerSet) PeersWithoutTx(hash common.Hash) []*Peer {
	ps.Lock.RLock()
	defer ps.Lock.RUnlock()

	list := make([]*Peer, 0, len(ps.Peers))
	for _, p := range ps.Peers {
		if !p.KnownTxs.Has(hash) {
			list = append(list, p)
		}
	}
	return list
}

// BestPeer retrieves the known Peer with the currently highest total difficulty.
func (ps *PeerSet) BestPeer() *Peer {
	ps.Lock.RLock()
	defer ps.Lock.RUnlock()

	var (
		bestPeer *Peer
		bestTd   *big.Int
	)
	for _, p := range ps.Peers {
		if _, td := p.GetHead(); bestPeer == nil || td.Cmp(bestTd) > 0 {
			bestPeer, bestTd = p, td
		}
	}
	return bestPeer
}

// Close disconnects all Peers.
// No new Peers can be registered after Close has returned.
func (ps *PeerSet) Close() {
	ps.Lock.Lock()
	defer ps.Lock.Unlock()

	for _, p := range ps.Peers {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.Closed = true
}
