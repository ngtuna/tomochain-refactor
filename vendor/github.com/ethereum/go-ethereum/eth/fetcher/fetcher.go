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

// Package fetcher contains the Block announcement based synchronisation.
package fetcher

import (
	"errors"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"gopkg.in/karalabe/cookiejar.v2/collections/prque"
)

const (
	ArriveTimeout = 500 * time.Millisecond // Time allowance before an Announced Block is explicitly requested
	GatherSlack  = 100 * time.Millisecond  // Interval used to collate almost-expired Announces with fetches
	FetchTimeout = 5 * time.Second         // Maximum allotted Time to return an explicitly requested Block
	MaxUncleDist = 7                       // Maximum allowed backward distance from the chain head
	MaxQueueDist = 32                      // Maximum allowed distance from the chain head to Queue
	HashLimit    = 256                     // Maximum Number of unique blocks a Peer may have Announced
	BlockLimit   = 64                      // Maximum Number of unique blocks a Peer may have delivered
)

var (
	errTerminated = errors.New("terminated")
)

// BlockRetrievalFn is a callback type for retrieving a Block from the local chain.
type BlockRetrievalFn func(common.Hash) *types.Block

// headerRequesterFn is a callback type for sending a Header retrieval request.
type headerRequesterFn func(common.Hash) error

// bodyRequesterFn is a callback type for sending a body retrieval request.
type bodyRequesterFn func([]common.Hash) error

// HeaderVerifierFn is a callback type to verify a Block's Header for fast propagation.
type HeaderVerifierFn func(header *types.Header) error

// BlockBroadcasterFn is a callback type for broadcasting a Block to connected peers.
type BlockBroadcasterFn func(block *types.Block, propagate bool)

// ChainHeightFn is a callback type to retrieve the current chain height.
type ChainHeightFn func() uint64

// ChainInsertFn is a callback type to Insert a batch of blocks into the local chain.
type ChainInsertFn func(types.Blocks) (int, error)

// PeerDropFn is a callback type for dropping a Peer detected as malicious.
type PeerDropFn func(id string)

// Announce is the Hash notification of the availability of a new Block in the
// network.
type Announce struct {
	Hash   common.Hash   // Hash of the Block being Announced
	Number uint64        // Number of the Block being Announced (0 = unknown | old protocol)
	Header *types.Header // Header of the Block partially reassembled (new protocol)
	Time   time.Time     // Timestamp of the announcement

	Origin string // Identifier of the Peer originating the notification

	FetchHeader headerRequesterFn // Fetcher function to retrieve the Header of an Announced Block
	FetchBodies bodyRequesterFn   // Fetcher function to retrieve the body of an Announced Block
}

// HeaderFilterTask represents a batch of Headers needing fetcher filtering.
type HeaderFilterTask struct {
	Peer    string          // The source Peer of Block Headers
	Headers []*types.Header // Collection of Headers to filter
	Time    time.Time       // Arrival Time of the Headers
}

// HeaderFilterTask represents a batch of Block bodies (Transactions and Uncles)
// needing fetcher filtering.
type BodyFilterTask struct {
	Peer         string                 // The source Peer of Block bodies
	Transactions [][]*types.Transaction // Collection of Transactions per Block bodies
	Uncles       [][]*types.Header      // Collection of Uncles per Block bodies
	Time         time.Time              // Arrival Time of the blocks' contents
}

// Inject represents a schedules import operation.
type Inject struct {
	Origin string
	Block  *types.Block
}

// Fetcher is responsible for accumulating Block announcements from various peers
// and scheduling them for retrieval.
type Fetcher struct {
	// Various event channels
	Notify chan *Announce
	Inject chan *Inject

	BlockFilter  chan chan []*types.Block
	HeaderFilter chan chan *HeaderFilterTask
	BodyFilter   chan chan *BodyFilterTask

	Done chan common.Hash
	Quit chan struct{}

	// Announce states
	Announces  map[string]int              // Per Peer Announce counts to prevent memory exhaustion
	Announced  map[common.Hash][]*Announce // Announced blocks, scheduled for Fetching
	Fetching   map[common.Hash]*Announce   // Announced blocks, currently Fetching
	Fetched    map[common.Hash][]*Announce // Blocks with Headers Fetched, scheduled for body retrieval
	Completing map[common.Hash]*Announce   // Blocks with Headers, currently body-Completing

	// Block cache
	Queue  *prque.Prque            // Queue containing the import operations (Block Number sorted)
	Queues map[string]int          // Per Peer Block counts to prevent memory exhaustion
	Queued map[common.Hash]*Inject // Set of already Queued blocks (to dedup imports)

	// Callbacks
	GetBlock       BlockRetrievalFn   // Retrieves a Block from the local chain
	VerifyHeader   HeaderVerifierFn   // Checks if a Block's Headers have a valid proof of work
	BroadcastBlock BlockBroadcasterFn // Broadcasts a Block to connected peers
	ChainHeight    ChainHeightFn      // Retrieves the current chain's height
	InsertChain    ChainInsertFn      // Injects a batch of blocks into the chain
	DropPeer       PeerDropFn         // Drops a Peer for misbehaving

	// Testing hooks
	AnnounceChangeHook func(common.Hash, bool) // Method to call upon adding or deleting a Hash from the Announce list
	QueueChangeHook    func(common.Hash, bool) // Method to call upon adding or deleting a Block from the import Queue
	FetchingHook       func([]common.Hash)     // Method to call upon starting a Block (eth/61) or Header (eth/62) fetch
	CompletingHook     func([]common.Hash)     // Method to call upon starting a Block body fetch (eth/62)
	ImportedHook       func(*types.Block)      // Method to call upon successful Block import (both eth/61 and eth/62)
}

// New creates a Block fetcher to retrieve blocks based on Hash announcements.
func New(getBlock BlockRetrievalFn, verifyHeader HeaderVerifierFn, broadcastBlock BlockBroadcasterFn, chainHeight ChainHeightFn, insertChain ChainInsertFn, dropPeer PeerDropFn) *Fetcher {
	return &Fetcher{
		Notify:         make(chan *Announce),
		Inject:         make(chan *Inject),
		BlockFilter:    make(chan chan []*types.Block),
		HeaderFilter:   make(chan chan *HeaderFilterTask),
		BodyFilter:     make(chan chan *BodyFilterTask),
		Done:           make(chan common.Hash),
		Quit:           make(chan struct{}),
		Announces:      make(map[string]int),
		Announced:      make(map[common.Hash][]*Announce),
		Fetching:       make(map[common.Hash]*Announce),
		Fetched:        make(map[common.Hash][]*Announce),
		Completing:     make(map[common.Hash]*Announce),
		Queue:          prque.New(),
		Queues:         make(map[string]int),
		Queued:         make(map[common.Hash]*Inject),
		GetBlock:       getBlock,
		VerifyHeader:   verifyHeader,
		BroadcastBlock: broadcastBlock,
		ChainHeight:    chainHeight,
		InsertChain:    insertChain,
		DropPeer:       dropPeer,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// Hash notifications and Block fetches until termination requested.
func (f *Fetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *Fetcher) Stop() {
	close(f.Quit)
}

// AddNotify Announces the fetcher of the potential availability of a new Block in
// the network.
func (f *Fetcher) AddNotify(peer string, hash common.Hash, number uint64, time time.Time,
	headerFetcher headerRequesterFn, bodyFetcher bodyRequesterFn) error {
	block := &Announce{
		Hash:        hash,
		Number:      number,
		Time:        time,
		Origin:      peer,
		FetchHeader: headerFetcher,
		FetchBodies: bodyFetcher,
	}
	select {
	case f.Notify <- block:
		return nil
	case <-f.Quit:
		return errTerminated
	}
}

// EnqueueTest tries to fill gaps the the fetcher's future import Queue.
func (f *Fetcher) EnqueueTest(peer string, block *types.Block) error {
	op := &Inject{
		Origin: peer,
		Block:  block,
	}
	select {
	case f.Inject <- op:
		return nil
	case <-f.Quit:
		return errTerminated
	}
}

// FilterHeaders extracts all the Headers that were explicitly requested by the fetcher,
// returning those that should be handled differently.
func (f *Fetcher) FilterHeaders(peer string, headers []*types.Header, time time.Time) []*types.Header {
	log.Trace("Filtering Headers", "Peer", peer, "Headers", len(headers))

	// Send the filter channel to the fetcher
	filter := make(chan *HeaderFilterTask)

	select {
	case f.HeaderFilter <- filter:
	case <-f.Quit:
		return nil
	}
	// Request the filtering of the Header list
	select {
	case filter <- &HeaderFilterTask{Peer: peer, Headers: headers, Time: time}:
	case <-f.Quit:
		return nil
	}
	// Retrieve the Headers remaining after filtering
	select {
	case task := <-filter:
		return task.Headers
	case <-f.Quit:
		return nil
	}
}

// FilterBodies extracts all the Block bodies that were explicitly requested by
// the fetcher, returning those that should be handled differently.
func (f *Fetcher) FilterBodies(peer string, transactions [][]*types.Transaction, uncles [][]*types.Header, time time.Time) ([][]*types.Transaction, [][]*types.Header) {
	log.Trace("Filtering bodies", "Peer", peer, "txs", len(transactions), "Uncles", len(uncles))

	// Send the filter channel to the fetcher
	filter := make(chan *BodyFilterTask)

	select {
	case f.BodyFilter <- filter:
	case <-f.Quit:
		return nil, nil
	}
	// Request the filtering of the body list
	select {
	case filter <- &BodyFilterTask{Peer: peer, Transactions: transactions, Uncles: uncles, Time: time}:
	case <-f.Quit:
		return nil, nil
	}
	// Retrieve the bodies remaining after filtering
	select {
	case task := <-filter:
		return task.Transactions, task.Uncles
	case <-f.Quit:
		return nil, nil
	}
}

// Loop is the main fetcher loop, checking and processing various notification
// events.
func (f *Fetcher) loop() {
	// Iterate the Block Fetching until a Quit is requested
	fetchTimer := time.NewTimer(0)
	completeTimer := time.NewTimer(0)

	for {
		// Clean up any expired Block fetches
		for hash, announce := range f.Fetching {
			if time.Since(announce.Time) > FetchTimeout {
				f.ForgetHash(hash)
			}
		}
		// Import any Queued blocks that could potentially fit
		height := f.ChainHeight()
		for !f.Queue.Empty() {
			op := f.Queue.PopItem().(*Inject)
			if f.QueueChangeHook != nil {
				f.QueueChangeHook(op.Block.Hash(), false)
			}
			// If too high up the chain or phase, continue later
			number := op.Block.NumberU64()
			if number > height+1 {
				f.Queue.Push(op, -float32(op.Block.NumberU64()))
				if f.QueueChangeHook != nil {
					f.QueueChangeHook(op.Block.Hash(), true)
				}
				break
			}
			// Otherwise if fresh and still unknown, try and import
			hash := op.Block.Hash()
			if number+MaxUncleDist < height || f.GetBlock(hash) != nil {
				f.ForgetBlock(hash)
				continue
			}
			f.Insert(op.Origin, op.Block)
		}
		// Wait for an outside event to occur
		select {
		case <-f.Quit:
			// Fetcher terminating, abort all operations
			return

		case notification := <-f.Notify:
			// A Block was Announced, make sure the Peer isn't DOSing us
			PropAnnounceInMeter.Mark(1)

			count := f.Announces[notification.Origin] + 1
			if count > HashLimit {
				log.Debug("Peer exceeded outstanding Announces", "Peer", notification.Origin, "limit", HashLimit)
				PropAnnounceDOSMeter.Mark(1)
				break
			}
			// If we have a valid Block Number, check that it's potentially useful
			if notification.Number > 0 {
				if dist := int64(notification.Number) - int64(f.ChainHeight()); dist < -MaxUncleDist || dist > MaxQueueDist {
					log.Debug("Peer discarded announcement", "Peer", notification.Origin, "Number", notification.Number, "Hash", notification.Hash, "distance", dist)
					PropAnnounceDropMeter.Mark(1)
					break
				}
			}
			// All is well, schedule the Announce if Block's not yet downloading
			if _, ok := f.Fetching[notification.Hash]; ok {
				break
			}
			if _, ok := f.Completing[notification.Hash]; ok {
				break
			}
			f.Announces[notification.Origin] = count
			f.Announced[notification.Hash] = append(f.Announced[notification.Hash], notification)
			if f.AnnounceChangeHook != nil && len(f.Announced[notification.Hash]) == 1 {
				f.AnnounceChangeHook(notification.Hash, true)
			}
			if len(f.Announced) == 1 {
				f.RescheduleFetch(fetchTimer)
			}

		case op := <-f.Inject:
			// A direct Block insertion was requested, try and fill any pending gaps
			PropBroadcastInMeter.Mark(1)
			f.Enqueue(op.Origin, op.Block)

		case hash := <-f.Done:
			// A pending import finished, remove all traces of the notification
			f.ForgetHash(hash)
			f.ForgetBlock(hash)

		case <-fetchTimer.C:
			// At least one Block's timer ran out, check for needing retrieval
			request := make(map[string][]common.Hash)

			for hash, announces := range f.Announced {
				if time.Since(announces[0].Time) > ArriveTimeout-GatherSlack {
					// Pick a random Peer to retrieve from, reset all others
					announce := announces[rand.Intn(len(announces))]
					f.ForgetHash(hash)

					// If the Block still didn't arrive, Queue for Fetching
					if f.GetBlock(hash) == nil {
						request[announce.Origin] = append(request[announce.Origin], hash)
						f.Fetching[hash] = announce
					}
				}
			}
			// Send out all Block Header requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled Headers", "Peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				fetchHeader, hashes := f.Fetching[hashes[0]].FetchHeader, hashes
				go func() {
					if f.FetchingHook != nil {
						f.FetchingHook(hashes)
					}
					for _, hash := range hashes {
						HeaderFetchMeter.Mark(1)
						fetchHeader(hash) // Suboptimal, but protocol doesn't allow batch Header retrievals
					}
				}()
			}
			// Schedule the next fetch if blocks are still pending
			f.RescheduleFetch(fetchTimer)

		case <-completeTimer.C:
			// At least one Header's timer ran out, retrieve everything
			request := make(map[string][]common.Hash)

			for hash, announces := range f.Fetched {
				// Pick a random Peer to retrieve from, reset all others
				announce := announces[rand.Intn(len(announces))]
				f.ForgetHash(hash)

				// If the Block still didn't arrive, Queue for completion
				if f.GetBlock(hash) == nil {
					request[announce.Origin] = append(request[announce.Origin], hash)
					f.Completing[hash] = announce
				}
			}
			// Send out all Block body requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled bodies", "Peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				if f.CompletingHook != nil {
					f.CompletingHook(hashes)
				}
				BodyFetchMeter.Mark(int64(len(hashes)))
				go f.Completing[hashes[0]].FetchBodies(hashes)
			}
			// Schedule the next fetch if blocks are still pending
			f.RescheduleComplete(completeTimer)

		case filter := <-f.HeaderFilter:
			// Headers arrived from a remote Peer. Extract those that were explicitly
			// requested by the fetcher, and return everything else so it's delivered
			// to other parts of the system.
			var task *HeaderFilterTask
			select {
			case task = <-filter:
			case <-f.Quit:
				return
			}
			HeaderFilterInMeter.Mark(int64(len(task.Headers)))

			// Split the batch of Headers into unknown ones (to return to the caller),
			// known incomplete ones (requiring body retrievals) and completed blocks.
			unknown, incomplete, complete := []*types.Header{}, []*Announce{}, []*types.Block{}
			for _, header := range task.Headers {
				hash := header.Hash()

				// Filter fetcher-requested Headers from other synchronisation algorithms
				if announce := f.Fetching[hash]; announce != nil && announce.Origin == task.Peer && f.Fetched[hash] == nil && f.Completing[hash] == nil && f.Queued[hash] == nil {
					// If the delivered Header does not match the promised Number, drop the announcer
					if header.Number.Uint64() != announce.Number {
						log.Trace("Invalid Block Number Fetched", "Peer", announce.Origin, "Hash", header.Hash(), "Announced", announce.Number, "provided", header.Number)
						f.DropPeer(announce.Origin)
						f.ForgetHash(hash)
						continue
					}
					// Only keep if not imported by other means
					if f.GetBlock(hash) == nil {
						announce.Header = header
						announce.Time = task.Time

						// If the Block is empty (Header only), short circuit into the final import Queue
						if header.TxHash == types.DeriveSha(types.Transactions{}) && header.UncleHash == types.CalcUncleHash([]*types.Header{}) {
							log.Trace("Block empty, skipping body retrieval", "Peer", announce.Origin, "Number", header.Number, "Hash", header.Hash())

							block := types.NewBlockWithHeader(header)
							block.ReceivedAt = task.Time

							complete = append(complete, block)
							f.Completing[hash] = announce
							continue
						}
						// Otherwise add to the list of blocks needing completion
						incomplete = append(incomplete, announce)
					} else {
						log.Trace("Block already imported, discarding Header", "Peer", announce.Origin, "Number", header.Number, "Hash", header.Hash())
						f.ForgetHash(hash)
					}
				} else {
					// Fetcher doesn't know about it, add to the return list
					unknown = append(unknown, header)
				}
			}
			HeaderFilterOutMeter.Mark(int64(len(unknown)))
			select {
			case filter <- &HeaderFilterTask{Headers: unknown, Time: task.Time}:
			case <-f.Quit:
				return
			}
			// Schedule the retrieved Headers for body completion
			for _, announce := range incomplete {
				hash := announce.Header.Hash()
				if _, ok := f.Completing[hash]; ok {
					continue
				}
				f.Fetched[hash] = append(f.Fetched[hash], announce)
				if len(f.Fetched) == 1 {
					f.RescheduleComplete(completeTimer)
				}
			}
			// Schedule the Header-only blocks for import
			for _, block := range complete {
				if announce := f.Completing[block.Hash()]; announce != nil {
					f.Enqueue(announce.Origin, block)
				}
			}

		case filter := <-f.BodyFilter:
			// Block bodies arrived, extract any explicitly requested blocks, return the rest
			var task *BodyFilterTask
			select {
			case task = <-filter:
			case <-f.Quit:
				return
			}
			BodyFilterInMeter.Mark(int64(len(task.Transactions)))

			blocks := []*types.Block{}
			for i := 0; i < len(task.Transactions) && i < len(task.Uncles); i++ {
				// Match up a body to any possible completion request
				matched := false

				for hash, announce := range f.Completing {
					if f.Queued[hash] == nil {
						txnHash := types.DeriveSha(types.Transactions(task.Transactions[i]))
						uncleHash := types.CalcUncleHash(task.Uncles[i])

						if txnHash == announce.Header.TxHash && uncleHash == announce.Header.UncleHash && announce.Origin == task.Peer {
							// Mark the body matched, reassemble if still unknown
							matched = true

							if f.GetBlock(hash) == nil {
								block := types.NewBlockWithHeader(announce.Header).WithBody(task.Transactions[i], task.Uncles[i])
								block.ReceivedAt = task.Time

								blocks = append(blocks, block)
							} else {
								f.ForgetHash(hash)
							}
						}
					}
				}
				if matched {
					task.Transactions = append(task.Transactions[:i], task.Transactions[i+1:]...)
					task.Uncles = append(task.Uncles[:i], task.Uncles[i+1:]...)
					i--
					continue
				}
			}

			BodyFilterOutMeter.Mark(int64(len(task.Transactions)))
			select {
			case filter <- task:
			case <-f.Quit:
				return
			}
			// Schedule the retrieved blocks for ordered import
			for _, block := range blocks {
				if announce := f.Completing[block.Hash()]; announce != nil {
					f.Enqueue(announce.Origin, block)
				}
			}
		}
	}
}

// RescheduleFetch resets the specified fetch timer to the next Announce timeout.
func (f *Fetcher) RescheduleFetch(fetch *time.Timer) {
	// Short circuit if no blocks are Announced
	if len(f.Announced) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.Announced {
		if earliest.After(announces[0].Time) {
			earliest = announces[0].Time
		}
	}
	fetch.Reset(ArriveTimeout - time.Since(earliest))
}

// RescheduleComplete resets the specified completion timer to the next fetch timeout.
func (f *Fetcher) RescheduleComplete(complete *time.Timer) {
	// Short circuit if no Headers are Fetched
	if len(f.Fetched) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.Fetched {
		if earliest.After(announces[0].Time) {
			earliest = announces[0].Time
		}
	}
	complete.Reset(GatherSlack - time.Since(earliest))
}

// Enqueue schedules a new future import operation, if the Block to be imported
// has not yet been seen.
func (f *Fetcher) Enqueue(peer string, block *types.Block) {
	hash := block.Hash()

	// Ensure the Peer isn't DOSing us
	count := f.Queues[peer] + 1
	if count > BlockLimit {
		log.Debug("Discarded propagated Block, exceeded allowance", "Peer", peer, "Number", block.Number(), "Hash", hash, "limit", BlockLimit)
		PropBroadcastDOSMeter.Mark(1)
		f.ForgetHash(hash)
		return
	}
	// Discard any past or too distant blocks
	if dist := int64(block.NumberU64()) - int64(f.ChainHeight()); dist < -MaxUncleDist || dist > MaxQueueDist {
		log.Debug("Discarded propagated Block, too far away", "Peer", peer, "Number", block.Number(), "Hash", hash, "distance", dist)
		PropBroadcastDropMeter.Mark(1)
		f.ForgetHash(hash)
		return
	}
	// Schedule the Block for future importing
	if _, ok := f.Queued[hash]; !ok {
		op := &Inject{
			Origin: peer,
			Block:  block,
		}
		f.Queues[peer] = count
		f.Queued[hash] = op
		f.Queue.Push(op, -float32(block.NumberU64()))
		if f.QueueChangeHook != nil {
			f.QueueChangeHook(op.Block.Hash(), true)
		}
		log.Debug("Queued propagated Block", "Peer", peer, "Number", block.Number(), "Hash", hash, "Queued", f.Queue.Size())
	}
}

// Insert spawns a new goroutine to run a Block insertion into the chain. If the
// Block's Number is at the same height as the current import phase, it updates
// the phase states accordingly.
func (f *Fetcher) Insert(peer string, block *types.Block) {
	hash := block.Hash()

	// Run the import on a new thread
	log.Debug("Importing propagated Block", "Peer", peer, "Number", block.Number(), "Hash", hash)
	go func() {
		defer func() { f.Done <- hash }()

		// If the parent's unknown, abort insertion
		parent := f.GetBlock(block.ParentHash())
		if parent == nil {
			log.Debug("Unknown parent of propagated Block", "Peer", peer, "Number", block.Number(), "Hash", hash, "parent", block.ParentHash())
			return
		}
		// Quickly validate the Header and propagate the Block if it passes
		switch err := f.VerifyHeader(block.Header()); err {
		case nil:
			// All ok, quickly propagate to our peers
			PropBroadcastOutTimer.UpdateSince(block.ReceivedAt)
			go f.BroadcastBlock(block, true)

		case consensus.ErrFutureBlock:
			// Weird future Block, don't fail, but neither propagate

		default:
			// Something went very wrong, drop the Peer
			log.Debug("Propagated Block verification failed", "Peer", peer, "Number", block.Number(), "Hash", hash, "err", err)
			f.DropPeer(peer)
			return
		}
		// Run the actual import and log any issues
		if _, err := f.InsertChain(types.Blocks{block}); err != nil {
			log.Debug("Propagated Block import failed", "Peer", peer, "Number", block.Number(), "Hash", hash, "err", err)
			return
		}
		// If import succeeded, broadcast the Block
		PropAnnounceOutTimer.UpdateSince(block.ReceivedAt)
		go f.BroadcastBlock(block, false)

		// Invoke the testing hook if needed
		if f.ImportedHook != nil {
			f.ImportedHook(block)
		}
	}()
}

// ForgetHash removes all traces of a Block announcement from the fetcher's
// internal state.
func (f *Fetcher) ForgetHash(hash common.Hash) {
	// Remove all pending Announces and decrement DOS counters
	for _, announce := range f.Announced[hash] {
		f.Announces[announce.Origin]--
		if f.Announces[announce.Origin] == 0 {
			delete(f.Announces, announce.Origin)
		}
	}
	delete(f.Announced, hash)
	if f.AnnounceChangeHook != nil {
		f.AnnounceChangeHook(hash, false)
	}
	// Remove any pending fetches and decrement the DOS counters
	if announce := f.Fetching[hash]; announce != nil {
		f.Announces[announce.Origin]--
		if f.Announces[announce.Origin] == 0 {
			delete(f.Announces, announce.Origin)
		}
		delete(f.Fetching, hash)
	}

	// Remove any pending completion requests and decrement the DOS counters
	for _, announce := range f.Fetched[hash] {
		f.Announces[announce.Origin]--
		if f.Announces[announce.Origin] == 0 {
			delete(f.Announces, announce.Origin)
		}
	}
	delete(f.Fetched, hash)

	// Remove any pending completions and decrement the DOS counters
	if announce := f.Completing[hash]; announce != nil {
		f.Announces[announce.Origin]--
		if f.Announces[announce.Origin] == 0 {
			delete(f.Announces, announce.Origin)
		}
		delete(f.Completing, hash)
	}
}

// ForgetBlock removes all traces of a Queued Block from the fetcher's internal
// state.
func (f *Fetcher) ForgetBlock(hash common.Hash) {
	if insert := f.Queued[hash]; insert != nil {
		f.Queues[insert.Origin]--
		if f.Queues[insert.Origin] == 0 {
			delete(f.Queues, insert.Origin)
		}
		delete(f.Queued, hash)
	}
}
