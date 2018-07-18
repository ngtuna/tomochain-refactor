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

// Package fetcher contains the block announcement based synchronisation.
package fetcher

import (
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/eth/fetcher"
)

// Fetcher is responsible for accumulating block announcements from various peers
// and scheduling them for retrieval.
type TomoFetcher struct {
	fetcher.Fetcher
}

// Loop is the main fetcher loop, checking and processing various notification
// events.
func (f *TomoFetcher) loop() {
	// Iterate the block fetching until a quit is requested
	fetchTimer := time.NewTimer(0)
	completeTimer := time.NewTimer(0)

	for {
		// Clean up any expired block fetches
		for hash, announce := range f.Fetching {
			if time.Since(announce.Time) > fetcher.FetchTimeout {
				f.ForgetHash(hash)
			}
		}
		// Import any queued blocks that could potentially fit
		height := f.ChainHeight()
		for !f.Queue.Empty() {
			op := f.Queue.PopItem().(*fetcher.Inject)
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
			if number+fetcher.MaxUncleDist < height || f.GetBlock(hash) != nil {
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
			// A block was announced, make sure the peer isn't DOSing us
			fetcher.PropAnnounceInMeter.Mark(1)

			count := f.Announces[notification.Origin] + 1
			if count > fetcher.HashLimit {
				log.Debug("Peer exceeded outstanding announces", "peer", notification.Origin, "limit", fetcher.HashLimit)
				fetcher.PropAnnounceDOSMeter.Mark(1)
				break
			}
			// If we have a valid block number, check that it's potentially useful
			if notification.Number > 0 {
				if dist := int64(notification.Number) - int64(f.ChainHeight()); dist < -fetcher.MaxUncleDist || dist > fetcher.MaxQueueDist {
					log.Debug("Peer discarded announcement", "peer", notification.Origin, "number", notification.Number, "hash", notification.Hash, "distance", dist)
					fetcher.PropAnnounceDropMeter.Mark(1)
					break
				}
			}
			// All is well, schedule the announce if block's not yet downloading
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
			// A direct block insertion was requested, try and fill any pending gaps
			fetcher.PropBroadcastInMeter.Mark(1)
			f.Enqueue(op.Origin, op.Block)

		case hash := <-f.Done:
			// A pending import finished, remove all traces of the notification
			f.ForgetHash(hash)
			f.ForgetBlock(hash)

		case <-fetchTimer.C:
			// At least one block's timer ran out, check for needing retrieval
			request := make(map[string][]common.Hash)

			for hash, announces := range f.Announced {
				if time.Since(announces[0].Time) > fetcher.ArriveTimeout-fetcher.GatherSlack {
					// Pick a random peer to retrieve from, reset all others
					announce := announces[rand.Intn(len(announces))]
					f.ForgetHash(hash)

					// If the block still didn't arrive, queue for fetching
					if f.GetBlock(hash) == nil {
						request[announce.Origin] = append(request[announce.Origin], hash)
						f.Fetching[hash] = announce
					}
				}
			}
			// Send out all block header requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled headers", "peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				fetchHeader, hashes := f.Fetching[hashes[0]].FetchHeader, hashes
				go func() {
					if f.FetchingHook != nil {
						f.FetchingHook(hashes)
					}
					for _, hash := range hashes {
						fetcher.HeaderFetchMeter.Mark(1)
						fetchHeader(hash) // Suboptimal, but protocol doesn't allow batch header retrievals
					}
				}()
			}
			// Schedule the next fetch if blocks are still pending
			f.RescheduleFetch(fetchTimer)

		case <-completeTimer.C:
			// At least one header's timer ran out, retrieve everything
			request := make(map[string][]common.Hash)

			for hash, announces := range f.Fetched {
				// Pick a random peer to retrieve from, reset all others
				announce := announces[rand.Intn(len(announces))]
				f.ForgetHash(hash)

				// If the block still didn't arrive, queue for completion
				if f.GetBlock(hash) == nil {
					request[announce.Origin] = append(request[announce.Origin], hash)
					f.Completing[hash] = announce
				}
			}
			// Send out all block body requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled bodies", "peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				if f.CompletingHook != nil {
					f.CompletingHook(hashes)
				}
				fetcher.BodyFetchMeter.Mark(int64(len(hashes)))
				go f.Completing[hashes[0]].FetchBodies(hashes)
			}
			// Schedule the next fetch if blocks are still pending
			f.RescheduleComplete(completeTimer)

		case filter := <-f.HeaderFilter:
			// Headers arrived from a remote peer. Extract those that were explicitly
			// requested by the fetcher, and return everything else so it's delivered
			// to other parts of the system.
			var task *fetcher.HeaderFilterTask
			select {
			case task = <-filter:
			case <-f.Quit:
				return
			}
			fetcher.HeaderFilterInMeter.Mark(int64(len(task.Headers)))

			// Split the batch of headers into unknown ones (to return to the caller),
			// known incomplete ones (requiring body retrievals) and completed blocks.
			unknown, incomplete, complete := []*types.Header{}, []*fetcher.Announce{}, []*types.Block{}
			for _, header := range task.Headers {
				hash := header.Hash()

				// Filter fetcher-requested headers from other synchronisation algorithms
				if announce := f.Fetching[hash]; announce != nil && announce.Origin == task.Peer && f.Fetched[hash] == nil && f.Completing[hash] == nil && f.Queued[hash] == nil {
					// If the delivered header does not match the promised number, drop the announcer
					if header.Number.Uint64() != announce.Number {
						log.Trace("Invalid block number fetched", "peer", announce.Origin, "hash", header.Hash(), "announced", announce.Number, "provided", header.Number)
						f.DropPeer(announce.Origin)
						f.ForgetHash(hash)
						continue
					}
					// Only keep if not imported by other means
					if f.GetBlock(hash) == nil {
						announce.Header = header
						announce.Time = task.Time

						// If the block is empty (header only), short circuit into the final import queue
						if header.TxHash == types.DeriveSha(types.Transactions{}) && header.UncleHash == types.CalcUncleHash([]*types.Header{}) {
							log.Trace("Block empty, skipping body retrieval", "peer", announce.Origin, "number", header.Number, "hash", header.Hash())

							block := types.NewBlockWithHeader(header)
							block.ReceivedAt = task.Time

							complete = append(complete, block)
							f.Completing[hash] = announce
							continue
						}
						// Otherwise add to the list of blocks needing completion
						incomplete = append(incomplete, announce)
					} else {
						log.Trace("Block already imported, discarding header", "peer", announce.Origin, "number", header.Number, "hash", header.Hash())
						f.ForgetHash(hash)
					}
				} else {
					// Fetcher doesn't know about it, add to the return list
					unknown = append(unknown, header)
				}
			}
			fetcher.HeaderFilterOutMeter.Mark(int64(len(unknown)))
			select {
			case filter <- &fetcher.HeaderFilterTask{Headers: unknown, Time: task.Time}:
			case <-f.Quit:
				return
			}
			// Schedule the retrieved headers for body completion
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
			// Schedule the header-only blocks for import
			for _, block := range complete {
				if announce := f.Completing[block.Hash()]; announce != nil {
					f.Enqueue(announce.Origin, block)
				}
			}

		case filter := <-f.BodyFilter:
			// Block bodies arrived, extract any explicitly requested blocks, return the rest
			var task *fetcher.BodyFilterTask
			select {
			case task = <-filter:
			case <-f.Quit:
				return
			}
			fetcher.BodyFilterInMeter.Mark(int64(len(task.Transactions)))

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

			fetcher.BodyFilterOutMeter.Mark(int64(len(task.Transactions)))
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

// Bind import hook when block imported into chain.
func SetImportedHook(f *fetcher.Fetcher ,importedHook func(*types.Block)) {
	f.ImportedHook = importedHook
}
