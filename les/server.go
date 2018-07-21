// Copyright 2016 The go-ethereum Authors
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

// Package les implements the Light Ethereum Subprotocol.
package les

import (
	"sync"

	tomoETh "github.com/tomochain/tomochain/eth"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/les"
)

func NewLesServer(eth *tomoETh.TomoChain, config *eth.Config) (*les.LesServer, error) {
	quitSync := make(chan struct{})
	pm, err := les.NewProtocolManager(eth.BlockChain().Config(), false, les.ServerProtocolVersions, config.NetworkId, eth.GetEventMux(), eth.GetEngine(), les.NewPeerSet(), eth.BlockChain(), eth.GetTxPool(), eth.GetChainDb(), nil, nil, quitSync, new(sync.WaitGroup))
	if err != nil {
		return nil, err
	}

	lesTopics := make([]discv5.Topic, len(les.AdvertiseProtocolVersions))
	for i, pv := range les.AdvertiseProtocolVersions {
		lesTopics[i] = les.LesTopic(eth.BlockChain().Genesis().Hash(), pv)
	}

	srv := &les.LesServer{
		Config:           config,
		ProtocolManager:  pm,
		QuitSync:         quitSync,
		LesTopics:        lesTopics,
		ChtIndexer:       light.NewChtIndexer(eth.GetChainDb(), false),
		BloomTrieIndexer: light.NewBloomTrieIndexer(eth.GetChainDb(), false),
	}
	logger := log.New()

	chtV1SectionCount, _, _ := srv.ChtIndexer.Sections() // indexer still uses LES/1 4k section size for backwards server compatibility
	chtV2SectionCount := chtV1SectionCount / (light.CHTFrequencyClient / light.CHTFrequencyServer)
	if chtV2SectionCount != 0 {
		// convert to LES/2 section
		chtLastSection := chtV2SectionCount - 1
		// convert last LES/2 section index back to LES/1 index for chtIndexer.SectionHead
		chtLastSectionV1 := (chtLastSection+1)*(light.CHTFrequencyClient/light.CHTFrequencyServer) - 1
		chtSectionHead := srv.ChtIndexer.SectionHead(chtLastSectionV1)
		chtRoot := light.GetChtV2Root(pm.ChainDb, chtLastSection, chtSectionHead)
		logger.Info("Loaded CHT", "section", chtLastSection, "head", chtSectionHead, "root", chtRoot)
	}
	bloomTrieSectionCount, _, _ := srv.BloomTrieIndexer.Sections()
	if bloomTrieSectionCount != 0 {
		bloomTrieLastSection := bloomTrieSectionCount - 1
		bloomTrieSectionHead := srv.BloomTrieIndexer.SectionHead(bloomTrieLastSection)
		bloomTrieRoot := light.GetBloomTrieRoot(pm.ChainDb, bloomTrieLastSection, bloomTrieSectionHead)
		logger.Info("Loaded bloom trie", "section", bloomTrieLastSection, "head", bloomTrieSectionHead, "root", bloomTrieRoot)
	}

	srv.ChtIndexer.Start(eth.BlockChain())
	pm.Server = srv

	srv.DefParams = &flowcontrol.ServerParams{
		BufLimit:    300000000,
		MinRecharge: 50000,
	}
	srv.FcManager = flowcontrol.NewClientManager(uint64(config.LightServ), 10, 1000000000)
	srv.FcCostStats = les.NewCostStats(eth.GetChainDb())
	return srv, nil
}
