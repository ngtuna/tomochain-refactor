// Copyright 2014 The go-ethereum Authors
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

// Package eth implements the Ethereum protocol.
package eth

import (
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/bloombits"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/eth/gasprice"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/miner"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

type LesServer interface {
	Start(srvr *p2p.Server)
	Stop()
	Protocols() []p2p.Protocol
	SetBloomBitsIndexer(bbIndexer *core.ChainIndexer)
}

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	Config      *Config
	ChainConfig *params.ChainConfig

	// Channel for shutting down the service
	ShutdownChan  chan bool    // Channel for shutting down the ethereum
	StopDbUpgrade func() error // stop chain db sequential key upgrade

	// Handlers
	TxPool          *core.TxPool
	Blockchain      *core.BlockChain
	ProtocolManager *ProtocolManager
	LesServer       LesServer

	// DB interfaces
	ChainDb ethdb.Database // Block chain database

	EventMux       *event.TypeMux
	Engine         consensus.Engine
	AccountManager *accounts.Manager

	BloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests
	BloomIndexer  *core.ChainIndexer             // Bloom indexer operating during block imports

	ApiBackend *EthApiBackend

	Miner     *miner.Miner
	GasPrice  *big.Int
	Etherbase common.Address

	NetworkId     uint64
	NetRPCService *ethapi.PublicNetAPI

	Lock sync.RWMutex // Protects the variadic fields (e.g. gas price and Etherbase)
}

func (s *Ethereum) AddLesServer(ls LesServer) {
	s.LesServer = ls
	ls.SetBloomBitsIndexer(s.BloomIndexer)
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(ctx *node.ServiceContext, config *Config) (*Ethereum, error) {
	if config.SyncMode == downloader.LightSync {
		return nil, errors.New("can't run eth.Ethereum in light sync mode, use les.LightEthereum")
	}
	if !config.SyncMode.IsValid() {
		return nil, fmt.Errorf("invalid sync mode %d", config.SyncMode)
	}
	chainDb, err := CreateDB(ctx, config, "chaindata")
	if err != nil {
		return nil, err
	}
	stopDbUpgrade := UpgradeDeduplicateData(chainDb)
	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlock(chainDb, config.Genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "Config", chainConfig)

	eth := &Ethereum{
		Config:         config,
		ChainDb:        chainDb,
		ChainConfig:    chainConfig,
		EventMux:       ctx.EventMux,
		AccountManager: ctx.AccountManager,
		Engine:         CreateConsensusEngine(ctx, &config.Ethash, chainConfig, chainDb),
		ShutdownChan:   make(chan bool),
		StopDbUpgrade:  stopDbUpgrade,
		NetworkId:      config.NetworkId,
		GasPrice:       config.GasPrice,
		Etherbase:      config.Etherbase,
		BloomRequests:  make(chan chan *bloombits.Retrieval),
		BloomIndexer:   NewBloomIndexer(chainDb, params.BloomBitsBlocks),
	}

	log.Info("Initialising Ethereum protocol", "versions", ProtocolVersions, "network", config.NetworkId)

	if !config.SkipBcVersionCheck {
		bcVersion := core.GetBlockChainVersion(chainDb)
		if bcVersion != core.BlockChainVersion && bcVersion != 0 {
			return nil, fmt.Errorf("Blockchain DB Version mismatch (%d / %d). Run geth upgradedb.\n", bcVersion, core.BlockChainVersion)
		}
		core.WriteBlockChainVersion(chainDb, core.BlockChainVersion)
	}
	var (
		vmConfig    = vm.Config{EnablePreimageRecording: config.EnablePreimageRecording}
		cacheConfig = &core.CacheConfig{Disabled: config.NoPruning, TrieNodeLimit: config.TrieCache, TrieTimeLimit: config.TrieTimeout}
	)
	eth.Blockchain, err = core.NewBlockChain(chainDb, cacheConfig, eth.ChainConfig, eth.Engine, vmConfig)
	if err != nil {
		return nil, err
	}
	// Rewind the chain in case of an incompatible Config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		eth.Blockchain.SetHead(compat.RewindTo)
		core.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}
	eth.BloomIndexer.Start(eth.Blockchain)

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = ctx.ResolvePath(config.TxPool.Journal)
	}
	eth.TxPool = core.NewTxPool(config.TxPool, eth.ChainConfig, eth.Blockchain)

	if eth.ProtocolManager, err = NewProtocolManager(eth.ChainConfig, config.SyncMode, config.NetworkId, eth.EventMux, eth.TxPool, eth.Engine, eth.Blockchain, chainDb); err != nil {
		return nil, err
	}
	eth.Miner = miner.New(eth, eth.ChainConfig, eth.GetEventMux(), eth.Engine)
	eth.Miner.SetExtra(MakeExtraData(config.ExtraData))

	eth.ApiBackend = &EthApiBackend{eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.GasPrice
	}
	eth.ApiBackend.gpo = gasprice.NewOracle(eth.ApiBackend, gpoParams)

	return eth, nil
}

func MakeExtraData(extra []byte) []byte {
	if len(extra) == 0 {
		// create default extradata
		extra, _ = rlp.EncodeToBytes([]interface{}{
			uint(params.VersionMajor<<16 | params.VersionMinor<<8 | params.VersionPatch),
			"geth",
			runtime.Version(),
			runtime.GOOS,
		})
	}
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		log.Warn("GetMiner extra data exceed limit", "extra", hexutil.Bytes(extra), "limit", params.MaximumExtraDataSize)
		extra = nil
	}
	return extra
}

// CreateDB creates the chain database.
func CreateDB(ctx *node.ServiceContext, config *Config, name string) (ethdb.Database, error) {
	db, err := ctx.OpenDatabase(name, config.DatabaseCache, config.DatabaseHandles)
	if err != nil {
		return nil, err
	}
	if db, ok := db.(*ethdb.LDBDatabase); ok {
		db.Meter("eth/db/chaindata/")
	}
	return db, nil
}

// CreateConsensusEngine creates the required type of consensus GetEngine instance for an Ethereum service
func CreateConsensusEngine(ctx *node.ServiceContext, config *ethash.Config, chainConfig *params.ChainConfig, db ethdb.Database) consensus.Engine {
	// If proof-of-authority is requested, set it up
	if chainConfig.Clique != nil {
		return clique.New(chainConfig.Clique, db)
	}
	// Otherwise assume proof-of-work
	switch {
	case config.PowMode == ethash.ModeFake:
		log.Warn("Ethash used in fake mode")
		return ethash.NewFaker()
	case config.PowMode == ethash.ModeTest:
		log.Warn("Ethash used in test mode")
		return ethash.NewTester()
	case config.PowMode == ethash.ModeShared:
		log.Warn("Ethash used in shared mode")
		return ethash.NewShared()
	default:
		engine := ethash.New(ethash.Config{
			CacheDir:       ctx.ResolvePath(config.CacheDir),
			CachesInMem:    config.CachesInMem,
			CachesOnDisk:   config.CachesOnDisk,
			DatasetDir:     config.DatasetDir,
			DatasetsInMem:  config.DatasetsInMem,
			DatasetsOnDisk: config.DatasetsOnDisk,
		})
		engine.SetThreads(-1) // Disable CPU mining
		return engine
	}
}

// APIs returns the collection of RPC services the ethereum package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
func (s *Ethereum) APIs() []rpc.API {
	apis := ethapi.GetAPIs(s.ApiBackend)

	// Append any APIs exposed explicitly by the consensus GetEngine
	apis = append(apis, s.Engine.APIs(s.BlockChain())...)

	// Append all the local APIs and return
	return append(apis, []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicEthereumAPI(s),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicMinerAPI(s),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   downloader.NewPublicDownloaderAPI(s.ProtocolManager.Downloader, s.EventMux),
			Public:    true,
		}, {
			Namespace: "Miner",
			Version:   "1.0",
			Service:   NewPrivateMinerAPI(s),
			Public:    false,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   filters.NewPublicFilterAPI(s.ApiBackend, false),
			Public:    true,
		}, {
			Namespace: "admin",
			Version:   "1.0",
			Service:   NewPrivateAdminAPI(s),
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPublicDebugAPI(s),
			Public:    true,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPrivateDebugAPI(s.ChainConfig, s),
		}, {
			Namespace: "net",
			Version:   "1.0",
			Service:   s.NetRPCService,
			Public:    true,
		},
	}...)
}

func (s *Ethereum) ResetWithGenesisBlock(gb *types.Block) {
	s.Blockchain.ResetWithGenesisBlock(gb)
}

func (s *Ethereum) GetEtherbase() (eb common.Address, err error) {
	s.Lock.RLock()
	etherbase := s.Etherbase
	s.Lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	if wallets := s.GetAccountManager().Wallets(); len(wallets) > 0 {
		if accounts := wallets[0].Accounts(); len(accounts) > 0 {
			etherbase := accounts[0].Address

			s.Lock.Lock()
			s.Etherbase = etherbase
			s.Lock.Unlock()

			log.Info("GetEtherbase automatically configured", "address", etherbase)
			return etherbase, nil
		}
	}
	return common.Address{}, fmt.Errorf("Etherbase must be explicitly specified")
}

// set in js console via admin interface or wrapper from cli flags
func (self *Ethereum) SetEtherbase(etherbase common.Address) {
	self.Lock.Lock()
	self.Etherbase = etherbase
	self.Lock.Unlock()

	self.Miner.SetEtherbase(etherbase)
}

func (s *Ethereum) StartMining(local bool) error {
	eb, err := s.GetEtherbase()
	if err != nil {
		log.Error("Cannot start mining without Etherbase", "err", err)
		return fmt.Errorf("Etherbase missing: %v", err)
	}
	if clique, ok := s.Engine.(*clique.Clique); ok {
		wallet, err := s.AccountManager.Find(accounts.Account{Address: eb})
		if wallet == nil || err != nil {
			log.Error("GetEtherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %v", err)
		}
		clique.Authorize(eb, wallet.SignHash)
	}
	if local {
		// If local (CPU) mining is started, we can disable the transaction rejection
		// mechanism introduced to speed sync times. CPU mining on mainnet is ludicrous
		// so noone will ever hit this path, whereas marking sync done on CPU mining
		// will ensure that private networks work in single Miner mode too.
		atomic.StoreUint32(&s.ProtocolManager.AcceptTxs, 1)
	}
	go s.Miner.Start(eb)
	return nil
}

func (s *Ethereum) StopMining()            { s.Miner.Stop() }
func (s *Ethereum) IsMining() bool         { return s.Miner.Mining() }
func (s *Ethereum) GetMiner() *miner.Miner { return s.Miner }

func (s *Ethereum) GetAccountManager() *accounts.Manager { return s.AccountManager }
func (s *Ethereum) BlockChain() *core.BlockChain         { return s.Blockchain }
func (s *Ethereum) GetTxPool() *core.TxPool              { return s.TxPool }
func (s *Ethereum) GetEventMux() *event.TypeMux          { return s.EventMux }
func (s *Ethereum) GetEngine() consensus.Engine          { return s.Engine }
func (s *Ethereum) GetChainDb() ethdb.Database           { return s.ChainDb }
func (s *Ethereum) IsListening() bool                    { return true } // Always listening
func (s *Ethereum) EthVersion() int                    { return int(s.ProtocolManager.SubProtocols[0].Version) }
func (s *Ethereum) NetVersion() uint64                 { return s.NetworkId }
func (s *Ethereum) Downloader() *downloader.Downloader { return s.ProtocolManager.Downloader }

// Protocols implements node.Service, returning all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	if s.LesServer == nil {
		return s.ProtocolManager.SubProtocols
	}
	return append(s.ProtocolManager.SubProtocols, s.LesServer.Protocols()...)
}

// Start implements node.Service, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start(srvr *p2p.Server) error {
	// Start the bloom bits servicing goroutines
	s.startBloomHandlers()

	// Start the RPC service
	s.NetRPCService = ethapi.NewPublicNetAPI(srvr, s.NetVersion())

	// Figure out a max Peers count based on the server limits
	maxPeers := srvr.MaxPeers
	if s.Config.LightServ > 0 {
		if s.Config.LightPeers >= srvr.MaxPeers {
			return fmt.Errorf("invalid Peer Config: light Peer count (%d) >= total Peer count (%d)", s.Config.LightPeers, srvr.MaxPeers)
		}
		maxPeers -= s.Config.LightPeers
	}
	// Start the networking layer and the light server if requested
	s.ProtocolManager.Start(maxPeers)
	if s.LesServer != nil {
		s.LesServer.Start(srvr)
	}
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	if s.StopDbUpgrade != nil {
		s.StopDbUpgrade()
	}
	s.BloomIndexer.Close()
	s.Blockchain.Stop()
	s.ProtocolManager.Stop()
	if s.LesServer != nil {
		s.LesServer.Stop()
	}
	s.TxPool.Stop()
	s.Miner.Stop()
	s.EventMux.Stop()

	s.ChainDb.Close()
	close(s.ShutdownChan)

	return nil
}
