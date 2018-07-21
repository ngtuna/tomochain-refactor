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

// Package eth implements the TomoChain protocol.
package eth

import (
	"errors"
	"fmt"
	"math/big"
	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/bloombits"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/gasprice"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/tomochain/tomochain/eth/fetcher"

	"github.com/tomochain/tomochain/contracts"
	"github.com/tomochain/tomochain/consensus/clique"
	"github.com/tomochain/tomochain/consensus"
	"sync/atomic"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/tomochain/tomochain/ethapi"
	"sync"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"

	"github.com/tomochain/tomochain/miner"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/core/state"
	tomocommon "github.com/tomochain/tomochain/common"
	"github.com/tomochain/tomochain/configs"
	"github.com/ethereum/go-ethereum/eth/filters"
	tomoCore "github.com/tomochain/tomochain/core"
	"github.com/ethereum/go-ethereum/consensus/ethash"
)

type TomoChain struct {
	Config      *eth.Config
	ChainConfig *params.ChainConfig

	// Channel for shutting down the service
	ShutdownChan chan bool // Channel for shutting down the TomoChain

	// Handlers
	TxPool          *core.TxPool
	Blockchain      *tomoCore.TomoBlockChain
	ProtocolManager *TomoProtocolManager
	LesServer       eth.LesServer

	// DB interfaces
	ChainDb ethdb.Database // Block chain database

	EventMux       *event.TypeMux
	Engine         consensus.Engine
	AccountManager *accounts.Manager

	BloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests
	BloomIndexer  *core.ChainIndexer             // Bloom indexer operating during block imports

	APIBackend *EthAPIBackend

	Miner     *miner.Miner
	GasPrice  *big.Int
	Etherbase common.Address

	NetworkID     uint64
	NetRPCService *ethapi.PublicNetAPI

	Lock sync.RWMutex // Protects the variadic fields (E.g. gas price and GetEtherbase)

	stopDbUpgrade func() error // stop chain db sequential key upgrade
	IPCEndpoint   string
	Client        *ethclient.Client // Global ipc client instance.
}

func (s *TomoChain) StopMining()                          { s.Miner.Stop() }
func (s *TomoChain) GetAccountManager() *accounts.Manager { return s.AccountManager }
func (s *TomoChain) BlockChain() *tomoCore.TomoBlockChain { return s.Blockchain }
func (s *TomoChain) GetTxPool() *core.TxPool              { return s.TxPool }
func (s *TomoChain) GetEventMux() *event.TypeMux          { return s.EventMux }
func (s *TomoChain) GetEngine() consensus.Engine          { return s.Engine }
func (s *TomoChain) GetChainDb() ethdb.Database           { return s.ChainDb }
func (s *TomoChain) IsListening() bool                    { return true } // Always listening
func (s *TomoChain) EthVersion() int                      { return int(s.ProtocolManager.SubProtocols[0].Version) }
func (s *TomoChain) NetVersion() uint64                   { return s.NetworkID }
func (s *TomoChain) Downloader() *downloader.Downloader   { return s.ProtocolManager.Downloader }
func (s *TomoChain) GetEtherbase() (eb common.Address, err error) {
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

			log.Info("Etherbase automatically configured", "address", etherbase)
			return etherbase, nil
		}
	}
	return common.Address{}, fmt.Errorf("etherbase must be explicitly specified")
}

// New creates a new TomoChain object (including the
// initialisation of the common TomoChain object)
func New(ctx *node.ServiceContext, config *eth.Config) (*TomoChain, error) {
	if config.SyncMode == downloader.LightSync {
		return nil, errors.New("can't run tomoChain.TomoChain in light sync mode, use les.LightEthereum")
	}
	if !config.SyncMode.IsValid() {
		return nil, fmt.Errorf("invalid sync mode %d", config.SyncMode)
	}
	chainDb, err := eth.CreateDB(ctx, config, "chaindata")
	if err != nil {
		return nil, err
	}
	stopDbUpgrade := eth.UpgradeDeduplicateData(chainDb)
	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlock(chainDb, config.Genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)
	engine := CreateConsensusEngine(ctx, &config.Ethash, chainConfig, chainDb)
	tomoChain := &TomoChain{
		Config:         config,
		ChainDb:        chainDb,
		ChainConfig:    chainConfig,
		EventMux:       ctx.EventMux,
		AccountManager: ctx.AccountManager,
		Engine:         engine,
		ShutdownChan:   make(chan bool),
		NetworkID:      config.NetworkId,
		GasPrice:       config.GasPrice,
		Etherbase:      config.Etherbase,
		BloomRequests:  make(chan chan *bloombits.Retrieval),
		BloomIndexer:   eth.NewBloomIndexer(chainDb, params.BloomBitsBlocks),

		stopDbUpgrade: stopDbUpgrade,
	}

	log.Info("Initialising TomoChain protocol", "versions", eth.ProtocolVersions, "network", config.NetworkId)

	if !config.SkipBcVersionCheck {
		bcVersion := core.GetBlockChainVersion(chainDb)
		if bcVersion != core.BlockChainVersion && bcVersion != 0 {
			return nil, fmt.Errorf("Blockchain DB version mismatch (%d / %d). Run geth upgradedb.\n", bcVersion, core.BlockChainVersion)
		}
		core.WriteBlockChainVersion(chainDb, core.BlockChainVersion)
	}
	var (
		vmConfig    = vm.Config{EnablePreimageRecording: config.EnablePreimageRecording}
		cacheConfig = &core.CacheConfig{Disabled: config.NoPruning, TrieNodeLimit: config.TrieCache, TrieTimeLimit: config.TrieTimeout}
	)
	tomoChain.Blockchain, err = tomoCore.NewBlockChain(chainDb, cacheConfig, tomoChain.ChainConfig, tomoChain.Engine.(consensus.Engine), vmConfig)
	if err != nil {
		return nil, err
	}
	// Rewind the chain in case of an incompatible config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		tomoChain.Blockchain.SetHead(compat.RewindTo)
		core.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}
	tomoChain.BloomIndexer.Start(tomoChain.Blockchain)

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = ctx.ResolvePath(config.TxPool.Journal)
	}
	tomoChain.TxPool = core.NewTxPool(config.TxPool, tomoChain.ChainConfig, tomoChain.Blockchain)

	if tomoChain.ProtocolManager, err = NewProtocolManager(tomoChain.ChainConfig, config.SyncMode, config.NetworkId, tomoChain.EventMux, tomoChain.TxPool, tomoChain.Engine, tomoChain.Blockchain, chainDb); err != nil {
		return nil, err
	}
	tomoChain.Miner = miner.New(tomoChain, tomoChain.ChainConfig, tomoChain.GetEventMux(), tomoChain.Engine.(consensus.Engine))
	tomoChain.Miner.SetExtra(eth.MakeExtraData(config.ExtraData))

	tomoChain.APIBackend = &EthAPIBackend{tomoChain, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.GasPrice
	}
	tomoChain.APIBackend.Gpo = gasprice.NewOracle(tomoChain.APIBackend, gpoParams)

	if tomoChain.ChainConfig.Clique != nil {
		c := tomoChain.Engine.(*clique.Clique)

		// Set global ipc endpoint.
		tomoChain.IPCEndpoint = ctx.Config.IPCEndpoint()

		// Inject hook for send tx sign to smartcontract after insert block into chain.
		importedHook := func(block *types.Block) {
			snap, err := c.GetSnapshot(tomoChain.Blockchain, block.Header())
			if err != nil {
				log.Error("Fail to get snapshot for sign tx validator.")
				return
			}
			if _, authorized := snap.Signers[tomoChain.Etherbase]; authorized {
				if err := contracts.CreateTransactionSign(chainConfig, tomoChain.TxPool, tomoChain.AccountManager, block); err != nil {
					log.Error("Fail to create tx sign for imported block", "error", err)
					return
				}
			}
		}
		fetcher.SetImportedHook(tomoChain.ProtocolManager.Fetcher, importedHook)

		//Hook reward for clique validator.
		c.HookReward = func(chain consensus.ChainReader, state *state.StateDB, header *types.Header) error {
			client, err := tomoChain.GetClient()
			if err != nil {
				log.Error("Fail to connect IPC client for blockSigner", "error", err)

				return err
			}

			number := header.Number.Uint64()
			rCheckpoint := configs.Config.RewardConfig.RewardCheckpoint
			if number > 0 && number-rCheckpoint > 0 {
				// Get signers in blockSigner smartcontract.
				addr := common.HexToAddress(tomocommon.BlockSigners)
				chainReward := new(big.Int).SetUint64(configs.Config.RewardConfig.Reward * params.Ether)
				totalSigner := new(uint64)
				signers, err := contracts.GetRewardForCheckpoint(addr, number, rCheckpoint, client, totalSigner)
				if err != nil {
					log.Error("Fail to get signers for reward checkpoint", "error", err)
				}
				rewardSigners, err := contracts.CalculateReward(chainReward, signers, *totalSigner)
				if err != nil {
					log.Error("Fail to calculate reward for signers", "error", err)
				}
				// Add reward for signers.
				if len(signers) > 0 {
					for signer, calcReward := range rewardSigners {
						state.AddBalance(signer, calcReward)
					}
				}
			}

			return nil
		}
	}

	return tomoChain, nil
}

// ValidateMiner checks if node's address is in set of validators
func (s *TomoChain) ValidateStaker() (bool, error) {
	eb, err := s.GetEtherbase()
	if err != nil {
		return false, err
	}
	if s.ChainConfig.Clique != nil {
		//check if miner's wallet is in set of validators
		c := s.Engine.(*clique.Clique)
		snap, err := c.GetSnapshot(s.Blockchain, s.Blockchain.CurrentHeader())
		if err != nil {
			return false, fmt.Errorf("Can't verify miner: %v", err)
		}
		if _, authorized := snap.Signers[eb]; !authorized {
			//This miner doesn't belong to set of validators
			return false, nil
		}
	} else {
		return false, fmt.Errorf("Only verify miners in Clique protocol")
	}
	return true, nil
}

func (s *TomoChain) StartStaking(local bool) error {
	eb, err := s.GetEtherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %v", err)
	}
	if clique, ok := s.Engine.(*clique.Clique); ok {
		wallet, err := s.AccountManager.Find(accounts.Account{Address: eb})
		if wallet == nil || err != nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %v", err)
		}
		clique.Authorize(eb, wallet.SignHash)
	}
	if local {
		// If local (CPU) mining is started, we can disable the transaction rejection
		// mechanism introduced to speed sync times. CPU mining on mainnet is ludicrous
		// so noone will ever hit this path, whereas marking sync done on CPU mining
		// will ensure that private networks work in single miner mode too.
		atomic.StoreUint32(&s.ProtocolManager.AcceptTxs, 1)
	}
	go s.Miner.Start(eb)
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// TomoChain protocol.
func (s *TomoChain) Stop() error {
	if s.stopDbUpgrade != nil {
		s.stopDbUpgrade()
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

// Get current IPC Client.
func (s *TomoChain) GetClient() (*ethclient.Client, error) {
	if s.Client == nil {
		// Inject ipc client global instance.
		client, err := ethclient.Dial(s.IPCEndpoint)
		if err != nil {
			log.Error("Fail to connect RPC", "error", err)
			return nil, err
		}
		s.Client = client
	}
	return s.Client, nil
}

func (s *TomoChain) AddLesServer(ls eth.LesServer) {
	s.LesServer = ls
	ls.SetBloomBitsIndexer(s.BloomIndexer)
}

func (s *TomoChain) Protocols() []p2p.Protocol {
	if s.LesServer == nil {
		return s.ProtocolManager.SubProtocols
	}
	return append(s.ProtocolManager.SubProtocols, s.LesServer.Protocols()...)
}

// APIs returns the collection of RPC services the ethereum package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
func (s *TomoChain) APIs() []rpc.API {
	apis := ethapi.GetAPIs(s.APIBackend)

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
			Service:   filters.NewPublicFilterAPI(s.APIBackend, false),
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
		},
		{
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPrivateDebugAPI(s.ChainConfig, s),
		},
		{
			Namespace: "net",
			Version:   "1.0",
			Service:   s.NetRPCService,
			Public:    true,
		},
	}...)
}

func (s *TomoChain) IsMining() bool         { return s.Miner.Mining() }
func (s *TomoChain) GetMiner() *miner.Miner { return s.Miner }

func (s *TomoChain) StartMining(local bool) error {
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

// set in js console via admin interface or wrapper from cli flags
func (self *TomoChain) SetEtherbase(etherbase common.Address) {
	self.Lock.Lock()
	self.Etherbase = etherbase
	self.Lock.Unlock()

	self.Miner.SetEtherbase(etherbase)
}

// Start implements node.Service, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *TomoChain) Start(srvr *p2p.Server) error {
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

// CreateConsensusEngine creates the required type of consensus GetEngine instance for an Ethereum service
func CreateConsensusEngine(ctx *node.ServiceContext, config *ethash.Config, chainConfig *params.ChainConfig, db ethdb.Database) consensus.Engine {
	// If proof-of-authority is requested, set it up
	//if chainConfig.Clique != nil {
	return clique.New(chainConfig.Clique, db)
	//}
	//// Otherwise assume proof-of-work
	//switch {
	//case config.PowMode == ethash.ModeFake:
	//	log.Warn("Ethash used in fake mode")
	//	return ethash.NewFaker()
	//case config.PowMode == ethash.ModeTest:
	//	log.Warn("Ethash used in test mode")
	//	return ethash.NewTester()
	//case config.PowMode == ethash.ModeShared:
	//	log.Warn("Ethash used in shared mode")
	//	return ethash.NewShared()
	//default:
	//	engine := ethash.New(ethash.Config{
	//		CacheDir:       ctx.ResolvePath(config.CacheDir),
	//		CachesInMem:    config.CachesInMem,
	//		CachesOnDisk:   config.CachesOnDisk,
	//		DatasetDir:     config.DatasetDir,
	//		DatasetsInMem:  config.DatasetsInMem,
	//		DatasetsOnDisk: config.DatasetsOnDisk,
	//	})
	//	engine.SetThreads(-1) // Disable CPU mining
	//	return engine
	//}
}
