package configs

import (
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/dashboard"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv6"
	"github.com/tomochain/tomochain/params"
)

type EthstatsConfig struct {
	URL string `toml:",omitempty"`
}

type TomoConfig struct {
	Eth          eth.Config
	Shh          whisper.Config
	Node         node.Config
	Ethstats     EthstatsConfig
	Dashboard    dashboard.Config
	RewardConfig params.RewardConfig
}

var Config TomoConfig

