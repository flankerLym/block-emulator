package params

import "math/big"

type ChainConfig struct {
	ChainID        uint64
	NodeID         uint64
	ShardID        uint64
	Nodes_perShard uint64
	ShardNums      uint64
	BlockSize      uint64
	BlockInterval  uint64
	InjectSpeed    uint64

	// used in transaction relaying, useless in brokerchain mechanism
	MaxRelayBlockSize uint64
}

var (
	SupervisorShard  = uint64(2147483647)
	Init_Balance, _  = new(big.Int).SetString("100000000000000000000000000000000000000000000", 10)
	IPmap_nodeTable  = make(map[uint64]map[uint64]string)
	CommitteeMethod  = []string{"CLPA_Broker", "CLPA", "Broker", "Relay"}
	MeasureBrokerMod = []string{"TPS_Broker", "TCL_Broker", "CrossTxRate_Broker", "TxNumberCount_Broker"}
	MeasureRelayMod  = []string{"TPS_Relay", "TCL_Relay", "CrossTxRate_Relay", "TxNumberCount_Relay"}

	// Justitia incentive mechanism parameters
	BlockReward, _          = new(big.Int).SetString("5000000000000000000", 10) // 5 ETH per block
	MinTxFee, _             = new(big.Int).SetString("1000000000000000", 10)    // 0.001 ETH minimum fee
	CrossShardRewardRate, _ = new(big.Int).SetString("2000000000000000", 10)    // 0.002 ETH per cross-shard tx
	BrokerRewardRate, _     = new(big.Int).SetString("1000000000000000", 10)    // 0.001 ETH per broker tx
	JustitiaEnabled         = true                                              // Enable Justitia incentive mechanism
)
