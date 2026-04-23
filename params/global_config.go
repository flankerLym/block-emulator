package params

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

var (
	// The following parameters can be set in main.go.
	// default values:
	NodesInShard = 4 // \# of Nodes in a shard.
	ShardNum     = 4 // \# of shards.
)

// consensus layer & output file path
var (
	ConsensusMethod = 0 // ConsensusMethod an Integer, which indicates the choice ID of methods / consensuses. Value range: [0, 4), representing [CLPA_Broker, CLPA, Broker, Relay]"

	PbftViewChangeTimeOut = 10000 // The view change threshold of pbft. If the process of PBFT is too slow, the view change mechanism will be triggered.

	Block_Interval = 5000 // The time interval for generating a new block

	MaxBlockSize_global = 2000  // The maximum number of transactions a block contains
	BlocksizeInBytes    = 20000 // The maximum size (in bytes) of block body
	UseBlocksizeInBytes = 0     // Use blocksizeInBytes as the blocksize measurement if '1'.

	InjectSpeed   = 2000   // The speed of transaction injection
	TotalDataSize = 160000 // The total number of txs to be injected
	TxBatchSize   = 16000  // The supervisor read a batch of txs then send them. The size of a batch is 'TxBatchSize'

	BrokerNum            = 10 // The # of Broker accounts used in Broker / CLPA_Broker.
	RelayWithMerkleProof = 0  // When using a consensus about "Relay", nodes will send Tx Relay with proof if "RelayWithMerkleProof" = 1

	ExpDataRootDir     = "expTest"                     // The root dir where the experimental data should locate.
	DataWrite_path     = ExpDataRootDir + "/result/"   // Measurement data result output path
	LogWrite_path      = ExpDataRootDir + "/log"       // Log output path
	DatabaseWrite_path = ExpDataRootDir + "/database/" // database write path

	SupervisorAddr = "127.0.0.1:18800"        // Supervisor ip address
	DatasetFile    = `./selectedTxs_300K.csv` // The raw BlockTransaction data path

	ReconfigTimeGap = 50 // The time gap between epochs. This variable is only used in CLPA / CLPA_Broker now.

	AdaptiveReconfigEnabled       = 0    // 1 enables ABR-Shard's adaptive trigger; 0 keeps fixed-interval behavior.
	ReconfigScoreThreshold        = 0.10 // Trigger threshold of ABR-Shard.
	ReconfigLoadWeight            = 0.30 // Weight of shard-load imbalance in the trigger score.
	ReconfigCrossWeight           = 0.50 // Weight of cross-shard pressure in the trigger score.
	ReconfigLatencyWeight         = 0.20 // Weight of block latency pressure in the trigger score.
	MigrationBudget               = 200  // Maximum number of accounts moved in one reconfiguration round.
	ReconfigMinImprovement        = 0.02 // If the previous improvement is below this ratio, the next trigger is suppressed.
	ReconfigEMAAlpha              = 0.35 // EMA factor for runtime metrics.
	ReconfigWarmupBlocks          = 8    // Minimum number of observed blocks before adaptive triggering starts.
	ReconfigIntervalFactor        = 0.60 // Moderate increase of checking frequency: 50s -> 30s when ReconfigTimeGap=50.
	ReconfigMinIntervalSec        = 20   // Lower bound of the effective reconfiguration check interval.
	ReconfigMinWindowTx           = 4000 // Minimum number of newly observed txs before a reconfiguration decision is allowed.
	MigrationLocalityWeight       = 0.20 // Residual locality preference in migration ranking.
	MigrationHotnessWeight        = 0.15 // Standalone hotness reward in migration ranking.
	MigrationCrossWeight          = 1.60 // Explicit reward for reducing cross-shard edges.
	MigrationHotCrossWeight       = 0.80 // Extra reward when a hot account also reduces cross-shard edges.
	MigrationBalancePenaltyWeight = 0.25 // Penalty of moving into an overloaded shard.
	MigrationStabilityBias        = 0.05 // Small stability bonus/penalty to avoid noisy moves.
)

// network layer
var (
	Delay       int // The delay of network (ms) when sending. 0 if delay < 0
	JitterRange int // The jitter range of delay (ms). Jitter follows a uniform distribution. 0 if JitterRange < 0.
	Bandwidth   int // The bandwidth limit (Bytes). +inf if bandwidth < 0
)

// read from file
type globalConfig struct {
	ConsensusMethod int `json:"ConsensusMethod"`

	PbftViewChangeTimeOut int `json:"PbftViewChangeTimeOut"`

	ExpDataRootDir string `json:"ExpDataRootDir"`

	BlockInterval int `json:"Block_Interval"`

	BlocksizeInBytes    int `json:"BlocksizeInBytes"`
	MaxBlockSizeGlobal  int `json:"BlockSize"`
	UseBlocksizeInBytes int `json:"UseBlocksizeInBytes"`

	InjectSpeed   int `json:"InjectSpeed"`
	TotalDataSize int `json:"TotalDataSize"`

	TxBatchSize          int    `json:"TxBatchSize"`
	BrokerNum            int    `json:"BrokerNum"`
	RelayWithMerkleProof int    `json:"RelayWithMerkleProof"`
	DatasetFile          string `json:"DatasetFile"`
	ReconfigTimeGap      int    `json:"ReconfigTimeGap"`

	AdaptiveReconfigEnabled       int     `json:"AdaptiveReconfigEnabled"`
	ReconfigScoreThreshold        float64 `json:"ReconfigScoreThreshold"`
	ReconfigLoadWeight            float64 `json:"ReconfigLoadWeight"`
	ReconfigCrossWeight           float64 `json:"ReconfigCrossWeight"`
	ReconfigLatencyWeight         float64 `json:"ReconfigLatencyWeight"`
	MigrationBudget               int     `json:"MigrationBudget"`
	ReconfigMinImprovement        float64 `json:"ReconfigMinImprovement"`
	ReconfigEMAAlpha              float64 `json:"ReconfigEMAAlpha"`
	ReconfigWarmupBlocks          int     `json:"ReconfigWarmupBlocks"`
	ReconfigIntervalFactor        float64 `json:"ReconfigIntervalFactor"`
	ReconfigMinIntervalSec        int     `json:"ReconfigMinIntervalSec"`
	ReconfigMinWindowTx           int     `json:"ReconfigMinWindowTx"`
	MigrationLocalityWeight       float64 `json:"MigrationLocalityWeight"`
	MigrationHotnessWeight        float64 `json:"MigrationHotnessWeight"`
	MigrationCrossWeight          float64 `json:"MigrationCrossWeight"`
	MigrationHotCrossWeight       float64 `json:"MigrationHotCrossWeight"`
	MigrationBalancePenaltyWeight float64 `json:"MigrationBalancePenaltyWeight"`
	MigrationStabilityBias        float64 `json:"MigrationStabilityBias"`

	Delay       int `json:"Delay"`
	JitterRange int `json:"JitterRange"`
	Bandwidth   int `json:"Bandwidth"`
}

func ReadConfigFile() {
	// read configurations from paramsConfig.json
	data, err := os.ReadFile("paramsConfig.json")
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	var config globalConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// output configurations
	fmt.Printf("Config: %+v\n", config)

	// set configurations to params
	// consensus params
	ConsensusMethod = config.ConsensusMethod

	PbftViewChangeTimeOut = config.PbftViewChangeTimeOut

	// data file params
	ExpDataRootDir = config.ExpDataRootDir
	DataWrite_path = ExpDataRootDir + "/result/"
	LogWrite_path = ExpDataRootDir + "/log"
	DatabaseWrite_path = ExpDataRootDir + "/database/"

	Block_Interval = config.BlockInterval

	MaxBlockSize_global = config.MaxBlockSizeGlobal
	BlocksizeInBytes = config.BlocksizeInBytes
	UseBlocksizeInBytes = config.UseBlocksizeInBytes

	InjectSpeed = config.InjectSpeed
	TotalDataSize = config.TotalDataSize
	TxBatchSize = config.TxBatchSize

	BrokerNum = config.BrokerNum
	RelayWithMerkleProof = config.RelayWithMerkleProof
	DatasetFile = config.DatasetFile

	ReconfigTimeGap = config.ReconfigTimeGap

	AdaptiveReconfigEnabled = config.AdaptiveReconfigEnabled
	if config.ReconfigScoreThreshold > 0 {
		ReconfigScoreThreshold = config.ReconfigScoreThreshold
	}
	if config.ReconfigLoadWeight > 0 {
		ReconfigLoadWeight = config.ReconfigLoadWeight
	}
	if config.ReconfigCrossWeight > 0 {
		ReconfigCrossWeight = config.ReconfigCrossWeight
	}
	if config.ReconfigLatencyWeight > 0 {
		ReconfigLatencyWeight = config.ReconfigLatencyWeight
	}
	if config.MigrationBudget > 0 {
		MigrationBudget = config.MigrationBudget
	}
	if config.ReconfigMinImprovement > 0 {
		ReconfigMinImprovement = config.ReconfigMinImprovement
	}
	if config.ReconfigEMAAlpha > 0 && config.ReconfigEMAAlpha <= 1 {
		ReconfigEMAAlpha = config.ReconfigEMAAlpha
	}
	if config.ReconfigWarmupBlocks > 0 {
		ReconfigWarmupBlocks = config.ReconfigWarmupBlocks
	}
	if config.ReconfigIntervalFactor > 0 {
		ReconfigIntervalFactor = config.ReconfigIntervalFactor
	}
	if config.ReconfigMinIntervalSec > 0 {
		ReconfigMinIntervalSec = config.ReconfigMinIntervalSec
	}
	if config.ReconfigMinWindowTx > 0 {
		ReconfigMinWindowTx = config.ReconfigMinWindowTx
	}
	if config.MigrationLocalityWeight > 0 {
		MigrationLocalityWeight = config.MigrationLocalityWeight
	}
	if config.MigrationHotnessWeight > 0 {
		MigrationHotnessWeight = config.MigrationHotnessWeight
	}
	if config.MigrationCrossWeight > 0 {
		MigrationCrossWeight = config.MigrationCrossWeight
	}
	if config.MigrationHotCrossWeight > 0 {
		MigrationHotCrossWeight = config.MigrationHotCrossWeight
	}
	if config.MigrationBalancePenaltyWeight > 0 {
		MigrationBalancePenaltyWeight = config.MigrationBalancePenaltyWeight
	}
	if config.MigrationStabilityBias > 0 {
		MigrationStabilityBias = config.MigrationStabilityBias
	}

	// network params
	Delay = config.Delay
	JitterRange = config.JitterRange
	Bandwidth = config.Bandwidth
}
