package committee

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/partition"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"encoding/json"
	"log"
	"sync"
	"time"
)

// ZKSCARCommitteeMod_Broker ZK-SCAR Broker委员会模块
type ZKSCARCommitteeMod_Broker struct {
	Ip_nodeTable    map[uint64]map[uint64]string
	Ss              *signal.StopSignal
	Sl              *supervisor_log.SupervisorLog
	DatasetFile     string
	TotalDataSize   int
	TxBatchSize     int
	ReconfigTimeGap int

	// ZK-SCAR specific fields
	partitioner     *partition.ZKSCARPartitioner
	shadowCapsules  []partition.ShadowCapsule
	batchTxs        [][]message.Tx
	batchLock       sync.Mutex
	lastReconfigTime time.Time
}

// NewZKSCARCommitteeMod_Broker 创建新的ZK-SCAR Broker委员会模块
func NewZKSCARCommitteeMod_Broker(ipMap map[uint64]map[uint64]string, ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, datasetFile string, totalDataSize, txBatchSize, reconfigTimeGap int) CommitteeModule {
	module := &ZKSCARCommitteeMod_Broker{
		Ip_nodeTable:    ipMap,
		Ss:              ss,
		Sl:              sl,
		DatasetFile:     datasetFile,
		TotalDataSize:   totalDataSize,
		TxBatchSize:     txBatchSize,
		ReconfigTimeGap: reconfigTimeGap,
		batchTxs:        make([][]message.Tx, 0),
		lastReconfigTime: time.Now(),
	}
	
	// 初始化ZK-SCAR分区器
	module.partitioner = partition.NewZKSCARPartitioner(params.ShardNum, 10000) // 假设10000个账户
	module.partitioner.Initialize()
	
	return module
}

// MsgSendingControl 消息发送控制
func (d *ZKSCARCommitteeMod_Broker) MsgSendingControl() {
	// 读取交易数据
	txs := readTxFromDataset(d.DatasetFile, d.TotalDataSize)
	if len(txs) == 0 {
		d.Sl.Slog.Println("No transactions found in dataset file!")
		return
	}

	txCount := 0
	batchIndex := 0

	for txCount < len(txs) {
		// 检查是否需要重新分区
		d.checkAndReconfig()

		// 准备当前批次的交易
		end := txCount + d.TxBatchSize
		if end > len(txs) {
			end = len(txs)
		}
		currentBatch := txs[txCount:end]
		txCount = end

		// 处理阴影胶囊
		d.processShadowCapsules()

		// 发送分区消息
		d.sendPartitionMessage()

		// 发送交易消息（Broker模式）
		d.sendTxMessages_Broker(currentBatch, batchIndex)

		batchIndex++
		// 模拟交易处理时间
		time.Sleep(time.Duration(params.Block_Interval) * time.Millisecond)
	}

	// 发送最后一个空批次，通知节点交易结束
	d.sendEmptyBatch_Broker(batchIndex)
}

// checkAndReconfig 检查并执行重新分区
func (d *ZKSCARCommitteeMod_Broker) checkAndReconfig() {
	if time.Since(d.lastReconfigTime).Seconds() > float64(d.ReconfigTimeGap) {
		d.Sl.Slog.Println("Performing ZK-SCAR Broker reconfiguration...")
		
		// 处理阴影胶囊
		d.processShadowCapsules()
		
		// 发送分区消息
		d.sendPartitionMessage()
		
		d.lastReconfigTime = time.Now()
	}
}

// processShadowCapsules 处理阴影胶囊
func (d *ZKSCARCommitteeMod_Broker) processShadowCapsules() {
	if len(d.shadowCapsules) > 0 {
		for _, capsule := range d.shadowCapsules {
			d.partitioner.AddShadowCapsule(capsule)
		}
		
		validCapsules := d.partitioner.ProcessShadowCapsules()
		d.Sl.Slog.Printf("Processed %d valid shadow capsules", len(validCapsules))
		
		// 清空阴影胶囊
		d.shadowCapsules = make([]partition.ShadowCapsule, 0)
	}
}

// sendPartitionMessage 发送分区消息
func (d *ZKSCARCommitteeMod_Broker) sendPartitionMessage() {
	partitionMsg := d.partitioner.GeneratePartitionMessage()
	partitionData, err := json.Marshal(partitionMsg)
	if err != nil {
		log.Panicf("Failed to marshal partition message: %v", err)
	}
	
	msg := message.MergeMessage(message.CPartition, partitionData)
	
	// 发送给所有节点
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		for nid := uint64(0); nid < uint64(params.NodesInShard); nid++ {
			networks.TcpDial(msg, d.Ip_nodeTable[sid][nid])
		}
	}
	
	d.Sl.Slog.Println("Sent partition message to all nodes")
}

// sendTxMessages_Broker 发送交易消息（Broker模式）
func (d *ZKSCARCommitteeMod_Broker) sendTxMessages_Broker(txs []message.Tx, batchIndex int) {
	// 在Broker模式下，交易首先发送给Broker节点
	// 这里简化处理，将所有交易发送给每个分片的Broker节点（假设节点0是Broker）
	
	txData, err := json.Marshal(txs)
	if err != nil {
		log.Panicf("Failed to marshal tx data: %v", err)
	}

	txMsg := &message.TxMsg{
		ShardID: params.SupervisorShard,
		NodeID:  0,
		TxData:  txData,
		BatchID: batchIndex,
	}

	txMsgData, err := json.Marshal(txMsg)
	if err != nil {
		log.Panicf("Failed to marshal tx message: %v", err)
	}

	msg := message.MergeMessage(message.CTx, txMsgData)
	
	// 发送给每个分片的Broker节点
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		networks.TcpDial(msg, d.Ip_nodeTable[sid][0])
	}

	d.Sl.Slog.Printf("Sent batch %d with %d transactions to all brokers", batchIndex, len(txs))
}

// sendEmptyBatch_Broker 发送空批次（Broker模式）
func (d *ZKSCARCommitteeMod_Broker) sendEmptyBatch_Broker(batchIndex int) {
	emptyTxMsg := &message.TxMsg{
		ShardID: params.SupervisorShard,
		NodeID:  0,
		TxData:  []byte("[]"), // 空交易数组
		BatchID: batchIndex,
	}

	txMsgData, err := json.Marshal(emptyTxMsg)
	if err != nil {
		log.Panicf("Failed to marshal empty tx message: %v", err)
	}

	msg := message.MergeMessage(message.CTx, txMsgData)

	// 发送给所有节点
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		for nid := uint64(0); nid < uint64(params.NodesInShard); nid++ {
			networks.TcpDial(msg, d.Ip_nodeTable[sid][nid])
		}
	}

	d.Sl.Slog.Printf("Sent empty batch %d to all nodes", batchIndex)
}

// HandleBlockInfo 处理区块信息
func (d *ZKSCARCommitteeMod_Broker) HandleBlockInfo(bim *message.BlockInfoMsg) {
	// 处理区块信息，更新热点分数
	d.partitioner.HandleBlockInfo(bim)
	
	// 生成并添加阴影胶囊
	if bim.BlockBodyLength > 0 {
		// 模拟生成阴影胶囊
		for i := uint64(0); i < bim.BlockBodyLength; i++ {
			accountID := uint64(i % 10000) // 模拟账户ID
			currentShard := d.partitioner.GetPartition()[accountID]
			capsule := d.partitioner.GenerateShadowCapsule(accountID, currentShard, bim.BlockBodyLength)
			d.shadowCapsules = append(d.shadowCapsules, capsule)
		}
	}
}

// HandleOtherMessage 处理其他消息
func (d *ZKSCARCommitteeMod_Broker) HandleOtherMessage(msg []byte) {
	// 处理其他类型的消息
	// 这里可以添加自定义消息处理逻辑
}

// GetName 获取模块名称
func (d *ZKSCARCommitteeMod_Broker) GetName() string {
	return "ZKSCAR_Broker_Committee"
}