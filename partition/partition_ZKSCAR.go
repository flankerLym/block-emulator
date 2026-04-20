package partition

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// ShadowCapsule 阴影胶囊，用于ZK-SCAR算法
type ShadowCapsule struct {
	AccountID   uint64
	CurrentShard uint64
	TargetShard  uint64
	TransactionCount uint64
	HotspotScore   float64
	ValidityProof  []byte // 有效性证明
}

// ZKSCARConfig ZK-SCAR算法配置
type ZKSCARConfig struct {
	ShardCount         int
	AccountCount       int
	ShadowThreshold    float64 // 阴影阈值
	HotspotThreshold   float64 // 热点阈值
	MigrationBatchSize int     // 迁移批次大小
}

// ZKSCARPartitioner ZK-SCAR分区器
type ZKSCARPartitioner struct {
	config     *ZKSCARConfig
	accountMap map[uint64]uint64 // accountID -> shardID
	shadowCapsules []ShadowCapsule
	rand *rand.Rand
}

// NewZKSCARPartitioner 创建新的ZK-SCAR分区器
func NewZKSCARPartitioner(shardCount, accountCount int) *ZKSCARPartitioner {
	source := rand.NewSource(time.Now().UnixNano())
	return &ZKSCARPartitioner{
		config: &ZKSCARConfig{
			ShardCount:         shardCount,
			AccountCount:       accountCount,
			ShadowThreshold:    0.7,
			HotspotThreshold:   0.8,
			MigrationBatchSize: 100,
		},
		accountMap:     make(map[uint64]uint64),
		shadowCapsules: make([]ShadowCapsule, 0),
		rand:          rand.New(source),
	}
}

// Initialize 初始化分区
func (z *ZKSCARPartitioner) Initialize() map[uint64]uint64 {
	// 初始随机分配
	for i := uint64(0); i < uint64(z.config.AccountCount); i++ {
		shardID := uint64(z.rand.Intn(z.config.ShardCount))
		z.accountMap[i] = shardID
	}
	return z.accountMap
}

// AddShadowCapsule 添加阴影胶囊
func (z *ZKSCARPartitioner) AddShadowCapsule(capsule ShadowCapsule) {
	z.shadowCapsules = append(z.shadowCapsules, capsule)
}

// ProcessShadowCapsules 处理阴影胶囊
func (z *ZKSCARPartitioner) ProcessShadowCapsules() []ShadowCapsule {
	validCapsules := make([]ShadowCapsule, 0)
	
	for _, capsule := range z.shadowCapsules {
		if z.verifyCapsule(capsule) {
			validCapsules = append(validCapsules, capsule)
		}
	}
	
	// 按热点分数排序
	z.sortCapsulesByHotspotScore(validCapsules)
	
	// 处理迁移
	z.processMigrations(validCapsules)
	
	// 清空处理过的胶囊
	z.shadowCapsules = make([]ShadowCapsule, 0)
	
	return validCapsules
}

// verifyCapsule 验证胶囊有效性
func (z *ZKSCARPartitioner) verifyCapsule(capsule ShadowCapsule) bool {
	// 简化的验证逻辑，实际应使用零知识证明
	return capsule.ValidityProof != nil && len(capsule.ValidityProof) > 0
}

// sortCapsulesByHotspotScore 按热点分数排序胶囊
func (z *ZKSCARPartitioner) sortCapsulesByHotspotScore(capsules []ShadowCapsule) {
	// 简单的冒泡排序
	for i := 0; i < len(capsules)-1; i++ {
		for j := 0; j < len(capsules)-i-1; j++ {
			if capsules[j].HotspotScore < capsules[j+1].HotspotScore {
				capsules[j], capsules[j+1] = capsules[j+1], capsules[j]
			}
		}
	}
}

// processMigrations 处理迁移
func (z *ZKSCARPartitioner) processMigrations(capsules []ShadowCapsule) {
	processed := 0
	for _, capsule := range capsules {
		if processed >= z.config.MigrationBatchSize {
			break
		}
		
		// 执行迁移
		z.accountMap[capsule.AccountID] = capsule.TargetShard
		processed++
	}
}

// GenerateShadowCapsule 生成阴影胶囊
func (z *ZKSCARPartitioner) GenerateShadowCapsule(accountID, currentShard uint64, txCount uint64) ShadowCapsule {
	// 计算热点分数
	hotspotScore := float64(txCount) / 1000.0
	if hotspotScore > 1.0 {
		hotspotScore = 1.0
	}
	
	// 确定目标分片
	targetShard := currentShard
	if hotspotScore > z.config.HotspotThreshold {
		// 随机选择一个不同的分片
		for targetShard == currentShard {
			targetShard = uint64(z.rand.Intn(z.config.ShardCount))
		}
	}
	
	// 生成简单的有效性证明
	proof := []byte(fmt.Sprintf("proof_%d_%d_%d", accountID, currentShard, targetShard))
	
	return ShadowCapsule{
		AccountID:        accountID,
		CurrentShard:     currentShard,
		TargetShard:      targetShard,
		TransactionCount: txCount,
		HotspotScore:     hotspotScore,
		ValidityProof:    proof,
	}
}

// GetPartition 获取当前分区
func (z *ZKSCARPartitioner) GetPartition() map[uint64]uint64 {
	return z.accountMap
}

// HandleBlockInfo 处理区块信息，用于更新热点分数
func (z *ZKSCARPartitioner) HandleBlockInfo(bim *message.BlockInfoMsg) {
	// 这里可以根据区块信息更新热点分数
	// 简化实现，实际应根据交易内容计算
}

// GeneratePartitionMessage 生成分区消息
func (z *ZKSCARPartitioner) GeneratePartitionMessage() *message.PartitionMsg {
	partitionData, err := json.Marshal(z.accountMap)
	if err != nil {
		log.Panicf("Failed to marshal partition data: %v", err)
	}
	
	return &message.PartitionMsg{
		ShardID:       params.SupervisorShard,
		NodeID:        0,
		PartitionData: partitionData,
		Timestamp:     time.Now().UnixNano(),
	}
}

// ApplyPartition 应用分区
func (z *ZKSCARPartitioner) ApplyPartition(partitionMsg *message.PartitionMsg) {
	err := json.Unmarshal(partitionMsg.PartitionData, &z.accountMap)
	if err != nil {
		log.Panicf("Failed to unmarshal partition data: %v", err)
	}
}

// CalculateLoadBalance 计算负载均衡度
func (z *ZKSCARPartitioner) CalculateLoadBalance() float64 {
	shardCounts := make(map[uint64]int)
	for _, shardID := range z.accountMap {
		shardCounts[shardID]++
	}
	
	if len(shardCounts) == 0 {
		return 1.0
	}
	
	avg := float64(len(z.accountMap)) / float64(z.config.ShardCount)
	variance := 0.0
	for _, count := range shardCounts {
		diff := float64(count) - avg
		variance += diff * diff
	}
	variance /= float64(len(shardCounts))
	
	// 计算均衡度 (0-1，越接近1越均衡)
	maxVariance := avg * avg * float64(z.config.ShardCount-1)
	if maxVariance == 0 {
		return 1.0
	}
	
	return 1.0 - (variance / maxVariance)
}