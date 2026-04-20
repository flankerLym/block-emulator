package partition

import (
	"blockEmulator/utils"
	"math"
)

// ShadowCapsule 是 ZK-SCAR 中的"影子胶囊"抽象。
// 这里不直接实现真正的零知识证明电路，而是在协议仿真层保留
// "待迁移账户 + 当前分片 + 目标分片 + 迁移收益"的最小信息，
// 供 supervisor 侧的重分片算法使用。
type ShadowCapsule struct {
	Addr         string
	CurrentShard int
	TargetShard  int
	Degree       int
	LocalityGain float64
}

// ZKSCARState 保存 ZK-SCAR 重分片算法运行所需的状态。
// 它复用当前项目 CLPA 的图建模方式，但分片打分逻辑不同：
// 1. 优先把高连接度账户迁到更多邻居所在的分片；
// 2. 同时对目标分片负载做惩罚；
// 3. 对频繁来回迁移给一个稳定性约束。
type ZKSCARState struct {
	NetGraph          Graph
	PartitionMap      map[Vertex]int
	VertexsNumInShard []int
	ShardNum          int

	MaxIterations  int
	HotnessWeight  float64
	BalanceWeight  float64
	StabilityBias  float64
	CrossShardEdge int

	ShadowCapsules map[Vertex]ShadowCapsule
}

// Init_ZKSCARState 初始化算法参数。
func (zs *ZKSCARState) Init_ZKSCARState(hotnessWeight, balanceWeight, stabilityBias float64, maxIter, sn int) {
	zs.ShardNum = sn
	zs.MaxIterations = maxIter
	zs.HotnessWeight = hotnessWeight
	zs.BalanceWeight = balanceWeight
	zs.StabilityBias = stabilityBias
	zs.VertexsNumInShard = make([]int, zs.ShardNum)
	zs.PartitionMap = make(map[Vertex]int)
	zs.ShadowCapsules = make(map[Vertex]ShadowCapsule)
}

// AddVertex 加入节点；若此前已经有分片映射则保持，否则按地址尾数默认分片。
func (zs *ZKSCARState) AddVertex(v Vertex) {
	zs.NetGraph.AddVertex(v)
	if _, ok := zs.PartitionMap[v]; !ok {
		zs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	}
}

// AddEdge 加入边，同时补齐端点默认分片。
func (zs *ZKSCARState) AddEdge(u, v Vertex) {
	if _, ok := zs.NetGraph.VertexSet[u]; !ok {
		zs.AddVertex(u)
	}
	if _, ok := zs.NetGraph.VertexSet[v]; !ok {
		zs.AddVertex(v)
	}
	zs.NetGraph.AddEdge(u, v)
}

// recomputeShardVertexCount 重新统计每个分片中的账户数。
func (zs *ZKSCARState) recomputeShardVertexCount() {
	zs.VertexsNumInShard = make([]int, zs.ShardNum)
	for v := range zs.NetGraph.VertexSet {
		if _, ok := zs.PartitionMap[v]; !ok {
			zs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
		}
		zs.VertexsNumInShard[zs.PartitionMap[v]]++
	}
}

// recomputeCrossShardEdges 重新统计跨分片边数。
func (zs *ZKSCARState) recomputeCrossShardEdges() {
	cross := 0
	for v, lst := range zs.NetGraph.EdgeSet {
		vShard := zs.PartitionMap[v]
		for _, u := range lst {
			if zs.PartitionMap[u] != vShard {
				cross++
			}
		}
	}
	zs.CrossShardEdge = cross / 2
}

// scoreMove 计算把 v 放到 targetShard 的分数。
// 分数越高越优：
// - localityScore：邻居越多落在目标分片，得分越高；
// - balancePenalty：目标分片越拥挤，惩罚越大；
// - stabilityTerm：留在原分片会有轻微稳定性偏置，减少抖动。
func (zs *ZKSCARState) scoreMove(v Vertex, targetShard int) float64 {
	neighbors := zs.NetGraph.EdgeSet[v]
	degree := len(neighbors)
	currentShard := zs.PartitionMap[v]

	if degree == 0 {
		if targetShard == currentShard {
			return zs.StabilityBias * 0.05
		}
		return -math.MaxFloat64
	}

	internalCnt := 0
	for _, u := range neighbors {
		if zs.PartitionMap[u] == targetShard {
			internalCnt++
		}
	}

	localityScore := float64(internalCnt) / float64(degree)

	avgLoad := float64(len(zs.NetGraph.VertexSet)) / float64(zs.ShardNum)
	if avgLoad < 1 {
		avgLoad = 1
	}
	targetLoad := float64(zs.VertexsNumInShard[targetShard] + 1)
	balancePenalty := math.Abs(targetLoad-avgLoad) / avgLoad

	score := localityScore + zs.HotnessWeight*localityScore - zs.BalanceWeight*balancePenalty

	if targetShard == currentShard {
		score += zs.StabilityBias * 0.05
	} else {
		score -= zs.StabilityBias * 0.01
	}

	if currentShard != targetShard && zs.VertexsNumInShard[currentShard] <= 1 {
		return -math.MaxFloat64
	}

	return score
}

// ZKSCAR_Partition 运行一次 ZK-SCAR 划分。
// 返回值与 CLPA_Partition 保持一致：
// map[string]uint64 记录发生迁移的账户及其目标分片；
// int 为更新后的跨分片边数。
func (zs *ZKSCARState) ZKSCAR_Partition() (map[string]uint64, int) {
	res := make(map[string]uint64)
	if len(zs.NetGraph.VertexSet) == 0 {
		return res, 0
	}

	zs.recomputeShardVertexCount()
	zs.recomputeCrossShardEdges()
	zs.ShadowCapsules = make(map[Vertex]ShadowCapsule)

	for iter := 0; iter < zs.MaxIterations; iter++ {
		updated := false

		for v := range zs.NetGraph.VertexSet {
			currentShard := zs.PartitionMap[v]
			bestShard := currentShard
			bestScore := zs.scoreMove(v, currentShard)

			checked := make(map[int]bool)
			checked[currentShard] = true

			for _, u := range zs.NetGraph.EdgeSet[v] {
				targetShard := zs.PartitionMap[u]
				if checked[targetShard] {
					continue
				}
				checked[targetShard] = true

				score := zs.scoreMove(v, targetShard)
				if score > bestScore+1e-9 {
					bestScore = score
					bestShard = targetShard
				}
			}

			if bestShard != currentShard {
				zs.VertexsNumInShard[currentShard]--
				zs.VertexsNumInShard[bestShard]++
				zs.PartitionMap[v] = bestShard
				res[v.Addr] = uint64(bestShard)
				zs.ShadowCapsules[v] = ShadowCapsule{
					Addr:         v.Addr,
					CurrentShard: currentShard,
					TargetShard:  bestShard,
					Degree:       len(zs.NetGraph.EdgeSet[v]),
					LocalityGain: bestScore,
				}
				updated = true
			}
		}

		if !updated {
			break
		}
	}

	zs.recomputeCrossShardEdges()
	return res, zs.CrossShardEdge
}

// EraseEdges 在一个 epoch 完成后清空交易图边集合，保留账户当前分片结果。
func (zs *ZKSCARState) EraseEdges() {
	zs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}
