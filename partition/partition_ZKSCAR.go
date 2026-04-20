package partition

import (
	"blockEmulator/utils"
	"math"
)

// ShadowCapsule 是 ZK-SCAR 在算法层的"迁移候选摘要"。
// 注意：这里是 partition 包内部的算法视角，不等同于 message 包里真正随协议流转的 capsule。
type ShadowCapsule struct {
	Addr         string
	CurrentShard int
	TargetShard  int
	Degree       int

	HotnessScore float64
	LocalityGain float64
}

type ZKSCARState struct {
	NetGraph          Graph
	PartitionMap      map[Vertex]int
	VertexsNumInShard []int
	ShardNum          int

	MaxIterations int

	HotnessWeight float64
	BalanceWeight float64
	StabilityBias float64

	CrossShardEdge int

	ShadowCapsules map[Vertex]ShadowCapsule
}

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

func (zs *ZKSCARState) AddVertex(v Vertex) {
	zs.NetGraph.AddVertex(v)
	if _, ok := zs.PartitionMap[v]; !ok {
		zs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	}
}

func (zs *ZKSCARState) AddEdge(u, v Vertex) {
	if _, ok := zs.NetGraph.VertexSet[u]; !ok {
		zs.AddVertex(u)
	}
	if _, ok := zs.NetGraph.VertexSet[v]; !ok {
		zs.AddVertex(v)
	}
	zs.NetGraph.AddEdge(u, v)
}

func (zs *ZKSCARState) recomputeShardVertexCount() {
	zs.VertexsNumInShard = make([]int, zs.ShardNum)
	for v := range zs.NetGraph.VertexSet {
		if _, ok := zs.PartitionMap[v]; !ok {
			zs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
		}
		zs.VertexsNumInShard[zs.PartitionMap[v]]++
	}
}

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

func (zs *ZKSCARState) averageDegree() float64 {
	if len(zs.NetGraph.VertexSet) == 0 {
		return 1.0
	}
	total := 0
	for v := range zs.NetGraph.VertexSet {
		total += len(zs.NetGraph.EdgeSet[v])
	}
	avg := float64(total) / float64(len(zs.NetGraph.VertexSet))
	if avg < 1.0 {
		return 1.0
	}
	return avg
}

func (zs *ZKSCARState) hotnessScore(v Vertex, avgDegree float64) float64 {
	degree := float64(len(zs.NetGraph.EdgeSet[v]))
	if avgDegree <= 0 {
		return 0
	}
	score := degree / avgDegree
	if score > 1.5 {
		score = 1.5
	}
	return score
}

// scoreMove 计算把 v 放到 targetShard 的分数。
// 当前分数由三部分构成：
// 1. localityScore：邻居越多在目标分片，越好
// 2. hotnessTerm：高热账户更重视 locality
// 3. balancePenalty：目标分片越拥挤，惩罚越大
// 4. stabilityBias：减少来回抖动
func (zs *ZKSCARState) scoreMove(v Vertex, targetShard int, avgDegree float64) float64 {
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

	hotness := zs.hotnessScore(v, avgDegree)
	hotnessTerm := zs.HotnessWeight * hotness * localityScore

	score := localityScore + hotnessTerm - zs.BalanceWeight*balancePenalty

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

func (zs *ZKSCARState) ZKSCAR_Partition() (map[string]uint64, int) {
	res := make(map[string]uint64)
	if len(zs.NetGraph.VertexSet) == 0 {
		return res, 0
	}

	zs.recomputeShardVertexCount()
	zs.recomputeCrossShardEdges()
	zs.ShadowCapsules = make(map[Vertex]ShadowCapsule)

	avgDegree := zs.averageDegree()

	for iter := 0; iter < zs.MaxIterations; iter++ {
		updated := false

		for v := range zs.NetGraph.VertexSet {
			currentShard := zs.PartitionMap[v]
			currentScore := zs.scoreMove(v, currentShard, avgDegree)
			bestShard := currentShard
			bestScore := currentScore

			checked := make(map[int]bool)
			checked[currentShard] = true

			for _, u := range zs.NetGraph.EdgeSet[v] {
				targetShard := zs.PartitionMap[u]
				if checked[targetShard] {
					continue
				}
				checked[targetShard] = true

				score := zs.scoreMove(v, targetShard, avgDegree)
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
					HotnessScore: zs.hotnessScore(v, avgDegree),
					LocalityGain: bestScore - currentScore,
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

func (zs *ZKSCARState) EraseEdges() {
	zs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}