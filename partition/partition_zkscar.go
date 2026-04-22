package partition

import (
	"blockEmulator/utils"
	"math"
	"sort"
)

// ShadowCapsuleCandidate is the supervisor-side metadata used to explain why an
// address is selected for cutover in the current ZK-SCAR epoch.
type ShadowCapsuleCandidate struct {
	Addr         string
	CurrentShard int
	TargetShard  int
	Degree       int
	Hotness      float64
	LocalityGain float64
}

// ZKSCARState implements a deterministic, locality-first re-sharding heuristic
// for the current emulator. It is purposely scoped to the account-transfer
// model already supported by the simulator: normal transfer accounts with MPT
// state commitments. Within that scope, the heuristic yields a concrete
// ownership-cutover plan plus shadow-capsule metadata for the next epoch.
type ZKSCARState struct {
	NetGraph     Graph
	PartitionMap map[Vertex]int
	ShardLoads   []int

	HotnessWeight float64
	BalanceWeight float64
	StabilityBias float64
	MaxIterations int
	ShardNum      int

	shadowCapsules []ShadowCapsuleCandidate
}

func (zs *ZKSCARState) Init_ZKSCARState(hotnessWeight, balanceWeight, stabilityBias float64, maxIterations, shardNum int) {
	zs.NetGraph = Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}
	zs.PartitionMap = make(map[Vertex]int)
	zs.ShardLoads = make([]int, shardNum)
	zs.HotnessWeight = hotnessWeight
	zs.BalanceWeight = balanceWeight
	zs.StabilityBias = stabilityBias
	zs.MaxIterations = maxIterations
	zs.ShardNum = shardNum
	zs.shadowCapsules = make([]ShadowCapsuleCandidate, 0)
}

func (zs *ZKSCARState) AddVertex(v Vertex) {
	if _, ok := zs.NetGraph.VertexSet[v]; ok {
		return
	}
	zs.NetGraph.AddVertex(v)
	if _, ok := zs.PartitionMap[v]; !ok {
		shard := utils.Addr2Shard(v.Addr)
		if zs.ShardNum > 0 {
			shard = shard % zs.ShardNum
		}
		zs.PartitionMap[v] = shard
		if shard >= 0 && shard < len(zs.ShardLoads) {
			zs.ShardLoads[shard]++
		}
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

func (zs *ZKSCARState) recomputeShardLoads() {
	zs.ShardLoads = make([]int, zs.ShardNum)
	for v := range zs.NetGraph.VertexSet {
		shard, ok := zs.PartitionMap[v]
		if !ok {
			continue
		}
		if shard >= 0 && shard < len(zs.ShardLoads) {
			zs.ShardLoads[shard]++
		}
	}
}

func (zs *ZKSCARState) degree(v Vertex) int {
	return len(zs.NetGraph.EdgeSet[v])
}

func (zs *ZKSCARState) hotness(v Vertex) float64 {
	return float64(zs.degree(v))
}

func (zs *ZKSCARState) neighborsInShard(v Vertex, shard int) int {
	cnt := 0
	for _, n := range zs.NetGraph.EdgeSet[v] {
		if zs.PartitionMap[n] == shard {
			cnt++
		}
	}
	return cnt
}

func (zs *ZKSCARState) shardLoadAfterMove(v Vertex, target int) float64 {
	current := zs.PartitionMap[v]
	load := float64(zs.ShardLoads[target])
	if current != target {
		load++
	}
	return load
}

func (zs *ZKSCARState) averageLoad() float64 {
	if zs.ShardNum == 0 {
		return 0
	}
	return float64(len(zs.NetGraph.VertexSet)) / float64(zs.ShardNum)
}

func (zs *ZKSCARState) balancePenalty(v Vertex, target int) float64 {
	avg := zs.averageLoad()
	if avg <= 0 {
		return 0
	}
	futureLoad := zs.shardLoadAfterMove(v, target)
	return math.Abs(futureLoad-avg) / avg
}

func (zs *ZKSCARState) score(v Vertex, target int) float64 {
	deg := zs.degree(v)
	if deg == 0 {
		if target == zs.PartitionMap[v] {
			return zs.StabilityBias
		}
		return -zs.BalanceWeight * zs.balancePenalty(v, target)
	}
	locality := float64(zs.neighborsInShard(v, target)) / float64(deg)
	score := zs.HotnessWeight*zs.hotness(v)*locality - zs.BalanceWeight*zs.balancePenalty(v, target)
	if target == zs.PartitionMap[v] {
		score += zs.StabilityBias
	}
	return score
}

func (zs *ZKSCARState) sortedVertices() []Vertex {
	vertices := make([]Vertex, 0, len(zs.NetGraph.VertexSet))
	for v := range zs.NetGraph.VertexSet {
		vertices = append(vertices, v)
	}
	sort.Slice(vertices, func(i, j int) bool {
		return vertices[i].Addr < vertices[j].Addr
	})
	return vertices
}

func (zs *ZKSCARState) rememberCapsule(v Vertex, from, to int, gain float64) {
	candidate := ShadowCapsuleCandidate{
		Addr:         v.Addr,
		CurrentShard: from,
		TargetShard:  to,
		Degree:       zs.degree(v),
		Hotness:      zs.hotness(v),
		LocalityGain: gain,
	}
	for i := range zs.shadowCapsules {
		if zs.shadowCapsules[i].Addr == candidate.Addr {
			zs.shadowCapsules[i] = candidate
			return
		}
	}
	zs.shadowCapsules = append(zs.shadowCapsules, candidate)
}

// ZKSCAR_Partition returns the moved-address map and an aggregate score that is
// analogous to a reduced cross-shard pressure metric. The heuristic is greedy
// but deterministic, which keeps experiments reproducible.
func (zs *ZKSCARState) ZKSCAR_Partition() (map[string]uint64, int) {
	zs.recomputeShardLoads()
	zs.shadowCapsules = make([]ShadowCapsuleCandidate, 0)
	moved := make(map[string]uint64)
	totalGain := 0.0

	if zs.ShardNum <= 1 || len(zs.NetGraph.VertexSet) == 0 {
		return moved, 0
	}

	for iter := 0; iter < zs.MaxIterations; iter++ {
		changed := false
		for _, v := range zs.sortedVertices() {
			current := zs.PartitionMap[v]
			if current < 0 || current >= len(zs.ShardLoads) {
				continue
			}
			if zs.ShardLoads[current] <= 1 {
				continue
			}

			bestShard := current
			bestScore := zs.score(v, current)
			for shard := 0; shard < zs.ShardNum; shard++ {
				if shard == current {
					continue
				}
				s := zs.score(v, shard)
				if s > bestScore+1e-9 {
					bestScore = s
					bestShard = shard
				}
			}

			if bestShard == current {
				continue
			}

			gain := bestScore - zs.score(v, current)
			zs.PartitionMap[v] = bestShard
			zs.ShardLoads[current]--
			zs.ShardLoads[bestShard]++
			moved[v.Addr] = uint64(bestShard)
			zs.rememberCapsule(v, current, bestShard, gain)
			totalGain += gain
			changed = true
		}
		if !changed {
			break
		}
	}

	return moved, int(math.Round(totalGain*1000))
}

func (zs *ZKSCARState) SnapshotShadowCapsules() []ShadowCapsuleCandidate {
	out := make([]ShadowCapsuleCandidate, len(zs.shadowCapsules))
	copy(out, zs.shadowCapsules)
	sort.Slice(out, func(i, j int) bool {
		if out[i].CurrentShard == out[j].CurrentShard {
			if out[i].TargetShard == out[j].TargetShard {
				return out[i].Addr < out[j].Addr
			}
			return out[i].TargetShard < out[j].TargetShard
		}
		return out[i].CurrentShard < out[j].CurrentShard
	})
	return out
}
