package committee

import (
	"blockEmulator/params"
	"blockEmulator/partition"
	"fmt"
	"math"
	"sort"
	"time"
)

type adaptiveReconfigStats struct {
	ShardLoadEMA     map[uint64]float64
	CrossRatioEMA    float64
	LatencyEMA       float64
	ObservedBlocks   int
	LastTriggerScore float64
	LastImprovement  float64
}

type adaptiveDecision struct {
	ShouldTrigger bool
	Score         float64
	LoadSignal    float64
	CrossSignal   float64
	LatencySignal float64
	Reason        string
}

type migrationCandidate struct {
	Addr   string
	Target uint64
	Score  float64
}

func newAdaptiveReconfigStats() *adaptiveReconfigStats {
	return &adaptiveReconfigStats{
		ShardLoadEMA: make(map[uint64]float64),
	}
}

func updateEMA(prev, value, alpha float64) float64 {
	if prev == 0 {
		return value
	}
	return alpha*value + (1-alpha)*prev
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func (ars *adaptiveReconfigStats) observeBlock(shardID uint64, blockLen int, crossCount int, latency time.Duration) {
	if blockLen <= 0 {
		return
	}
	alpha := params.ReconfigEMAAlpha
	if alpha <= 0 || alpha > 1 {
		alpha = 0.35
	}
	ars.ShardLoadEMA[shardID] = updateEMA(ars.ShardLoadEMA[shardID], float64(blockLen), alpha)

	crossRatio := 0.0
	if blockLen > 0 {
		crossRatio = float64(crossCount) / float64(blockLen)
	}
	ars.CrossRatioEMA = updateEMA(ars.CrossRatioEMA, clamp01(crossRatio), alpha)

	latencyMs := float64(latency.Milliseconds())
	ars.LatencyEMA = updateEMA(ars.LatencyEMA, latencyMs, alpha)

	ars.ObservedBlocks++
}

func (ars *adaptiveReconfigStats) makeDecision() adaptiveDecision {
	warmup := params.ReconfigWarmupBlocks
	if warmup <= 0 {
		warmup = 1
	}
	if ars.ObservedBlocks < warmup {
		return adaptiveDecision{
			ShouldTrigger: false,
			Reason:        fmt.Sprintf("warmup_blocks=%d/%d", ars.ObservedBlocks, warmup),
		}
	}

	avgLoad := 0.0
	maxLoad := 0.0
	minLoad := math.MaxFloat64
	for sid := 0; sid < params.ShardNum; sid++ {
		load := ars.ShardLoadEMA[uint64(sid)]
		avgLoad += load
		if load > maxLoad {
			maxLoad = load
		}
		if load < minLoad {
			minLoad = load
		}
	}
	if params.ShardNum > 0 {
		avgLoad /= float64(params.ShardNum)
	}
	if minLoad == math.MaxFloat64 {
		minLoad = 0
	}
	loadImbalance := 0.0
	if avgLoad > 0 {
		loadImbalance = (maxLoad - minLoad) / avgLoad
	}
	loadSignal := clamp01(loadImbalance / 2.0)

	crossSignal := clamp01(ars.CrossRatioEMA)

	latencyBase := float64(params.Block_Interval)
	if latencyBase <= 0 {
		latencyBase = 5000
	}
	latencySignal := clamp01(ars.LatencyEMA / latencyBase)

	score := params.ReconfigLoadWeight*loadSignal +
		params.ReconfigCrossWeight*crossSignal +
		params.ReconfigLatencyWeight*latencySignal

	if ars.LastImprovement > 0 && ars.LastImprovement < params.ReconfigMinImprovement {
		score *= 0.80
	}

	threshold := params.ReconfigScoreThreshold
	if threshold <= 0 {
		threshold = 0.55
	}
	ars.LastTriggerScore = score

	reason := fmt.Sprintf(
		"score=%.3f threshold=%.3f load=%.3f cross=%.3f latency=%.3f lastImprove=%.3f",
		score, threshold, loadSignal, crossSignal, latencySignal, ars.LastImprovement,
	)

	return adaptiveDecision{
		ShouldTrigger: score >= threshold,
		Score:         score,
		LoadSignal:    loadSignal,
		CrossSignal:   crossSignal,
		LatencySignal: latencySignal,
		Reason:        reason,
	}
}

func estimateMigrationScore(state *partition.CLPAState, addr string, oldShard int, newShard int) float64 {
	v := partition.Vertex{Addr: addr}
	neighbors := state.NetGraph.EdgeSet[v]
	if len(neighbors) == 0 {
		return 0
	}

	gain := 0
	for _, nb := range neighbors {
		neighborShard := state.PartitionMap[nb]
		if neighborShard == newShard {
			gain++
		}
		if neighborShard == oldShard {
			gain--
		}
	}

	return float64(gain) + 0.01*float64(len(neighbors))
}

func limitPartitionByBudget(
	state *partition.CLPAState,
	moved map[string]uint64,
	oldShardFn func(string) uint64,
	budget int,
) map[string]uint64 {
	if budget <= 0 || len(moved) <= budget {
		return moved
	}

	candidates := make([]migrationCandidate, 0, len(moved))
	for addr, target := range moved {
		oldShard := int(oldShardFn(addr))
		candidates = append(candidates, migrationCandidate{
			Addr:   addr,
			Target: target,
			Score:  estimateMigrationScore(state, addr, oldShard, int(target)),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].Addr < candidates[j].Addr
		}
		return candidates[i].Score > candidates[j].Score
	})

	selected := make(map[string]uint64)
	for _, cand := range candidates {
		if len(selected) >= budget {
			break
		}
		if cand.Score <= 0 {
			continue
		}
		selected[cand.Addr] = cand.Target
	}

	for addr := range moved {
		if _, ok := selected[addr]; !ok {
			state.PartitionMap[partition.Vertex{Addr: addr}] = int(oldShardFn(addr))
		}
	}
	state.ComputeEdges2Shard()

	return selected
}
