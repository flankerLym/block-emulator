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
	WindowTxs        int
	WindowCrossTxs   int
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

func (ars *adaptiveReconfigStats) resetWindow() {
	ars.ShardLoadEMA = make(map[uint64]float64)
	ars.CrossRatioEMA = 0
	ars.LatencyEMA = 0
	ars.ObservedBlocks = 0
	ars.WindowTxs = 0
	ars.WindowCrossTxs = 0
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

func effectiveReconfigInterval(baseSeconds int) time.Duration {
	if baseSeconds <= 0 {
		baseSeconds = 1
	}
	factor := params.ReconfigIntervalFactor
	if factor <= 0 {
		factor = 1
	}
	effective := int(math.Round(float64(baseSeconds) * factor))
	if params.ReconfigMinIntervalSec > 0 && effective < params.ReconfigMinIntervalSec {
		effective = params.ReconfigMinIntervalSec
	}
	if effective <= 0 {
		effective = 1
	}
	return time.Duration(effective) * time.Second
}

func graphCrossSignal(state *partition.CLPAState) float64 {
	if state == nil {
		return 0
	}
	state.ComputeEdges2Shard()
	totalEdges := 0
	for _, lst := range state.NetGraph.EdgeSet {
		totalEdges += len(lst)
	}
	totalEdges /= 2
	if totalEdges <= 0 {
		return 0
	}
	return clamp01(float64(state.CrossShardEdgeNum) / float64(totalEdges))
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
	ars.WindowTxs += blockLen
	ars.WindowCrossTxs += crossCount
}

func (ars *adaptiveReconfigStats) makeDecision(state *partition.CLPAState) adaptiveDecision {
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

	minWindowTxs := params.ReconfigMinWindowTx
	if minWindowTxs > 0 && ars.WindowTxs < minWindowTxs {
		return adaptiveDecision{
			ShouldTrigger: false,
			Reason:        fmt.Sprintf("insufficient_window_txs=%d/%d", ars.WindowTxs, minWindowTxs),
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

	currentGraphCross := graphCrossSignal(state)
	crossSignal := clamp01(0.65*ars.CrossRatioEMA + 0.35*currentGraphCross)

	latencyBase := float64(params.Block_Interval)
	if latencyBase <= 0 {
		latencyBase = 5000
	}
	latencySignal := clamp01(ars.LatencyEMA / latencyBase)

	score := params.ReconfigLoadWeight*loadSignal +
		params.ReconfigCrossWeight*crossSignal +
		params.ReconfigLatencyWeight*latencySignal

	if ars.LastImprovement > 0 && ars.LastImprovement < params.ReconfigMinImprovement {
		score *= 0.85
	}

	threshold := params.ReconfigScoreThreshold
	if threshold <= 0 {
		threshold = 0.10
	}
	ars.LastTriggerScore = score

	reason := fmt.Sprintf(
		"score=%.3f threshold=%.3f load=%.3f cross=%.3f latency=%.3f graphCross=%.3f windowTxs=%d lastImprove=%.3f",
		score, threshold, loadSignal, crossSignal, latencySignal, currentGraphCross, ars.WindowTxs, ars.LastImprovement,
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
	degree := len(neighbors)
	if degree == 0 {
		return 0
	}

	sameOld := 0
	sameNew := 0
	for _, nb := range neighbors {
		neighborShard := state.PartitionMap[nb]
		if neighborShard == oldShard {
			sameOld++
		}
		if neighborShard == newShard {
			sameNew++
		}
	}

	crossBefore := degree - sameOld
	crossAfter := degree - sameNew
	crossDelta := float64(crossBefore-crossAfter) / float64(degree)
	localityScore := float64(sameNew) / float64(degree)

	hotnessScore := clamp01(math.Log1p(float64(degree)) / math.Log1p(32.0))

	avgLoad := 0.0
	for _, cnt := range state.VertexsNumInShard {
		avgLoad += float64(cnt)
	}
	if len(state.VertexsNumInShard) > 0 {
		avgLoad /= float64(len(state.VertexsNumInShard))
	}
	balancePenalty := 0.0
	if avgLoad > 0 && newShard >= 0 && newShard < len(state.VertexsNumInShard) {
		targetLoad := float64(state.VertexsNumInShard[newShard] + 1)
		balancePenalty = math.Max(0, (targetLoad-avgLoad)/avgLoad)
	}

	stabilityTerm := 0.0
	if crossDelta > 0 {
		stabilityTerm = params.MigrationStabilityBias
	} else if crossDelta < 0 {
		stabilityTerm = -params.MigrationStabilityBias
	}

	score := params.MigrationLocalityWeight*localityScore +
		params.MigrationHotnessWeight*hotnessScore +
		params.MigrationCrossWeight*crossDelta +
		params.MigrationHotCrossWeight*hotnessScore*crossDelta -
		params.MigrationBalancePenaltyWeight*balancePenalty +
		stabilityTerm

	return score
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

	for addr, target := range moved {
		if _, ok := selected[addr]; ok {
			continue
		}
		oldShard := int(oldShardFn(addr))
		newShard := int(target)
		if newShard >= 0 && newShard < len(state.VertexsNumInShard) {
			state.VertexsNumInShard[newShard]--
		}
		if oldShard >= 0 && oldShard < len(state.VertexsNumInShard) {
			state.VertexsNumInShard[oldShard]++
		}
		state.PartitionMap[partition.Vertex{Addr: addr}] = oldShard
	}
	state.ComputeEdges2Shard()

	return selected
}
