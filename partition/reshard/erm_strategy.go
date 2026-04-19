package reshard

import (
	"fmt"
	"math"
	"sort"
)

type ERMStrategy struct{}

func (s *ERMStrategy) Name() string {
	return "event-risk-minimal"
}

func (s *ERMStrategy) BuildPlan(snapshot SystemSnapshot, cfg ReshardConfig) ReshardPlan {
	plan := ReshardPlan{
		Triggered:    false,
		VerifierMode: cfg.Verifier,
	}

	if len(snapshot.ShardMetrics) == 0 || snapshot.ShardCount <= 1 {
		plan.Reason = "empty snapshot or shard count <= 1"
		return plan
	}

	loads := make([]float64, 0, len(snapshot.ShardMetrics))
	var crossRates, hotspotScores, riskScores []float64
	for _, sm := range snapshot.ShardMetrics {
		loads = append(loads, sm.Load)
		crossRates = append(crossRates, sm.CrossTxRate)
		hotspotScores = append(hotspotScores, sm.HotspotScore)
		riskScores = append(riskScores, sm.RiskScore)
	}

	triggerScore :=
		cfg.AlphaLoad*CV(loads) +
			cfg.AlphaCross*Average(crossRates) +
			cfg.AlphaHot*CV(hotspotScores) +
			cfg.AlphaRisk*CV(riskScores)

	plan.TriggerScore = triggerScore
	if triggerScore <= cfg.TriggerThreshold {
		plan.Reason = fmt.Sprintf("trigger score %.4f <= threshold %.4f", triggerScore, cfg.TriggerThreshold)
		return plan
	}

	candidates := selectCandidates(snapshot, cfg)
	if len(candidates) == 0 {
		plan.Reason = "triggered but no candidate accounts"
		return plan
	}

	moves := greedyMoves(snapshot, candidates, cfg)
	if len(moves) == 0 {
		plan.Reason = "triggered but no positive-utility moves"
		return plan
	}

	plan.Triggered = true
	plan.Reason = "event-driven trigger matched; positive utility moves found"
	plan.SelectedMoves = moves
	plan.OldCommitment = BuildCommitment(snapshot, nil)
	plan.NewCommitment = BuildCommitment(snapshot, moves)
	return plan
}

func selectCandidates(snapshot SystemSnapshot, cfg ReshardConfig) []AccountStat {
	scoreFn := func(a AccountStat) float64 {
		hot := 0.35*a.Degree + 0.35*a.TxVolume + 0.30*a.Burst
		return 0.40*a.CrossContribution + 0.30*hot + 0.20*a.RiskScore + 0.10*a.RecentActivity
	}

	top := TopNAccountsByScore(snapshot.Accounts, scoreFn, cfg.MaxCandidates)
	out := make([]AccountStat, 0, len(top))
	for _, a := range top {
		score := scoreFn(a)
		if score >= cfg.HotThreshold {
			out = append(out, a)
		}
	}
	return out
}

func greedyMoves(snapshot SystemSnapshot, candidates []AccountStat, cfg ReshardConfig) []CandidateMove {
	loadByShard := map[uint64]float64{}
	riskByShard := map[uint64]float64{}
	for _, sm := range snapshot.ShardMetrics {
		loadByShard[sm.ShardID] = sm.Load
		riskByShard[sm.ShardID] = sm.RiskScore
	}

	var proposed []CandidateMove

	for _, acc := range candidates {
		best := CandidateMove{
			Account:   acc.Account,
			FromShard: acc.CurrentShard,
			ToShard:   acc.CurrentShard,
			Utility:   math.Inf(-1),
		}

		for target := 0; target < snapshot.ShardCount; target++ {
			toShard := uint64(target)
			if toShard == acc.CurrentShard {
				continue
			}

			gainCross := acc.CrossContribution
			gainBalance := math.Max(0, loadByShard[acc.CurrentShard]-loadByShard[toShard]) * 0.5
			costMig := acc.MigrationCost
			costRisk := math.Max(0, riskByShard[toShard]+acc.RiskScore-1.0)

			utility :=
				cfg.GammaCross*gainCross +
					cfg.GammaBalance*gainBalance -
					cfg.GammaMig*costMig -
					cfg.GammaRisk*costRisk

			if utility > best.Utility {
				best = CandidateMove{
					Account:     acc.Account,
					FromShard:   acc.CurrentShard,
					ToShard:     toShard,
					Utility:     utility,
					GainCross:   gainCross,
					GainBalance: gainBalance,
					CostMig:     costMig,
					CostRisk:    costRisk,
				}
			}
		}

		if best.Utility > 0 {
			proposed = append(proposed, best)
		}
	}

	sort.Slice(proposed, func(i, j int) bool {
		return proposed[i].Utility > proposed[j].Utility
	})

	if cfg.MaxMoves > 0 && len(proposed) > cfg.MaxMoves {
		proposed = proposed[:cfg.MaxMoves]
	}

	return proposed
}
