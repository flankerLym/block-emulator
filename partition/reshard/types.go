package reshard

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"time"
)

type VerifierMode string
type StrategyMode string

const (
	VerifierNone   VerifierMode = "none"
	VerifierFull   VerifierMode = "full"
	VerifierBatch  VerifierMode = "batch"
	VerifierSample VerifierMode = "sample"

	StrategyLegacy StrategyMode = "legacy"
	StrategyERM    StrategyMode = "erm"
)

type ReshardConfig struct {
	Enabled bool `json:"enabled"`

	Strategy StrategyMode `json:"strategy"`
	Verifier VerifierMode `json:"verifier"`

	WindowSize int `json:"window_size"`

	TriggerThreshold float64 `json:"trigger_threshold"`

	AlphaLoad  float64 `json:"alpha_load"`
	AlphaCross float64 `json:"alpha_cross"`
	AlphaHot   float64 `json:"alpha_hot"`
	AlphaRisk  float64 `json:"alpha_risk"`

	OmegaCross     float64 `json:"omega_cross"`
	OmegaBalance   float64 `json:"omega_balance"`
	OmegaMigration float64 `json:"omega_migration"`
	OmegaRisk      float64 `json:"omega_risk"`

	GammaCross   float64 `json:"gamma_cross"`
	GammaBalance float64 `json:"gamma_balance"`
	GammaMig     float64 `json:"gamma_mig"`
	GammaRisk    float64 `json:"gamma_risk"`

	HotThreshold  float64 `json:"hot_threshold"`
	MaxCandidates int     `json:"max_candidates"`
	MaxMoves      int     `json:"max_moves"`

	SampleRatio float64 `json:"sample_ratio"`
	RandomSeed  int64   `json:"random_seed"`
}

type ShardMetrics struct {
	ShardID        uint64
	Load           float64
	QueueLen       float64
	CrossTxRate    float64
	HotspotScore   float64
	RiskScore      float64
	LatencyScore   float64
	LastUpdateUnix int64
}

type AccountStat struct {
	Account           string
	CurrentShard      uint64
	TxFreq            float64
	TxVolume          float64
	RecentActivity    float64
	CrossContribution float64
	Degree            float64
	Burst             float64
	RiskScore         float64
	MigrationCost     float64
}

type SystemSnapshot struct {
	Now          time.Time
	ShardMetrics []ShardMetrics
	Accounts     []AccountStat
	ShardCount   int
}

type CandidateMove struct {
	Account     string
	FromShard   uint64
	ToShard     uint64
	Utility     float64
	GainCross   float64
	GainBalance float64
	CostMig     float64
	CostRisk    float64
}

type ReshardPlan struct {
	Triggered       bool
	TriggerScore    float64
	Reason          string
	SelectedMoves   []CandidateMove
	OldCommitment   string
	NewCommitment   string
	VerifierMode    VerifierMode
	VerificationOK  bool
	VerificationMsg string
}

func CV(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	if mean == 0 {
		return 0
	}
	var ss float64
	for _, v := range vals {
		d := v - mean
		ss += d * d
	}
	std := math.Sqrt(ss / float64(len(vals)))
	return std / mean
}

func Average(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func TopNAccountsByScore(accounts []AccountStat, scoreFn func(AccountStat) float64, n int) []AccountStat {
	cp := make([]AccountStat, len(accounts))
	copy(cp, accounts)
	sort.Slice(cp, func(i, j int) bool {
		return scoreFn(cp[i]) > scoreFn(cp[j])
	})
	if n > len(cp) {
		n = len(cp)
	}
	return cp[:n]
}

func BuildCommitment(snapshot SystemSnapshot, moves []CandidateMove) string {
	h := sha256.New()
	h.Write([]byte(snapshot.Now.UTC().Format(time.RFC3339Nano)))
	for _, s := range snapshot.ShardMetrics {
		h.Write([]byte(
			strconv.FormatUint(s.ShardID, 10) +
				formatFloat(s.Load) +
				formatFloat(s.QueueLen) +
				formatFloat(s.CrossTxRate) +
				formatFloat(s.HotspotScore) +
				formatFloat(s.RiskScore) +
				formatFloat(s.LatencyScore),
		))
	}
	for _, m := range moves {
		h.Write([]byte(m.Account))
		h.Write([]byte(strconv.FormatUint(m.FromShard, 10)))
		h.Write([]byte(strconv.FormatUint(m.ToShard, 10)))
		h.Write([]byte(formatFloat(m.Utility)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}
