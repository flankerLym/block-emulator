package reshard

import "fmt"

type NoopVerifier struct{}

func (v *NoopVerifier) Name() string {
	return "noop"
}

func (v *NoopVerifier) Verify(snapshot SystemSnapshot, plan *ReshardPlan, cfg ReshardConfig) bool {
	plan.VerificationOK = true
	plan.VerificationMsg = "verifier disabled"
	return true
}

type LightVerifier struct {
	mode VerifierMode
}

func NewLightVerifier(mode VerifierMode) *LightVerifier {
	return &LightVerifier{mode: mode}
}

func (v *LightVerifier) Name() string {
	return string(v.mode)
}

func (v *LightVerifier) Verify(snapshot SystemSnapshot, plan *ReshardPlan, cfg ReshardConfig) bool {
	if !plan.Triggered {
		plan.VerificationOK = true
		plan.VerificationMsg = "not triggered; nothing to verify"
		return true
	}

	switch v.mode {
	case VerifierFull:
		return verifyFull(snapshot, plan)
	case VerifierBatch:
		return verifyBatch(snapshot, plan)
	case VerifierSample:
		return verifySample(snapshot, plan, cfg)
	default:
		plan.VerificationOK = true
		plan.VerificationMsg = "unknown verifier mode -> skip"
		return true
	}
}

func verifyFull(snapshot SystemSnapshot, plan *ReshardPlan) bool {
	seen := map[string]bool{}
	for _, mv := range plan.SelectedMoves {
		if seen[mv.Account] {
			plan.VerificationOK = false
			plan.VerificationMsg = "duplicate account move found"
			return false
		}
		seen[mv.Account] = true
		if mv.FromShard == mv.ToShard {
			plan.VerificationOK = false
			plan.VerificationMsg = "illegal move: source shard equals target shard"
			return false
		}
	}
	plan.VerificationOK = true
	plan.VerificationMsg = "full verification passed"
	return true
}

func verifyBatch(snapshot SystemSnapshot, plan *ReshardPlan) bool {
	if len(plan.SelectedMoves) == 0 {
		plan.VerificationOK = true
		plan.VerificationMsg = "batch verification passed (empty moves)"
		return true
	}
	set := map[string]struct{}{}
	for _, mv := range plan.SelectedMoves {
		if _, ok := set[mv.Account]; ok {
			plan.VerificationOK = false
			plan.VerificationMsg = "batch verification failed: duplicate account"
			return false
		}
		set[mv.Account] = struct{}{}
	}
	if plan.OldCommitment == "" || plan.NewCommitment == "" {
		plan.VerificationOK = false
		plan.VerificationMsg = "batch verification failed: missing commitment"
		return false
	}
	plan.VerificationOK = true
	plan.VerificationMsg = "batch verification passed"
	return true
}

func verifySample(snapshot SystemSnapshot, plan *ReshardPlan, cfg ReshardConfig) bool {
	if len(plan.SelectedMoves) == 0 {
		plan.VerificationOK = true
		plan.VerificationMsg = "sample verification passed (empty moves)"
		return true
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 0.2
	}
	sampleN := int(float64(len(plan.SelectedMoves)) * cfg.SampleRatio)
	if sampleN < 1 {
		sampleN = 1
	}

	for i := 0; i < sampleN && i < len(plan.SelectedMoves); i++ {
		mv := plan.SelectedMoves[i]
		if mv.FromShard == mv.ToShard {
			plan.VerificationOK = false
			plan.VerificationMsg = fmt.Sprintf("sample verification failed at account=%s", mv.Account)
			return false
		}
	}
	plan.VerificationOK = true
	plan.VerificationMsg = "sample verification passed"
	return true
}
