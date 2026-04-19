package reshard

import "fmt"

func BuildStrategy(cfg ReshardConfig) (Strategy, error) {
	switch cfg.Strategy {
	case StrategyLegacy:
		return &LegacyPassthroughStrategy{}, nil
	case StrategyERM:
		return &ERMStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown reshard strategy: %s", cfg.Strategy)
	}
}

func BuildVerifier(cfg ReshardConfig) Verifier {
	switch cfg.Verifier {
	case VerifierNone:
		return &NoopVerifier{}
	case VerifierFull, VerifierBatch, VerifierSample:
		return NewLightVerifier(cfg.Verifier)
	default:
		return &NoopVerifier{}
	}
}

type LegacyPassthroughStrategy struct{}

func (s *LegacyPassthroughStrategy) Name() string {
	return "legacy-passthrough"
}

func (s *LegacyPassthroughStrategy) BuildPlan(snapshot SystemSnapshot, cfg ReshardConfig) ReshardPlan {
	return ReshardPlan{
		Triggered:      false,
		TriggerScore:   0,
		Reason:         "legacy strategy selected; use original reshard path",
		VerifierMode:   cfg.Verifier,
		VerificationOK: true,
	}
}
