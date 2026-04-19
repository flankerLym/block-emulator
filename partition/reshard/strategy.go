package reshard

type Strategy interface {
	Name() string
	BuildPlan(snapshot SystemSnapshot, cfg ReshardConfig) ReshardPlan
}

type Verifier interface {
	Name() string
	Verify(snapshot SystemSnapshot, plan *ReshardPlan, cfg ReshardConfig) bool
}
