package measure

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/message"
)

type TestModule_ZKSCARProof struct{}

func NewTestModule_ZKSCARProof() *TestModule_ZKSCARProof {
	return &TestModule_ZKSCARProof{}
}

func (tm *TestModule_ZKSCARProof) UpdateMeasureRecord(*message.BlockInfoMsg) {}

func (tm *TestModule_ZKSCARProof) HandleExtraMessage([]byte) {}

func (tm *TestModule_ZKSCARProof) OutputMetricName() string {
	return "ZKSCAR_Proof_Backend"
}

func (tm *TestModule_ZKSCARProof) OutputRecord() ([]float64, float64) {
	s := pbft_all.GetZKSCARMetricsSnapshot()
	records := []float64{
		float64(s.RVCProofCount),
		float64(s.RVCVerifyCount),
		float64(s.ChunkProofCount),
		float64(s.ChunkVerifyCount),
		float64(s.RetirementProofCount),
		float64(s.RetirementVerifyCount),
		float64(s.RVCProofMicros),
		float64(s.RVCVerifyMicros),
		float64(s.ChunkProofMicros),
		float64(s.ChunkVerifyMicros),
		float64(s.RetirementProofMicros),
		float64(s.RetirementVerifyMicros),
	}
	var total float64
	for _, v := range records {
		total += v
	}
	return records, total
}
