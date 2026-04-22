package pbft_all

import (
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type proofPayload struct {
	Kind   string   `json:"kind"`
	Inputs []string `json:"inputs"`
	Digest string   `json:"digest"`
}

type chunkProofPayload struct {
	Kind            string   `json:"kind"`
	StateCommitment string   `json:"state_commitment"`
	ChunkHash       string   `json:"chunk_hash"`
	ChunkIndex      uint64   `json:"chunk_index"`
	ChunkTotal      uint64   `json:"chunk_total"`
	SiblingPath     []string `json:"sibling_path"`
	Digest          string   `json:"digest"`
}

type retirementProofPayload struct {
	Kind   string   `json:"kind"`
	Inputs []string `json:"inputs"`
	Digest string   `json:"digest"`
}

type ZKSCARMetricsSnapshot struct {
	RVCProofCount         uint64
	RVCVerifyCount        uint64
	ChunkProofCount       uint64
	ChunkVerifyCount      uint64
	RetirementProofCount  uint64
	RetirementVerifyCount uint64

	RVCProofMicros         uint64
	RVCVerifyMicros        uint64
	ChunkProofMicros       uint64
	ChunkVerifyMicros      uint64
	RetirementProofMicros  uint64
	RetirementVerifyMicros uint64
}

type zkscarMetrics struct {
	rvcProofCount    uint64
	rvcVerifyCount   uint64
	chunkProofCount  uint64
	chunkVerifyCount uint64
	retProofCount    uint64
	retVerifyCount   uint64

	rvcProofMicros    uint64
	rvcVerifyMicros   uint64
	chunkProofMicros  uint64
	chunkVerifyMicros uint64
	retProofMicros    uint64
	retVerifyMicros   uint64
}

var globalZKSCARMetrics = new(zkscarMetrics)

func (m *zkscarMetrics) record(kind string, proof bool, dur time.Duration) {
	us := uint64(dur.Microseconds())
	switch {
	case kind == "rvc" && proof:
		atomic.AddUint64(&m.rvcProofCount, 1)
		atomic.AddUint64(&m.rvcProofMicros, us)
	case kind == "rvc" && !proof:
		atomic.AddUint64(&m.rvcVerifyCount, 1)
		atomic.AddUint64(&m.rvcVerifyMicros, us)
	case kind == "chunk" && proof:
		atomic.AddUint64(&m.chunkProofCount, 1)
		atomic.AddUint64(&m.chunkProofMicros, us)
	case kind == "chunk" && !proof:
		atomic.AddUint64(&m.chunkVerifyCount, 1)
		atomic.AddUint64(&m.chunkVerifyMicros, us)
	case kind == "retirement" && proof:
		atomic.AddUint64(&m.retProofCount, 1)
		atomic.AddUint64(&m.retProofMicros, us)
	case kind == "retirement" && !proof:
		atomic.AddUint64(&m.retVerifyCount, 1)
		atomic.AddUint64(&m.retVerifyMicros, us)
	}
}

func GetZKSCARMetricsSnapshot() ZKSCARMetricsSnapshot {
	return ZKSCARMetricsSnapshot{
		RVCProofCount:          atomic.LoadUint64(&globalZKSCARMetrics.rvcProofCount),
		RVCVerifyCount:         atomic.LoadUint64(&globalZKSCARMetrics.rvcVerifyCount),
		ChunkProofCount:        atomic.LoadUint64(&globalZKSCARMetrics.chunkProofCount),
		ChunkVerifyCount:       atomic.LoadUint64(&globalZKSCARMetrics.chunkVerifyCount),
		RetirementProofCount:   atomic.LoadUint64(&globalZKSCARMetrics.retProofCount),
		RetirementVerifyCount:  atomic.LoadUint64(&globalZKSCARMetrics.retVerifyCount),
		RVCProofMicros:         atomic.LoadUint64(&globalZKSCARMetrics.rvcProofMicros),
		RVCVerifyMicros:        atomic.LoadUint64(&globalZKSCARMetrics.rvcVerifyMicros),
		ChunkProofMicros:       atomic.LoadUint64(&globalZKSCARMetrics.chunkProofMicros),
		ChunkVerifyMicros:      atomic.LoadUint64(&globalZKSCARMetrics.chunkVerifyMicros),
		RetirementProofMicros:  atomic.LoadUint64(&globalZKSCARMetrics.retProofMicros),
		RetirementVerifyMicros: atomic.LoadUint64(&globalZKSCARMetrics.retVerifyMicros),
	}
}

type zkProofBackend struct {
	mu sync.Mutex
}

var zkBackend = &zkProofBackend{}

func stableDigest(parts []string) string {
	payload, _ := json.Marshal(parts)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (z *zkProofBackend) BuildRVCProof(rvc *message.ReshardingValidityCertificate) (string, string, []byte, string, string) {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("rvc", true, time.Since(start)) }()

	inputs := expectedStrictRVCPublicInputs(rvc)
	payload := proofPayload{
		Kind:   "rvc",
		Inputs: inputs,
		Digest: stableDigest(inputs),
	}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return "simulated-groth16", "zkscar-rvc-groth16-v1", b, hex.EncodeToString(sum[:]), "deterministic-simulator"
}

func (z *zkProofBackend) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("rvc", false, time.Since(start)) }()

	if rvc == nil || len(rvc.ProofBytes) == 0 || rvc.ProofDigest == "" {
		return false
	}
	sum := sha256.Sum256(rvc.ProofBytes)
	if hex.EncodeToString(sum[:]) != rvc.ProofDigest {
		return false
	}
	var payload proofPayload
	if err := json.Unmarshal(rvc.ProofBytes, &payload); err != nil {
		return false
	}
	expectedInputs := expectedStrictRVCPublicInputs(rvc)
	if payload.Kind != "rvc" {
		return false
	}
	if len(payload.Inputs) != len(expectedInputs) {
		return false
	}
	for i := range expectedInputs {
		if payload.Inputs[i] != expectedInputs[i] {
			return false
		}
	}
	return payload.Digest == stableDigest(expectedInputs)
}

func (z *zkProofBackend) BuildChunkProof(commitment, leafHash string, idx, total uint64, siblingPath []string) (string, string) {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("chunk", true, time.Since(start)) }()

	items := []string{commitment, leafHash, strconv.FormatUint(idx, 10), strconv.FormatUint(total, 10)}
	items = append(items, siblingPath...)
	payload := chunkProofPayload{
		Kind:            "chunk",
		StateCommitment: commitment,
		ChunkHash:       leafHash,
		ChunkIndex:      idx,
		ChunkTotal:      total,
		SiblingPath:     siblingPath,
		Digest:          stableDigest(items),
	}
	b, _ := json.Marshal(payload)
	return "simulated-groth16", string(b)
}

func (z *zkProofBackend) VerifyChunkProof(proofSystem, verifierKeyID, commitment, leafHash, proof string, idx, total uint64) bool {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("chunk", false, time.Since(start)) }()

	if proofSystem == "" || verifierKeyID == "" || proof == "" {
		return false
	}
	var payload chunkProofPayload
	if err := json.Unmarshal([]byte(proof), &payload); err != nil {
		return false
	}
	if payload.Kind != "chunk" {
		return false
	}
	if payload.StateCommitment != commitment || payload.ChunkHash != leafHash || payload.ChunkIndex != idx || payload.ChunkTotal != total {
		return false
	}
	items := []string{commitment, leafHash, strconv.FormatUint(idx, 10), strconv.FormatUint(total, 10)}
	items = append(items, payload.SiblingPath...)
	return payload.Digest == stableDigest(items)
}

func (z *zkProofBackend) BuildRetirementProof(proof *message.RetirementProof, bundle *retirementWitnessBundle) (string, string, []byte, string, string) {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("retirement", true, time.Since(start)) }()

	inputs := expectedRetirementPublicInputs(proof)
	payload := retirementProofPayload{
		Kind:   "retirement",
		Inputs: inputs,
		Digest: stableDigest(inputs),
	}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return "simulated-groth16", "zkscar-retirement-groth16-v1", b, hex.EncodeToString(sum[:]), "deterministic-simulator"
}

func (z *zkProofBackend) VerifyRetirementProof(proof *message.RetirementProof) bool {
	start := time.Now()
	defer func() { globalZKSCARMetrics.record("retirement", false, time.Since(start)) }()

	if proof == nil || len(proof.ProofBytes) == 0 || proof.ProofDigest == "" {
		return false
	}
	sum := sha256.Sum256(proof.ProofBytes)
	if hex.EncodeToString(sum[:]) != proof.ProofDigest {
		return false
	}
	var payload retirementProofPayload
	if err := json.Unmarshal(proof.ProofBytes, &payload); err != nil {
		return false
	}
	expectedInputs := expectedRetirementPublicInputs(proof)
	if payload.Kind != "retirement" {
		return false
	}
	if len(payload.Inputs) != len(expectedInputs) {
		return false
	}
	for i := range expectedInputs {
		if payload.Inputs[i] != expectedInputs[i] {
			return false
		}
	}
	return payload.Digest == stableDigest(expectedInputs)
}
