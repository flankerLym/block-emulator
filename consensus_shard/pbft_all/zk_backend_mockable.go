package pbft_all

import (
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

type ZKBackend interface {
	BuildRVCProof(publicInputs []string) (proofSystem string, verifierKeyID string, proofBytes []byte, proofDigest string, proofMode string)
	VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool

	BuildChunkProof(commitment, hash string, idx, total uint64) (proofSystem string, proof string)
	VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool
}

type MockZKBackend struct{}

func buildBackendDigest(parts []string) string {
	h := sha256.Sum256([]byte(stringsJoin(parts)))
	return hex.EncodeToString(h[:])
}

func (m *MockZKBackend) BuildRVCProof(publicInputs []string) (string, string, []byte, string, string) {
	proofBytes := []byte(buildBackendDigest(append([]string{"mock-proof"}, publicInputs...)))
	proofDigest := buildBackendDigest([]string{
		"mock-groth16",
		"zkscar-vk-v2",
		stringsJoin(publicInputs),
		hex.EncodeToString(proofBytes),
	})
	return "mock-groth16", "zkscar-vk-v2", proofBytes, proofDigest, "mock"
}

func (m *MockZKBackend) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	if rvc == nil {
		return false
	}
	if rvc.ProofSystem == "" || rvc.VerifierKeyID == "" {
		return false
	}
	expectedDigest := buildBackendDigest([]string{
		rvc.ProofSystem,
		rvc.VerifierKeyID,
		stringsJoin(rvc.PublicInputs),
		hex.EncodeToString(rvc.ProofBytes),
	})
	return expectedDigest == rvc.ProofDigest
}

func (m *MockZKBackend) BuildChunkProof(commitment, hash string, idx, total uint64) (string, string) {
	proof := buildBackendDigest([]string{
		commitment,
		hash,
		strconv.FormatUint(idx, 10),
		strconv.FormatUint(total, 10),
	})
	return "mock-merkle", proof
}

func (m *MockZKBackend) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	if proofSystem != "mock-merkle" {
		return false
	}
	expected := buildBackendDigest([]string{
		commitment,
		hash,
		strconv.FormatUint(idx, 10),
		strconv.FormatUint(total, 10),
	})
	return expected == proof
}

var zkBackend ZKBackend = &MockZKBackend{}
