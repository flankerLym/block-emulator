package pbft_all

import (
	"blockEmulator/message"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

type backendDispatch struct {
	mock *MockZKBackend
}

type externalProofRequest struct {
	PublicInputs []string `json:"public_inputs,omitempty"`

	Commitment string `json:"commitment,omitempty"`
	Hash       string `json:"hash,omitempty"`
	Index      uint64 `json:"index,omitempty"`
	Total      uint64 `json:"total,omitempty"`

	ProofSystem   string `json:"proof_system,omitempty"`
	Proof         string `json:"proof,omitempty"`
	VerifierKey   string `json:"verifier_key_id,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
}

type externalProofResponse struct {
	OK bool `json:"ok"`

	ProofSystem   string `json:"proof_system,omitempty"`
	VerifierKey   string `json:"verifier_key_id,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`

	Proof string `json:"proof,omitempty"`
	Valid bool   `json:"valid,omitempty"`
	Error string `json:"error,omitempty"`
}

func backendMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("ZKSCAR_PROOF_BACKEND")))
	if mode == "" {
		return "mock"
	}
	switch mode {
	case "mock", "external":
		return mode
	default:
		return "mock"
	}
}

func runExternalBackend(command string, payload any, out any) error {
	cmd := exec.Command(command)
	var stdin bytes.Buffer
	if err := json.NewEncoder(&stdin).Encode(payload); err != nil {
		return err
	}
	cmd.Stdin = &stdin
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return exec.ErrNotFound
		}
		return err
	}
	return json.NewDecoder(&stdout).Decode(out)
}

func (b *backendDispatch) BuildRVCProof(publicInputs []string) (string, string, []byte, string, string) {
	if backendMode() == "external" {
		command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_PROVER"))
		if command != "" {
			req := &externalProofRequest{PublicInputs: publicInputs}
			resp := new(externalProofResponse)
			if err := runExternalBackend(command, req, resp); err == nil && resp.OK {
				proofBytes, _ := base64.StdEncoding.DecodeString(resp.ProofBytesB64)
				proofMode := resp.ProofMode
				if proofMode == "" {
					proofMode = "external"
				}
				return resp.ProofSystem, resp.VerifierKey, proofBytes, resp.ProofDigest, proofMode
			}
		}
		ps, vk, pb, pd, _ := b.mock.BuildRVCProof(publicInputs)
		return ps, vk, pb, pd, "external-fallback-mock"
	}
	return b.mock.BuildRVCProof(publicInputs)
}

func (b *backendDispatch) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	if backendMode() == "external" {
		command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_VERIFIER"))
		if command != "" {
			req := &externalProofRequest{
				PublicInputs:  rvc.PublicInputs,
				ProofSystem:   rvc.ProofSystem,
				VerifierKey:   rvc.VerifierKeyID,
				ProofDigest:   rvc.ProofDigest,
				ProofMode:     rvc.ProofMode,
				ProofBytesB64: base64.StdEncoding.EncodeToString(rvc.ProofBytes),
			}
			resp := new(externalProofResponse)
			if err := runExternalBackend(command, req, resp); err == nil && resp.OK {
				return resp.Valid
			}
		}
	}
	return b.mock.VerifyRVCProof(rvc)
}

func (b *backendDispatch) BuildChunkProof(commitment, hash string, idx, total uint64) (string, string) {
	if backendMode() == "external" {
		command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_PROVER"))
		if command != "" {
			req := &externalProofRequest{
				Commitment: commitment,
				Hash:       hash,
				Index:      idx,
				Total:      total,
			}
			resp := new(externalProofResponse)
			if err := runExternalBackend(command, req, resp); err == nil && resp.OK {
				mode := resp.ProofSystem
				if mode == "" {
					mode = "external-merkle"
				}
				return mode, resp.Proof
			}
		}
	}
	return b.mock.BuildChunkProof(commitment, hash, idx, total)
}

func (b *backendDispatch) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	if backendMode() == "external" {
		command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_VERIFIER"))
		if command != "" {
			req := &externalProofRequest{
				Commitment:  commitment,
				Hash:        hash,
				Index:       idx,
				Total:       total,
				ProofSystem: proofSystem,
				Proof:       proof,
			}
			resp := new(externalProofResponse)
			if err := runExternalBackend(command, req, resp); err == nil && resp.OK {
				return resp.Valid
			}
		}
	}
	return b.mock.VerifyChunkProof(proofSystem, commitment, hash, proof, idx, total)
}

var zkBackend ZKBackend = &backendDispatch{mock: &MockZKBackend{}}
