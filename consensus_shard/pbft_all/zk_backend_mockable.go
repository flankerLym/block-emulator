package pbft_all

import (
	"blockEmulator/message"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

type ZKBackend interface {
	BuildRVCProof(rvc *message.ReshardingValidityCertificate) (proofSystem string, verifierKeyID string, proofBytes []byte, proofDigest string, proofMode string)
	VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool

	BuildChunkProof(commitment, hash string, idx, total uint64) (proofSystem string, proof string)
	VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool
}

type MockZKBackend struct{}

func buildBackendDigest(parts []string) string {
	h := sha256.Sum256([]byte(stringsJoin(parts)))
	return hex.EncodeToString(h[:])
}

func (m *MockZKBackend) BuildRVCProof(rvc *message.ReshardingValidityCertificate) (string, string, []byte, string, string) {
	proofBytes := []byte(buildBackendDigest(append([]string{"mock-proof"}, rvc.PublicInputs...)))
	proofDigest := buildBackendDigest([]string{
		"mock-groth16",
		rvc.VerifierKeyID,
		stringsJoin(rvc.PublicInputs),
		rvc.WitnessBundleHash,
		hex.EncodeToString(proofBytes),
	})
	return "mock-groth16", rvc.VerifierKeyID, proofBytes, proofDigest, "legacy-mock"
}

func (m *MockZKBackend) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	if rvc == nil || rvc.ProofSystem == "" || rvc.VerifierKeyID == "" {
		return false
	}
	expectedDigest := buildBackendDigest([]string{
		rvc.ProofSystem,
		rvc.VerifierKeyID,
		stringsJoin(rvc.PublicInputs),
		rvc.WitnessBundleHash,
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

type externalRVCProofRequest struct {
	ProtocolVersion   string   `json:"protocol_version"`
	CircuitVersion    string   `json:"circuit_version"`
	VerifierKeyID     string   `json:"verifier_key_id"`
	PublicInputs      []string `json:"public_inputs"`
	WitnessBundleHash string   `json:"witness_bundle_hash"`
	WitnessBundleB64  string   `json:"witness_bundle_b64,omitempty"`

	ProofSystem   string `json:"proof_system,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
}

type externalChunkProofRequest struct {
	ProtocolVersion string `json:"protocol_version"`
	VerifierKeyID   string `json:"verifier_key_id"`
	Commitment      string `json:"commitment"`
	Hash            string `json:"hash"`
	Index           uint64 `json:"index"`
	Total           uint64 `json:"total"`

	ProofSystem string `json:"proof_system,omitempty"`
	Proof       string `json:"proof,omitempty"`
}

type externalProofResponse struct {
	OK bool `json:"ok"`

	ProofSystem   string `json:"proof_system,omitempty"`
	VerifierKeyID string `json:"verifier_key_id,omitempty"`
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
		return "external"
	}
	switch mode {
	case "external", "legacy-mock":
		return mode
	default:
		return "external"
	}
}

func splitCommandLine(command string) []string {
	parts := strings.Fields(strings.TrimSpace(command))
	if len(parts) == 0 {
		return nil
	}
	return parts
}

func defaultExternalCommand(kind string) string {
	pythonBin := "python3"
	if runtime.GOOS == "windows" {
		pythonBin = "python"
	}
	switch kind {
	case "rvc_prover":
		return fmt.Sprintf("%s tools/zkscar_backend/rvc_prover.py", pythonBin)
	case "rvc_verifier":
		return fmt.Sprintf("%s tools/zkscar_backend/rvc_verifier.py", pythonBin)
	case "chunk_prover":
		return fmt.Sprintf("%s tools/zkscar_backend/chunk_prover.py", pythonBin)
	case "chunk_verifier":
		return fmt.Sprintf("%s tools/zkscar_backend/chunk_verifier.py", pythonBin)
	default:
		return ""
	}
}

func runExternalBackend(command string, payload any, out any) error {
	parts := splitCommandLine(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty external backend command")
	}
	cmd := exec.Command(parts[0], parts[1:]...)
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
			return fmt.Errorf("external backend failed: %s", strings.TrimSpace(stderr.String()))
		}
		return err
	}
	return json.NewDecoder(&stdout).Decode(out)
}

func witnessBundleB64(rvc *message.ReshardingValidityCertificate) string {
	raw, _ := json.Marshal(struct {
		ProtocolVersion   string                         `json:"protocol_version"`
		CircuitVersion    string                         `json:"circuit_version"`
		CertificateID     string                         `json:"certificate_id"`
		WitnessBundleHash string                         `json:"witness_bundle_hash"`
		StateWitnesses    []*message.AccountStateWitness `json:"state_witnesses"`
	}{
		ProtocolVersion:   rvc.ProtocolVersion,
		CircuitVersion:    rvc.CircuitVersion,
		CertificateID:     rvc.CertificateID,
		WitnessBundleHash: rvc.WitnessBundleHash,
		StateWitnesses:    rvc.StateWitnesses,
	})
	return base64.StdEncoding.EncodeToString(raw)
}

func (b *backendDispatch) BuildRVCProof(rvc *message.ReshardingValidityCertificate) (string, string, []byte, string, string) {
	if backendMode() == "legacy-mock" {
		return b.mock.BuildRVCProof(rvc)
	}
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_PROVER"))
	if command == "" {
		command = defaultExternalCommand("rvc_prover")
	}
	resp := new(externalProofResponse)
	req := &externalRVCProofRequest{
		ProtocolVersion:   rvc.ProtocolVersion,
		CircuitVersion:    rvc.CircuitVersion,
		VerifierKeyID:     rvc.VerifierKeyID,
		PublicInputs:      rvc.PublicInputs,
		WitnessBundleHash: rvc.WitnessBundleHash,
		WitnessBundleB64:  witnessBundleB64(rvc),
	}
	if err := runExternalBackend(command, req, resp); err == nil && resp.OK {
		proofBytes, _ := base64.StdEncoding.DecodeString(resp.ProofBytesB64)
		proofMode := resp.ProofMode
		if proofMode == "" {
			proofMode = "external-strict"
		}
		return resp.ProofSystem, resp.VerifierKeyID, proofBytes, resp.ProofDigest, proofMode
	}
	return "", rvc.VerifierKeyID, nil, "", "external-error"
}

func (b *backendDispatch) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	if backendMode() == "legacy-mock" {
		return b.mock.VerifyRVCProof(rvc)
	}
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_VERIFIER"))
	if command == "" {
		command = defaultExternalCommand("rvc_verifier")
	}
	resp := new(externalProofResponse)
	req := &externalRVCProofRequest{
		ProtocolVersion:   rvc.ProtocolVersion,
		CircuitVersion:    rvc.CircuitVersion,
		VerifierKeyID:     rvc.VerifierKeyID,
		PublicInputs:      rvc.PublicInputs,
		WitnessBundleHash: rvc.WitnessBundleHash,
		ProofSystem:       rvc.ProofSystem,
		ProofDigest:       rvc.ProofDigest,
		ProofMode:         rvc.ProofMode,
		ProofBytesB64:     base64.StdEncoding.EncodeToString(rvc.ProofBytes),
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return false
	}
	return resp.Valid
}

func (b *backendDispatch) BuildChunkProof(commitment, hash string, idx, total uint64) (string, string) {
	if backendMode() == "legacy-mock" {
		return b.mock.BuildChunkProof(commitment, hash, idx, total)
	}
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_PROVER"))
	if command == "" {
		command = defaultExternalCommand("chunk_prover")
	}
	resp := new(externalProofResponse)
	req := &externalChunkProofRequest{
		ProtocolVersion: "zkscar-chunk-v1",
		VerifierKeyID:   "zkscar-chunk-v1",
		Commitment:      commitment,
		Hash:            hash,
		Index:           idx,
		Total:           total,
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return "", ""
	}
	return resp.ProofSystem, resp.Proof
}

func (b *backendDispatch) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	if backendMode() == "legacy-mock" {
		return b.mock.VerifyChunkProof(proofSystem, commitment, hash, proof, idx, total)
	}
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_VERIFIER"))
	if command == "" {
		command = defaultExternalCommand("chunk_verifier")
	}
	resp := new(externalProofResponse)
	req := &externalChunkProofRequest{
		ProtocolVersion: "zkscar-chunk-v1",
		VerifierKeyID:   "zkscar-chunk-v1",
		Commitment:      commitment,
		Hash:            hash,
		Index:           idx,
		Total:           total,
		ProofSystem:     proofSystem,
		Proof:           proof,
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return false
	}
	return resp.Valid
}

var zkBackend ZKBackend = &backendDispatch{mock: &MockZKBackend{}}
