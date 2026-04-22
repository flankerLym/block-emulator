package pbft_all

import (
	"blockEmulator/message"
	"bytes"
	"encoding/base64"
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

	BuildChunkProof(commitment, hash string, idx, total uint64, siblings []string) (proofSystem string, proof string)
	VerifyChunkProof(proofSystem, verifierKeyID, commitment, hash, proof string, idx, total uint64) bool

	BuildRetirementProof(rp *message.RetirementProof) (proofSystem string, verifierKeyID string, proofBytes []byte, proofDigest string, proofMode string)
	VerifyRetirementProof(rp *message.RetirementProof) bool
}

type backendDispatch struct{}

func (b *backendDispatch) BuildRetirementProof(rp *message.RetirementProof) (proofSystem string, verifierKeyID string, proofBytes []byte, proofDigest string, proofMode string) {
	//TODO implement me
	panic("implement me")
}

func (b *backendDispatch) VerifyRetirementProof(rp *message.RetirementProof) bool {
	//TODO implement me
	panic("implement me")
}

type externalRVCProofRequest struct {
	ProtocolVersion       string   `json:"protocol_version"`
	CircuitVersion        string   `json:"circuit_version"`
	VerifierKeyID         string   `json:"verifier_key_id"`
	CertificateID         string   `json:"certificate_id"`
	EpochTag              uint64   `json:"epoch_tag"`
	FromShard             uint64   `json:"from_shard"`
	ToShard               uint64   `json:"to_shard"`
	BatchSize             uint64   `json:"batch_size"`
	WitnessBundleHash     string   `json:"witness_bundle_hash"`
	WitnessBundleBinding  string   `json:"witness_bundle_binding"`
	CertificateBinding    string   `json:"certificate_binding"`
	SemanticWitnessDigest string   `json:"semantic_witness_digest"`
	PublicInputs          []string `json:"public_inputs"`
	WitnessBundleB64      string   `json:"witness_bundle_b64,omitempty"`

	ProofSystem   string `json:"proof_system,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
}

type externalChunkProofRequest struct {
	ProtocolVersion string   `json:"protocol_version"`
	CircuitVersion  string   `json:"circuit_version"`
	VerifierKeyID   string   `json:"verifier_key_id"`
	Commitment      string   `json:"commitment"`
	Hash            string   `json:"hash"`
	Index           uint64   `json:"index"`
	Total           uint64   `json:"total"`
	Siblings        []string `json:"siblings,omitempty"`

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
	cmd.Env = os.Environ()
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
		SemanticWitnesses []*message.RVCSemanticWitness  `json:"semantic_witnesses"`
	}{
		ProtocolVersion:   rvc.ProtocolVersion,
		CircuitVersion:    rvc.CircuitVersion,
		CertificateID:     rvc.CertificateID,
		WitnessBundleHash: rvc.WitnessBundleHash,
		StateWitnesses:    rvc.StateWitnesses,
		SemanticWitnesses: rvc.SemanticWitnesses,
	})
	return base64.StdEncoding.EncodeToString(raw)
}

func (b *backendDispatch) BuildRVCProof(rvc *message.ReshardingValidityCertificate) (string, string, []byte, string, string) {
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_PROVER"))
	if command == "" {
		command = defaultExternalCommand("rvc_prover")
	}
	resp := new(externalProofResponse)
	req := &externalRVCProofRequest{
		ProtocolVersion:       rvc.ProtocolVersion,
		CircuitVersion:        rvc.CircuitVersion,
		VerifierKeyID:         rvc.VerifierKeyID,
		CertificateID:         rvc.CertificateID,
		EpochTag:              rvc.EpochTag,
		FromShard:             rvc.FromShard,
		ToShard:               rvc.ToShard,
		BatchSize:             rvc.BatchSize,
		WitnessBundleHash:     rvc.WitnessBundleHash,
		WitnessBundleBinding:  buildWitnessBundleBinding(rvc.WitnessBundleHash),
		CertificateBinding:    buildCertificateBinding(rvc.CertificateID),
		SemanticWitnessDigest: rvc.SemanticWitnessDigest,
		PublicInputs:          rvc.PublicInputs,
		WitnessBundleB64:      witnessBundleB64(rvc),
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return "", rvc.VerifierKeyID, nil, "", "external-error"
	}
	proofBytes, _ := base64.StdEncoding.DecodeString(resp.ProofBytesB64)
	proofMode := resp.ProofMode
	if proofMode == "" {
		proofMode = "external-groth16-strict-v1"
	}
	return resp.ProofSystem, resp.VerifierKeyID, proofBytes, resp.ProofDigest, proofMode
}

func (b *backendDispatch) VerifyRVCProof(rvc *message.ReshardingValidityCertificate) bool {
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_VERIFIER"))
	if command == "" {
		command = defaultExternalCommand("rvc_verifier")
	}
	resp := new(externalProofResponse)
	req := &externalRVCProofRequest{
		ProtocolVersion:       rvc.ProtocolVersion,
		CircuitVersion:        rvc.CircuitVersion,
		VerifierKeyID:         rvc.VerifierKeyID,
		CertificateID:         rvc.CertificateID,
		EpochTag:              rvc.EpochTag,
		FromShard:             rvc.FromShard,
		ToShard:               rvc.ToShard,
		BatchSize:             rvc.BatchSize,
		WitnessBundleHash:     rvc.WitnessBundleHash,
		WitnessBundleBinding:  buildWitnessBundleBinding(rvc.WitnessBundleHash),
		CertificateBinding:    buildCertificateBinding(rvc.CertificateID),
		SemanticWitnessDigest: rvc.SemanticWitnessDigest,
		PublicInputs:          rvc.PublicInputs,
		ProofSystem:           rvc.ProofSystem,
		ProofDigest:           rvc.ProofDigest,
		ProofMode:             rvc.ProofMode,
		ProofBytesB64:         base64.StdEncoding.EncodeToString(rvc.ProofBytes),
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return false
	}
	return resp.Valid
}

func (b *backendDispatch) BuildChunkProof(commitment, hash string, idx, total uint64, siblings []string) (string, string) {
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_PROVER"))
	if command == "" {
		command = defaultExternalCommand("chunk_prover")
	}
	resp := new(externalProofResponse)
	req := &externalChunkProofRequest{
		ProtocolVersion: "zkscar-chunk-v2",
		CircuitVersion:  "chunk-membership-groth16-v1",
		VerifierKeyID:   "zkscar-chunk-groth16-v1",
		Commitment:      commitment,
		Hash:            hash,
		Index:           idx,
		Total:           total,
		Siblings:        siblings,
	}
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return "", ""
	}
	return resp.ProofSystem, resp.Proof
}

func (b *backendDispatch) VerifyChunkProof(proofSystem, verifierKeyID, commitment, hash, proof string, idx, total uint64) bool {
	if verifierKeyID == "" {
		verifierKeyID = "zkscar-chunk-groth16-v1"
	}
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_CHUNK_VERIFIER"))
	if command == "" {
		command = defaultExternalCommand("chunk_verifier")
	}
	resp := new(externalProofResponse)
	req := &externalChunkProofRequest{
		ProtocolVersion: "zkscar-chunk-v2",
		CircuitVersion:  "chunk-membership-groth16-v1",
		VerifierKeyID:   verifierKeyID,
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

var zkBackend ZKBackend = &backendDispatch{}

func expectedStrictRVCPublicInputs(rvc *message.ReshardingValidityCertificate) []string {
	return []string{
		strconv.FormatUint(rvc.EpochTag, 10),
		strconv.FormatUint(rvc.FromShard, 10),
		strconv.FormatUint(rvc.ToShard, 10),
		strconv.FormatUint(rvc.BatchSize, 10),
		rvc.SemanticWitnessDigest,
		buildWitnessBundleBinding(rvc.WitnessBundleHash),
		buildCertificateBinding(rvc.CertificateID),
	}
}

type externalRetirementProofRequest struct {
	ProtocolVersion         string   `json:"protocol_version"`
	CircuitVersion          string   `json:"circuit_version"`
	VerifierKeyID           string   `json:"verifier_key_id"`
	Addr                    string   `json:"addr"`
	EpochTag                uint64   `json:"epoch_tag"`
	FromShard               uint64   `json:"from_shard"`
	ToShard                 uint64   `json:"to_shard"`
	HydratedFlag            uint64   `json:"hydrated_flag"`
	DebtRootClearedFlag     uint64   `json:"debt_root_cleared_flag"`
	SettledReceiptCount     uint64   `json:"settled_receipt_count"`
	OutstandingReceiptCount uint64   `json:"outstanding_receipt_count"`
	PostCutoverWriteCount   uint64   `json:"post_cutover_write_count"`
	DebtWitnessDigest       string   `json:"debt_witness_digest"`
	NoWriteWitnessDigest    string   `json:"no_write_witness_digest"`
	RetirementWitnessDigest string   `json:"retirement_witness_digest"`
	RVCBinding              string   `json:"rvc_binding"`
	PublicInputs            []string `json:"public_inputs"`

	ProofSystem   string `json:"proof_system,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
}

func expectedStrictRetirementPublicInputs(rp *message.RetirementProof) []string {
	hydratedFlag := "0"
	if rp.Hydrated {
		hydratedFlag = "1"
	}
	debtClearedFlag := "0"
	if rp.DebtRootCleared {
		debtClearedFlag = "1"
	}
	return []string{
		strconv.FormatUint(rp.EpochTag, 10),
		strconv.FormatUint(rp.FromShard, 10),
		strconv.FormatUint(rp.ToShard, 10),
		hydratedFlag,
		debtClearedFlag,
		strconv.FormatUint(rp.SettledReceiptCount, 10),
		strconv.FormatUint(rp.OutstandingReceiptCount, 10),
		strconv.FormatUint(rp.PostCutoverWriteCount, 10),
		rp.DebtWitnessDigest,
		rp.NoWriteWitnessDigest,
		rp.RetirementWitnessDigest,
		buildCertificateBinding(rp.RVCID),
	}
}
