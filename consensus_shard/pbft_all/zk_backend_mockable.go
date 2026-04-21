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
	"strings"
)

type ZKBackend interface {
	BuildRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule, payload *rvcStateWitness) (proofSystem string, verifierKeyID string, proofBytes []byte, proofDigest string, proofMode string)
	VerifyRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule) bool

	BuildChunkProof(commitment string, leafHashes []string, idx uint64) (proofSystem string, proof string)
	VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool
}

type externalProofRequest struct {
	RVC          *message.ReshardingValidityCertificate `json:"rvc,omitempty"`
	Capsules     []message.ShadowCapsule                `json:"capsules,omitempty"`
	WitnessB64   string                                 `json:"witness_b64,omitempty"`
	PublicInputs []string                               `json:"public_inputs,omitempty"`

	Commitment string   `json:"commitment,omitempty"`
	LeafHashes []string `json:"leaf_hashes,omitempty"`
	Hash       string   `json:"hash,omitempty"`
	Index      uint64   `json:"index,omitempty"`
	Total      uint64   `json:"total,omitempty"`

	ProofSystem   string `json:"proof_system,omitempty"`
	Proof         string `json:"proof,omitempty"`
	VerifierKeyID string `json:"verifier_key_id,omitempty"`
	ProofDigest   string `json:"proof_digest,omitempty"`
	ProofMode     string `json:"proof_mode,omitempty"`
	ProofBytesB64 string `json:"proof_bytes_b64,omitempty"`
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

type chunkMerkleStep struct {
	SiblingHash string `json:"sibling_hash"`
	IsLeft      bool   `json:"is_left"`
}

type chunkMerkleProofEnvelope struct {
	Schema   string            `json:"schema"`
	Root     string            `json:"root"`
	LeafHash string            `json:"leaf_hash"`
	Index    uint64            `json:"index"`
	Total    uint64            `json:"total"`
	Steps    []chunkMerkleStep `json:"steps"`
}

type nativeStateProofBackend struct{}

type externalZKBackend struct{}

func buildBackendDigest(parts []string) string {
	h := sha256.Sum256([]byte(stringsJoin(parts)))
	return hex.EncodeToString(h[:])
}

func backendMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("ZKSCAR_PROOF_BACKEND")))
	if mode == "" {
		return "native"
	}
	switch mode {
	case "native", "external":
		return mode
	default:
		return "native"
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
		return err
	}
	return json.NewDecoder(&stdout).Decode(out)
}

func (n *nativeStateProofBackend) BuildRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule, payload *rvcStateWitness) (string, string, []byte, string, string) {
	if rvc == nil || payload == nil {
		return "", "", nil, "", ""
	}
	if !validateRVCStateWitness(rvc, caps, payload) {
		return "", "", nil, "", ""
	}
	env := map[string]any{
		"schema":         "zkscar-native-proof-v1",
		"witness_digest": witnessDigest(payload),
		"witness":        payload,
	}
	proofBytes, _ := json.Marshal(env)
	proofSystem := "native-state-proof-v1"
	verifierKeyID := "native-rvc-v1"
	proofDigest := buildBackendDigest([]string{
		proofSystem,
		verifierKeyID,
		stringsJoin(rvc.PublicInputs),
		hex.EncodeToString(proofBytes),
	})
	return proofSystem, verifierKeyID, proofBytes, proofDigest, "native"
}

func (n *nativeStateProofBackend) VerifyRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule) bool {
	if rvc == nil || len(rvc.ProofBytes) == 0 {
		return false
	}
	var env struct {
		Schema        string           `json:"schema"`
		WitnessDigest string           `json:"witness_digest"`
		Witness       *rvcStateWitness `json:"witness"`
	}
	if err := json.Unmarshal(rvc.ProofBytes, &env); err != nil {
		return false
	}
	if env.Schema != "zkscar-native-proof-v1" || env.Witness == nil {
		return false
	}
	if witnessDigest(env.Witness) != env.WitnessDigest {
		return false
	}
	if !validateRVCStateWitness(rvc, caps, env.Witness) {
		return false
	}
	expected := buildBackendDigest([]string{
		rvc.ProofSystem,
		rvc.VerifierKeyID,
		stringsJoin(rvc.PublicInputs),
		hex.EncodeToString(rvc.ProofBytes),
	})
	return expected == rvc.ProofDigest
}

func hashPairHex(left, right string) string {
	lb, err := hex.DecodeString(left)
	if err != nil {
		lb = []byte(left)
	}
	rb, err := hex.DecodeString(right)
	if err != nil {
		rb = []byte(right)
	}
	h := sha256.Sum256(append(lb, rb...))
	return hex.EncodeToString(h[:])
}

func computeMerkleRootFromLeafHashes(leaves []string) string {
	if len(leaves) == 0 {
		return buildBackendDigest([]string{"empty-merkle"})
	}
	layer := append([]string(nil), leaves...)
	for len(layer) > 1 {
		next := make([]string, 0, (len(layer)+1)/2)
		for i := 0; i < len(layer); i += 2 {
			left := layer[i]
			right := left
			if i+1 < len(layer) {
				right = layer[i+1]
			}
			next = append(next, hashPairHex(left, right))
		}
		layer = next
	}
	return layer[0]
}

func buildChunkMerkleProof(leaves []string, idx uint64) chunkMerkleProofEnvelope {
	env := chunkMerkleProofEnvelope{
		Schema: "merkle-sha256-v1",
		Index:  idx,
		Total:  uint64(len(leaves)),
		Steps:  make([]chunkMerkleStep, 0),
	}
	if len(leaves) == 0 {
		env.Root = computeMerkleRootFromLeafHashes(leaves)
		return env
	}
	i := int(idx)
	layer := append([]string(nil), leaves...)
	env.LeafHash = layer[i]
	for len(layer) > 1 {
		if i%2 == 0 {
			sib := i
			if i+1 < len(layer) {
				sib = i + 1
			}
			env.Steps = append(env.Steps, chunkMerkleStep{SiblingHash: layer[sib], IsLeft: false})
		} else {
			env.Steps = append(env.Steps, chunkMerkleStep{SiblingHash: layer[i-1], IsLeft: true})
		}
		next := make([]string, 0, (len(layer)+1)/2)
		for j := 0; j < len(layer); j += 2 {
			left := layer[j]
			right := left
			if j+1 < len(layer) {
				right = layer[j+1]
			}
			next = append(next, hashPairHex(left, right))
		}
		i = i / 2
		layer = next
	}
	env.Root = layer[0]
	return env
}

func verifyChunkMerkleProof(commitment, leafHash string, idx, total uint64, env *chunkMerkleProofEnvelope) bool {
	if env == nil || env.Schema != "merkle-sha256-v1" {
		return false
	}
	if env.Index != idx || env.Total != total || env.LeafHash != leafHash {
		return false
	}
	cur := leafHash
	for _, step := range env.Steps {
		if step.IsLeft {
			cur = hashPairHex(step.SiblingHash, cur)
		} else {
			cur = hashPairHex(cur, step.SiblingHash)
		}
	}
	return cur == commitment && env.Root == commitment
}

func (n *nativeStateProofBackend) BuildChunkProof(commitment string, leafHashes []string, idx uint64) (string, string) {
	env := buildChunkMerkleProof(leafHashes, idx)
	if env.Root != commitment {
		return "", ""
	}
	b, _ := json.Marshal(env)
	return "merkle-sha256-v1", string(b)
}

func (n *nativeStateProofBackend) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	if proofSystem != "merkle-sha256-v1" {
		return false
	}
	env := new(chunkMerkleProofEnvelope)
	if err := json.Unmarshal([]byte(proof), env); err != nil {
		return false
	}
	return verifyChunkMerkleProof(commitment, hash, idx, total, env)
}

func (e *externalZKBackend) BuildRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule, payload *rvcStateWitness) (string, string, []byte, string, string) {
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_PROVER"))
	if command == "" || rvc == nil || payload == nil {
		return "", "", nil, "", ""
	}
	witnessBytes, _ := json.Marshal(payload)
	req := &externalProofRequest{
		RVC:          rvc,
		Capsules:     caps,
		WitnessB64:   base64.StdEncoding.EncodeToString(witnessBytes),
		PublicInputs: rvc.PublicInputs,
	}
	resp := new(externalProofResponse)
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return "", "", nil, "", ""
	}
	pb, err := base64.StdEncoding.DecodeString(resp.ProofBytesB64)
	if err != nil {
		return "", "", nil, "", ""
	}
	mode := resp.ProofMode
	if mode == "" {
		mode = "external"
	}
	return resp.ProofSystem, resp.VerifierKeyID, pb, resp.ProofDigest, mode
}

func (e *externalZKBackend) VerifyRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule) bool {
	command := strings.TrimSpace(os.Getenv("ZKSCAR_EXTERNAL_RVC_VERIFIER"))
	if command == "" || rvc == nil {
		return false
	}
	req := &externalProofRequest{
		RVC:           rvc,
		Capsules:      caps,
		PublicInputs:  rvc.PublicInputs,
		ProofSystem:   rvc.ProofSystem,
		VerifierKeyID: rvc.VerifierKeyID,
		ProofDigest:   rvc.ProofDigest,
		ProofMode:     rvc.ProofMode,
		ProofBytesB64: base64.StdEncoding.EncodeToString(rvc.ProofBytes),
	}
	resp := new(externalProofResponse)
	if err := runExternalBackend(command, req, resp); err != nil || !resp.OK {
		return false
	}
	return resp.Valid
}

func (e *externalZKBackend) BuildChunkProof(commitment string, leafHashes []string, idx uint64) (string, string) {
	// Chunk proofs stay as native Merkle proofs even in external mode.
	return (&nativeStateProofBackend{}).BuildChunkProof(commitment, leafHashes, idx)
}

func (e *externalZKBackend) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	return (&nativeStateProofBackend{}).VerifyChunkProof(proofSystem, commitment, hash, proof, idx, total)
}

type backendDispatch struct {
	native   *nativeStateProofBackend
	external *externalZKBackend
}

func (b *backendDispatch) active() ZKBackend {
	if backendMode() == "external" {
		return b.external
	}
	return b.native
}

func (b *backendDispatch) BuildRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule, payload *rvcStateWitness) (string, string, []byte, string, string) {
	return b.active().BuildRVCProof(rvc, caps, payload)
}

func (b *backendDispatch) VerifyRVCProof(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule) bool {
	return b.active().VerifyRVCProof(rvc, caps)
}

func (b *backendDispatch) BuildChunkProof(commitment string, leafHashes []string, idx uint64) (string, string) {
	return b.active().BuildChunkProof(commitment, leafHashes, idx)
}

func (b *backendDispatch) VerifyChunkProof(proofSystem, commitment, hash, proof string, idx, total uint64) bool {
	return b.active().VerifyChunkProof(proofSystem, commitment, hash, proof, idx, total)
}

var zkBackend ZKBackend = &backendDispatch{
	native:   &nativeStateProofBackend{},
	external: &externalZKBackend{},
}
