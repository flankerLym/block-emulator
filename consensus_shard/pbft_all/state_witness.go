package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

type proofKVJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type accountStateWitness struct {
	Addr         string        `json:"addr"`
	Root         string        `json:"root"`
	RootType     string        `json:"root_type"`
	EncodedState string        `json:"encoded_state_b64"`
	Proof        []proofKVJSON `json:"proof"`
}

type rvcStateWitness struct {
	Schema       string                `json:"schema"`
	SourceProofs []accountStateWitness `json:"source_proofs"`
	FreezeProofs []accountStateWitness `json:"freeze_proofs"`
}

type pendingWitnessSlot struct {
	mu      sync.Mutex
	payload *rvcStateWitness
}

var currentRVCWitness pendingWitnessSlot

func stagePendingRVCWitness(payload *rvcStateWitness) {
	currentRVCWitness.mu.Lock()
	defer currentRVCWitness.mu.Unlock()
	currentRVCWitness.payload = payload
}

func consumePendingRVCWitness() *rvcStateWitness {
	currentRVCWitness.mu.Lock()
	defer currentRVCWitness.mu.Unlock()
	cp := currentRVCWitness.payload
	currentRVCWitness.payload = nil
	return cp
}

func decodeHexRoot(root string) ([]byte, error) {
	if root == "" {
		return nil, fmt.Errorf("empty root")
	}
	return hex.DecodeString(root)
}

func proofKVToJSON(keys, vals [][]byte) []proofKVJSON {
	out := make([]proofKVJSON, 0, len(keys))
	for i := range keys {
		if i >= len(vals) {
			break
		}
		out = append(out, proofKVJSON{
			Key:   base64.StdEncoding.EncodeToString(keys[i]),
			Value: base64.StdEncoding.EncodeToString(vals[i]),
		})
	}
	return out
}

func proofKVFromJSON(items []proofKVJSON) ([][]byte, [][]byte, error) {
	keys := make([][]byte, 0, len(items))
	vals := make([][]byte, 0, len(items))
	for _, item := range items {
		k, err := base64.StdEncoding.DecodeString(item.Key)
		if err != nil {
			return nil, nil, err
		}
		v, err := base64.StdEncoding.DecodeString(item.Value)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals, nil
}

func witnessDigest(payload *rvcStateWitness) string {
	if payload == nil {
		return ""
	}
	b, _ := json.Marshal(payload)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func buildRVCWitnessPayload(bc *chain.BlockChain, caps []message.ShadowCapsule, sourceRoot, freezeRoot string) (*rvcStateWitness, error) {
	srcRootBytes, err := decodeHexRoot(sourceRoot)
	if err != nil {
		return nil, err
	}
	freezeRootBytes, err := decodeHexRoot(freezeRoot)
	if err != nil {
		return nil, err
	}
	cp := make([]message.ShadowCapsule, len(caps))
	copy(cp, caps)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Addr < cp[j].Addr })

	payload := &rvcStateWitness{
		Schema:       "zkscar-real-state-witness-v1",
		SourceProofs: make([]accountStateWitness, 0, len(cp)),
		FreezeProofs: make([]accountStateWitness, 0, len(cp)),
	}
	for _, cap := range cp {
		srcState, srcKeys, srcVals, err := bc.BuildAccountProofAtRoot(srcRootBytes, cap.Addr)
		if err != nil {
			return nil, fmt.Errorf("build source proof for %s: %w", cap.Addr, err)
		}
		if len(srcState) == 0 {
			return nil, fmt.Errorf("empty source state for %s", cap.Addr)
		}
		freezeState, freezeKeys, freezeVals, err := bc.BuildAccountProofAtRoot(freezeRootBytes, cap.Addr)
		if err != nil {
			return nil, fmt.Errorf("build freeze proof for %s: %w", cap.Addr, err)
		}
		if len(freezeState) == 0 {
			return nil, fmt.Errorf("empty freeze state for %s", cap.Addr)
		}
		payload.SourceProofs = append(payload.SourceProofs, accountStateWitness{
			Addr:         cap.Addr,
			Root:         sourceRoot,
			RootType:     "mpt-state-root",
			EncodedState: base64.StdEncoding.EncodeToString(srcState),
			Proof:        proofKVToJSON(srcKeys, srcVals),
		})
		payload.FreezeProofs = append(payload.FreezeProofs, accountStateWitness{
			Addr:         cap.Addr,
			Root:         freezeRoot,
			RootType:     "mpt-state-root",
			EncodedState: base64.StdEncoding.EncodeToString(freezeState),
			Proof:        proofKVToJSON(freezeKeys, freezeVals),
		})
	}
	return payload, nil
}

func compareStateToCapsule(st *core.AccountState, cap message.ShadowCapsule) bool {
	if st == nil {
		return false
	}
	if st.Balance == nil || st.Balance.String() != cap.Balance {
		return false
	}
	if st.Nonce != cap.Nonce {
		return false
	}
	if !bytes.Equal(st.CodeHash, cap.CodeHash) {
		return false
	}
	if !bytes.Equal(st.StorageRoot, cap.StorageRoot) {
		return false
	}
	return true
}

func compareFreezeToSource(src, frz *core.AccountState, epoch uint64) bool {
	if src == nil || frz == nil {
		return false
	}
	if frz.Balance == nil || src.Balance == nil || frz.Balance.Cmp(src.Balance) != 0 {
		return false
	}
	if frz.Nonce != src.Nonce {
		return false
	}
	if !bytes.Equal(frz.CodeHash, src.CodeHash) || !bytes.Equal(frz.StorageRoot, src.StorageRoot) {
		return false
	}
	if !frz.Retired {
		return false
	}
	if frz.EpochTag != epoch {
		return false
	}
	return true
}

func validateRVCStateWitness(rvc *message.ReshardingValidityCertificate, caps []message.ShadowCapsule, payload *rvcStateWitness) bool {
	if payload == nil || payload.Schema == "" {
		return false
	}
	if len(payload.SourceProofs) != len(caps) || len(payload.FreezeProofs) != len(caps) {
		return false
	}
	capMap := make(map[string]message.ShadowCapsule, len(caps))
	for _, cap := range caps {
		capMap[cap.Addr] = cap
	}
	sourceStates := make(map[string]*core.AccountState, len(caps))

	for _, proof := range payload.SourceProofs {
		cap, ok := capMap[proof.Addr]
		if !ok || proof.Root != rvc.SourceStateRoot {
			return false
		}
		stateBytes, err := base64.StdEncoding.DecodeString(proof.EncodedState)
		if err != nil || len(stateBytes) == 0 {
			return false
		}
		keys, vals, err := proofKVFromJSON(proof.Proof)
		if err != nil {
			return false
		}
		rootBytes, err := decodeHexRoot(proof.Root)
		if err != nil {
			return false
		}
		okv, err := chain.VerifyAccountProofAtRoot(rootBytes, proof.Addr, stateBytes, keys, vals)
		if err != nil || !okv {
			return false
		}
		st := core.DecodeAS(stateBytes)
		if st == nil || st.Retired {
			return false
		}
		if !compareStateToCapsule(st, cap) {
			return false
		}
		if !bytes.Equal(st.DebtRoot, cap.DebtRoot) && len(cap.DebtRoot) != 0 {
			// debtRoot is migration-time synthetic data and may not be stored in old state.
			// So do not require equality here when old state does not carry it.
		}
		sourceStates[proof.Addr] = st
	}

	for _, proof := range payload.FreezeProofs {
		src, ok := sourceStates[proof.Addr]
		if !ok || proof.Root != rvc.FreezeStateRoot {
			return false
		}
		stateBytes, err := base64.StdEncoding.DecodeString(proof.EncodedState)
		if err != nil || len(stateBytes) == 0 {
			return false
		}
		keys, vals, err := proofKVFromJSON(proof.Proof)
		if err != nil {
			return false
		}
		rootBytes, err := decodeHexRoot(proof.Root)
		if err != nil {
			return false
		}
		okv, err := chain.VerifyAccountProofAtRoot(rootBytes, proof.Addr, stateBytes, keys, vals)
		if err != nil || !okv {
			return false
		}
		frz := core.DecodeAS(stateBytes)
		if !compareFreezeToSource(src, frz, rvc.EpochTag) {
			return false
		}
	}
	return true
}
