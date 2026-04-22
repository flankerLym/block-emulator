package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

func accountStateMatchesCapsuleBase(state *core.AccountState, cap message.ShadowCapsule) bool {
	if state == nil || state.Balance == nil {
		return false
	}
	if state.Balance.String() != cap.Balance {
		return false
	}
	if state.Nonce != cap.Nonce {
		return false
	}
	if hex.EncodeToString(state.CodeHash) != hex.EncodeToString(cap.CodeHash) {
		return false
	}
	if hex.EncodeToString(state.StorageRoot) != hex.EncodeToString(cap.StorageRoot) {
		return false
	}
	return true
}

func freezeStateMatchesSourceAndEpoch(source, freeze *core.AccountState, epochTag uint64) bool {
	if source == nil || freeze == nil {
		return false
	}
	if !freeze.Retired || freeze.EpochTag != epochTag {
		return false
	}
	if source.Balance == nil || freeze.Balance == nil || source.Balance.String() != freeze.Balance.String() {
		return false
	}
	if source.Nonce != freeze.Nonce {
		return false
	}
	if hex.EncodeToString(source.CodeHash) != hex.EncodeToString(freeze.CodeHash) {
		return false
	}
	if hex.EncodeToString(source.StorageRoot) != hex.EncodeToString(freeze.StorageRoot) {
		return false
	}
	return true
}

func shadowStateMatchesCapsule(state *core.AccountState, cap message.ShadowCapsule, rvcID string) bool {
	if state == nil || !state.IsShadow() {
		return false
	}
	if !accountStateMatchesCapsuleBase(state, cap) {
		return false
	}
	if hex.EncodeToString(state.DebtRoot) != hex.EncodeToString(cap.DebtRoot) {
		return false
	}
	if state.EpochTag != cap.EpochTag || state.SourceShard != cap.CurrentShard || state.TargetShard != cap.TargetShard {
		return false
	}
	if state.LastRVC != rvcID {
		return false
	}
	return true
}

func buildStateWitnessesForBatch(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) ([]*message.AccountStateWitness, error) {
	sourceRoot, err := hex.DecodeString(rvc.SourceStateRoot)
	if err != nil {
		return nil, err
	}
	freezeRoot, err := hex.DecodeString(rvc.FreezeStateRoot)
	if err != nil {
		return nil, err
	}
	ordered := append([]message.ShadowCapsule(nil), capsules...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Addr < ordered[j].Addr })
	witnesses := make([]*message.AccountStateWitness, 0, len(ordered))
	for _, cap := range ordered {
		sourceProof, err := pbftNode.CurChain.BuildAccountProofAtStateRoot(sourceRoot, cap.Addr)
		if err != nil {
			return nil, err
		}
		freezeProof, err := pbftNode.CurChain.BuildAccountProofAtStateRoot(freezeRoot, cap.Addr)
		if err != nil {
			return nil, err
		}
		witnesses = append(witnesses, &message.AccountStateWitness{
			Addr:        cap.Addr,
			SourceProof: sourceProof,
			FreezeProof: freezeProof,
		})
	}
	return witnesses, nil
}

func buildShadowWitnessesForInstalledAccounts(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) ([]*message.ShadowStateWitness, error) {
	shadowRootHex := rvc.TargetShadowRoot
	if shadowRootHex == "" {
		shadowRootHex = stateRootHex(pbftNode.CurChain.CurrentBlock.Header.StateRoot)
	}
	shadowRoot, err := hex.DecodeString(shadowRootHex)
	if err != nil {
		return nil, err
	}
	ordered := append([]message.ShadowCapsule(nil), capsules...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Addr < ordered[j].Addr })
	witnesses := make([]*message.ShadowStateWitness, 0, len(ordered))
	for _, cap := range ordered {
		proof, err := pbftNode.CurChain.BuildAccountProofAtStateRoot(shadowRoot, cap.Addr)
		if err != nil {
			return nil, err
		}
		witnesses = append(witnesses, &message.ShadowStateWitness{Addr: cap.Addr, ShadowProof: proof})
	}
	return witnesses, nil
}

func witnessBundleHash(rvc *message.ReshardingValidityCertificate) string {
	type serialBundle struct {
		ProtocolVersion string                         `json:"protocol_version"`
		CircuitVersion  string                         `json:"circuit_version"`
		CertificateID   string                         `json:"certificate_id"`
		SourceStateRoot string                         `json:"source_state_root"`
		FreezeStateRoot string                         `json:"freeze_state_root"`
		BatchSize       uint64                         `json:"batch_size"`
		Witnesses       []*message.AccountStateWitness `json:"witnesses"`
	}
	ws := append([]*message.AccountStateWitness(nil), rvc.StateWitnesses...)
	sort.Slice(ws, func(i, j int) bool { return ws[i].Addr < ws[j].Addr })
	bundle := serialBundle{
		ProtocolVersion: rvc.ProtocolVersion,
		CircuitVersion:  rvc.CircuitVersion,
		CertificateID:   rvc.CertificateID,
		SourceStateRoot: rvc.SourceStateRoot,
		FreezeStateRoot: rvc.FreezeStateRoot,
		BatchSize:       rvc.BatchSize,
		Witnesses:       ws,
	}
	raw, _ := json.Marshal(bundle)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func validateStateWitnesses(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil || len(rvc.StateWitnesses) != len(capsules) {
		return false
	}
	capsByAddr := make(map[string]message.ShadowCapsule, len(capsules))
	for _, cap := range capsules {
		capsByAddr[cap.Addr] = cap
	}
	for _, wit := range rvc.StateWitnesses {
		cap, ok := capsByAddr[wit.Addr]
		if !ok || wit == nil || wit.SourceProof == nil || wit.FreezeProof == nil {
			return false
		}
		sourceBytes, err := chain.VerifyAccountProof(wit.SourceProof)
		if err != nil {
			return false
		}
		freezeBytes, err := chain.VerifyAccountProof(wit.FreezeProof)
		if err != nil {
			return false
		}
		sourceState := core.DecodeAS(sourceBytes)
		freezeState := core.DecodeAS(freezeBytes)
		if !accountStateMatchesCapsuleBase(sourceState, cap) {
			return false
		}
		if !freezeStateMatchesSourceAndEpoch(sourceState, freezeState, rvc.EpochTag) {
			return false
		}
	}
	return rvc.WitnessBundleHash == witnessBundleHash(rvc)
}

func validateInstalledShadowAccounts(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil || len(rvc.ShadowWitnesses) != len(capsules) {
		return false
	}
	capsByAddr := make(map[string]message.ShadowCapsule, len(capsules))
	for _, cap := range capsules {
		capsByAddr[cap.Addr] = cap
	}
	for _, wit := range rvc.ShadowWitnesses {
		cap, ok := capsByAddr[wit.Addr]
		if !ok || wit == nil || wit.ShadowProof == nil {
			return false
		}
		value, err := chain.VerifyAccountProof(wit.ShadowProof)
		if err != nil {
			return false
		}
		state := core.DecodeAS(value)
		if !shadowStateMatchesCapsule(state, cap, rvc.CertificateID) {
			return false
		}
		current := pbftNode.CurChain.GetAccountState(cap.Addr)
		if !shadowStateMatchesCapsule(current, cap, rvc.CertificateID) {
			return false
		}
	}
	return true
}

func groupShadowCapsulesByRVCID(capsules []message.ShadowCapsule) map[string][]message.ShadowCapsule {
	out := make(map[string][]message.ShadowCapsule)
	for _, cap := range capsules {
		out[cap.RVCID] = append(out[cap.RVCID], cap)
	}
	for id := range out {
		sort.Slice(out[id], func(i, j int) bool { return out[id][i].Addr < out[id][j].Addr })
	}
	return out
}
