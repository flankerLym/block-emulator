package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
)

type retirementWitnessBundle struct {
	SettledReceiptKeys     []string
	OutstandingReceiptKeys []string
	PostCutoverWriteKeys   []string
}

func hashStrings(parts []string) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func capsuleStateHash(cap message.ShadowCapsule) string {
	parts := []string{
		cap.Addr,
		cap.Balance,
		strconv.FormatUint(cap.Nonce, 10),
		hex.EncodeToString(cap.CodeHash),
		hex.EncodeToString(cap.StorageRoot),
	}
	return hashStrings(parts)
}

func expectedStrictRVCPublicInputs(rvc *message.ReshardingValidityCertificate) []string {
	if rvc == nil {
		return []string{}
	}
	return []string{
		rvc.ProtocolVersion,
		rvc.CircuitVersion,
		strconv.FormatUint(rvc.EpochTag, 10),
		strconv.FormatUint(rvc.FromShard, 10),
		strconv.FormatUint(rvc.ToShard, 10),
		rvc.CertificateID,
		rvc.SourceStateRoot,
		rvc.FreezeStateRoot,
		rvc.TargetShadowRoot,
		rvc.PartitionDigest,
		rvc.CapsuleDigest,
		rvc.BalanceDigest,
		rvc.UniqueAddrDigest,
		rvc.DebtWitnessDigest,
		rvc.FreezeWitnessDigest,
		rvc.WitnessBundleHash,
		rvc.SemanticWitnessDigest,
		strconv.FormatUint(rvc.BatchSize, 10),
	}
}

func buildStateWitnessesForBatch(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) ([]*message.AccountStateWitness, error) {
	out := make([]*message.AccountStateWitness, 0, len(capsules))
	for _, cap := range capsules {
		accountHash := capsuleStateHash(cap)
		sourceProof := chain.NewSyntheticAccountTrieProof(rvc.SourceStateRoot, cap.Addr, accountHash, true)
		freezeProof := chain.NewSyntheticAccountTrieProof(rvc.FreezeStateRoot, cap.Addr, accountHash, true)
		out = append(out, &message.AccountStateWitness{
			Addr:        cap.Addr,
			SourceProof: sourceProof,
			FreezeProof: freezeProof,
		})
	}
	return out, nil
}

func witnessBundleHash(rvc *message.ReshardingValidityCertificate) string {
	if rvc == nil {
		return ""
	}
	lines := make([]string, 0)
	for _, w := range rvc.StateWitnesses {
		if w == nil || w.SourceProof == nil || w.FreezeProof == nil {
			continue
		}
		lines = append(lines,
			w.Addr+"|"+
				w.SourceProof.StateRoot+"|"+
				w.SourceProof.AccountHash+"|"+
				w.FreezeProof.StateRoot+"|"+
				w.FreezeProof.AccountHash)
	}
	sort.Strings(lines)
	return hashStrings(lines)
}

func buildSemanticWitnessesFromRVC(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) ([]*message.RVCSemanticWitness, error) {
	out := make([]*message.RVCSemanticWitness, 0, len(capsules))
	for _, cap := range capsules {
		out = append(out, &message.RVCSemanticWitness{
			Addr:                  cap.Addr,
			SourceBalance:         cap.Balance,
			SourceNonce:           cap.Nonce,
			SourceCodeHashHex:     hex.EncodeToString(cap.CodeHash),
			SourceStorageRootHex:  hex.EncodeToString(cap.StorageRoot),
			FreezeBalance:         cap.Balance,
			FreezeNonce:           cap.Nonce,
			FreezeCodeHashHex:     hex.EncodeToString(cap.CodeHash),
			FreezeStorageRootHex:  hex.EncodeToString(cap.StorageRoot),
			CapsuleBalance:        cap.Balance,
			CapsuleNonce:          cap.Nonce,
			CapsuleCodeHashHex:    hex.EncodeToString(cap.CodeHash),
			CapsuleStorageRootHex: hex.EncodeToString(cap.StorageRoot),
			DebtRootHex:           hex.EncodeToString(cap.DebtRoot),
		})
	}
	return out, nil
}

func buildWitnessBundleBinding(bundleHash string) string {
	return hashStrings([]string{"witness-bundle", bundleHash})
}

func buildCertificateBinding(id string) string {
	return hashStrings([]string{"certificate-binding", id})
}

func buildSemanticWitnessDigest(epochTag, fromShard, toShard, batchSize uint64, witnessBinding, certBinding string, witnesses []*message.RVCSemanticWitness) string {
	lines := []string{
		strconv.FormatUint(epochTag, 10),
		strconv.FormatUint(fromShard, 10),
		strconv.FormatUint(toShard, 10),
		strconv.FormatUint(batchSize, 10),
		witnessBinding,
		certBinding,
	}
	for _, w := range witnesses {
		if w == nil {
			continue
		}
		lines = append(lines,
			w.Addr+"|"+
				w.SourceBalance+"|"+
				strconv.FormatUint(w.SourceNonce, 10)+"|"+
				w.SourceCodeHashHex+"|"+
				w.SourceStorageRootHex+"|"+
				w.FreezeBalance+"|"+
				strconv.FormatUint(w.FreezeNonce, 10)+"|"+
				w.FreezeCodeHashHex+"|"+
				w.FreezeStorageRootHex+"|"+
				w.CapsuleBalance+"|"+
				strconv.FormatUint(w.CapsuleNonce, 10)+"|"+
				w.CapsuleCodeHashHex+"|"+
				w.CapsuleStorageRootHex+"|"+
				w.DebtRootHex)
	}
	sort.Strings(lines)
	return hashStrings(lines)
}

func validateStateWitnesses(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil {
		return false
	}
	if len(rvc.StateWitnesses) != len(capsules) {
		return false
	}
	index := make(map[string]message.ShadowCapsule)
	for _, cap := range capsules {
		index[cap.Addr] = cap
	}
	for _, w := range rvc.StateWitnesses {
		if w == nil || w.SourceProof == nil || w.FreezeProof == nil {
			return false
		}
		cap, ok := index[w.Addr]
		if !ok {
			return false
		}
		expectedHash := capsuleStateHash(cap)
		if !chain.VerifySyntheticAccountTrieProof(w.SourceProof, rvc.SourceStateRoot, cap.Addr, expectedHash) {
			return false
		}
		if !chain.VerifySyntheticAccountTrieProof(w.FreezeProof, rvc.FreezeStateRoot, cap.Addr, expectedHash) {
			return false
		}
	}
	return witnessBundleHash(rvc) == rvc.WitnessBundleHash
}

func validateSemanticWitnessDigest(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil {
		return false
	}
	witnesses := rvc.SemanticWitnesses
	if len(witnesses) == 0 {
		var err error
		witnesses, err = buildSemanticWitnessesFromRVC(rvc, capsules)
		if err != nil {
			return false
		}
	}
	expected := buildSemanticWitnessDigest(
		rvc.EpochTag,
		rvc.FromShard,
		rvc.ToShard,
		rvc.BatchSize,
		buildWitnessBundleBinding(rvc.WitnessBundleHash),
		buildCertificateBinding(rvc.CertificateID),
		witnesses,
	)
	return expected == rvc.SemanticWitnessDigest
}

func groupShadowCapsulesByRVCID(capsules []message.ShadowCapsule) map[string][]message.ShadowCapsule {
	out := make(map[string][]message.ShadowCapsule)
	for _, cap := range capsules {
		out[cap.RVCID] = append(out[cap.RVCID], cap)
	}
	return out
}

func buildShadowWitnessesForInstalledAccounts(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) ([]*message.ShadowStateWitness, error) {
	out := make([]*message.ShadowStateWitness, 0, len(capsules))
	for _, cap := range capsules {
		proof, err := pbftNode.CurChain.BuildAccountTrieProof(cap.Addr)
		if err != nil {
			return nil, err
		}
		out = append(out, &message.ShadowStateWitness{
			Addr:        cap.Addr,
			ShadowProof: proof,
		})
	}
	return out, nil
}

func validateInstalledShadowAccounts(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil {
		return false
	}
	rootHex := stateRootHex(pbftNode.CurChain.CurrentBlock.Header.StateRoot)
	if rvc.TargetShadowRoot != rootHex {
		return false
	}
	if len(rvc.ShadowWitnesses) != len(capsules) {
		return false
	}
	index := make(map[string]message.ShadowCapsule)
	for _, cap := range capsules {
		index[cap.Addr] = cap
	}
	for _, w := range rvc.ShadowWitnesses {
		if w == nil || w.ShadowProof == nil {
			return false
		}
		cap, ok := index[w.Addr]
		if !ok {
			return false
		}
		if w.ShadowProof.StateRoot != rootHex || !w.ShadowProof.Exists {
			return false
		}
		as := pbftNode.CurChain.GetAccountState(cap.Addr)
		if as == nil || !as.OwnershipTransferred || as.SourceShard != cap.CurrentShard || as.TargetShard != cap.TargetShard || as.LastRVC != cap.RVCID {
			return false
		}
	}
	return true
}

func buildChunkMerkleRoot(hashes []string) (string, map[uint64][]string) {
	paths := make(map[uint64][]string)
	if len(hashes) == 0 {
		return hashStrings([]string{"empty"}), paths
	}
	level := make([]string, len(hashes))
	copy(level, hashes)
	idxMap := make([][]int, len(hashes))
	for i := range hashes {
		idxMap[i] = []int{i}
		paths[uint64(i)] = []string{}
	}
	for len(level) > 1 {
		nextLevel := make([]string, 0)
		nextIdxMap := make([][]int, 0)
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			leftIdx := idxMap[i]
			right := left
			rightIdx := leftIdx
			if i+1 < len(level) {
				right = level[i+1]
				rightIdx = idxMap[i+1]
			}
			parent := hashStrings([]string{left, right})
			nextLevel = append(nextLevel, parent)
			combinedIdx := append([]int{}, leftIdx...)
			if i+1 < len(level) {
				combinedIdx = append(combinedIdx, rightIdx...)
			}
			nextIdxMap = append(nextIdxMap, combinedIdx)

			for _, idx := range leftIdx {
				paths[uint64(idx)] = append(paths[uint64(idx)], "R:"+right)
			}
			for _, idx := range rightIdx {
				if i+1 < len(level) {
					paths[uint64(idx)] = append(paths[uint64(idx)], "L:"+left)
				}
			}
		}
		level = nextLevel
		idxMap = nextIdxMap
	}
	return level[0], paths
}

func validateRetirementProofAgainstState(cdm *dataSupport.Data_supportCLPA, proof *message.RetirementProof, cap *message.ShadowCapsule) bool {
	if cdm == nil || proof == nil || cap == nil {
		return false
	}
	bundle := buildRetirementWitnessBundle(cdm, proof.Addr)
	if proof.RVCID != cap.RVCID || proof.FromShard != cap.CurrentShard || proof.ToShard != cap.TargetShard {
		return false
	}
	if proof.SettledReceiptCount != uint64(len(bundle.SettledReceiptKeys)) {
		return false
	}
	if proof.OutstandingReceiptCount != uint64(len(bundle.OutstandingReceiptKeys)) {
		return false
	}
	if proof.PostCutoverWriteCount != uint64(len(bundle.PostCutoverWriteKeys)) {
		return false
	}
	expectedDebtDigest := buildRetirementDebtWitnessDigest(bundle.SettledReceiptKeys, bundle.OutstandingReceiptKeys)
	expectedNoWriteDigest := buildNoWriteWitnessDigest(bundle.PostCutoverWriteKeys)
	if proof.DebtWitnessDigest != expectedDebtDigest || proof.NoWriteWitnessDigest != expectedNoWriteDigest {
		return false
	}
	expectedRetDigest := buildRetirementWitnessDigest(
		buildCertificateBinding(proof.Addr),
		proof.EpochTag,
		proof.FromShard,
		proof.ToShard,
		proof.SettledReceiptCount,
		proof.OutstandingReceiptCount,
		proof.PostCutoverWriteCount,
		expectedDebtDigest,
		expectedNoWriteDigest,
		buildCertificateBinding(proof.RVCID),
	)
	if proof.RetirementWitnessDigest != expectedRetDigest {
		return false
	}
	return proof.PublicInputs != nil && len(proof.PublicInputs) == len(expectedRetirementPublicInputs(proof))
}

func buildRetirementWitnessBundle(cdm *dataSupport.Data_supportCLPA, addr string) *retirementWitnessBundle {
	out := &retirementWitnessBundle{
		SettledReceiptKeys:     []string{},
		OutstandingReceiptKeys: []string{},
		PostCutoverWriteKeys:   []string{},
	}
	if cdm == nil {
		return out
	}
	if keys, ok := cdm.AddressReceiptIndex[addr]; ok {
		for key := range keys {
			if cdm.SettledDualAnchorReceipts[key] {
				out.SettledReceiptKeys = append(out.SettledReceiptKeys, key)
			} else {
				out.OutstandingReceiptKeys = append(out.OutstandingReceiptKeys, key)
			}
		}
	}
	if keys, ok := cdm.PostCutoverWriteSet[addr]; ok {
		for key := range keys {
			out.PostCutoverWriteKeys = append(out.PostCutoverWriteKeys, key)
		}
	}
	sort.Strings(out.SettledReceiptKeys)
	sort.Strings(out.OutstandingReceiptKeys)
	sort.Strings(out.PostCutoverWriteKeys)
	return out
}

func buildRetirementDebtWitnessDigest(settled []string, outstanding []string) string {
	lines := []string{"settled"}
	lines = append(lines, settled...)
	lines = append(lines, "outstanding")
	lines = append(lines, outstanding...)
	return hashStrings(lines)
}

func buildNoWriteWitnessDigest(keys []string) string {
	lines := []string{"post-cutover-write-set"}
	lines = append(lines, keys...)
	return hashStrings(lines)
}

func buildRetirementWitnessDigest(addrBinding string, epochTag, fromShard, toShard, settledCount, outstandingCount, postWriteCount uint64, debtDigest, noWriteDigest, rvcBinding string) string {
	lines := []string{
		addrBinding,
		strconv.FormatUint(epochTag, 10),
		strconv.FormatUint(fromShard, 10),
		strconv.FormatUint(toShard, 10),
		strconv.FormatUint(settledCount, 10),
		strconv.FormatUint(outstandingCount, 10),
		strconv.FormatUint(postWriteCount, 10),
		debtDigest,
		noWriteDigest,
		rvcBinding,
	}
	return hashStrings(lines)
}

func expectedRetirementPublicInputs(proof *message.RetirementProof) []string {
	if proof == nil {
		return []string{}
	}
	return []string{
		proof.ProtocolVersion,
		proof.CircuitVersion,
		proof.Addr,
		strconv.FormatUint(proof.EpochTag, 10),
		strconv.FormatUint(proof.FromShard, 10),
		strconv.FormatUint(proof.ToShard, 10),
		strconv.FormatBool(proof.Hydrated),
		strconv.FormatBool(proof.DebtRootCleared),
		strconv.FormatUint(proof.SettledReceiptCount, 10),
		strconv.FormatUint(proof.OutstandingReceiptCount, 10),
		strconv.FormatUint(proof.PostCutoverWriteCount, 10),
		proof.AddressBinding,
		proof.RVCBinding,
		proof.DebtWitnessDigest,
		proof.NoWriteWitnessDigest,
		proof.RetirementWitnessDigest,
		proof.RVCID,
	}
}
