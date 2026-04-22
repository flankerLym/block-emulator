package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"time"
)

func stableHashStrings(items []string) string {
	cp := make([]string, len(items))
	copy(cp, items)
	sort.Strings(cp)
	h := sha256.Sum256([]byte(stringsJoin(cp)))
	return hex.EncodeToString(h[:])
}

func stringsJoin(items []string) string {
	if len(items) == 0 {
		return ""
	}
	out := items[0]
	for i := 1; i < len(items); i++ {
		out += "|" + items[i]
	}
	return out
}

func latestPartitionMeta(cdm *dataSupport.Data_supportCLPA) *message.PartitionModifiedMap {
	if len(cdm.PartitionMeta) == 0 {
		return nil
	}
	return &cdm.PartitionMeta[len(cdm.PartitionMeta)-1]
}

func templateCapsule(meta *message.PartitionModifiedMap, addr string) *message.ShadowCapsule {
	if meta == nil {
		return nil
	}
	for _, cap := range meta.ShadowCapsules {
		if cap.Addr == addr {
			cp := cap
			return &cp
		}
	}
	return nil
}

func stateRootHex(root []byte) string {
	if len(root) == 0 {
		return ""
	}
	return hex.EncodeToString(root)
}

func debtRootForAddr(txs []*core.Transaction, cdm *dataSupport.Data_supportCLPA, addr string) []byte {
	parts := make([]string, 0)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		if string(tx.Sender) == addr || string(tx.Recipient) == addr || string(tx.OriginalSender) == addr || string(tx.FinalRecipient) == addr {
			parts = append(parts,
				"tx|"+
					hex.EncodeToString(tx.TxHash)+"|"+
					string(tx.Sender)+"|"+
					string(tx.Recipient)+"|"+
					string(tx.OriginalSender)+"|"+
					string(tx.FinalRecipient))
		}
	}
	if cdm != nil {
		if keys, ok := cdm.AddressReceiptIndex[addr]; ok {
			for key := range keys {
				receipt, rok := cdm.DualAnchorReceiptPool[key]
				if !rok || receipt == nil {
					continue
				}
				settled := cdm.SettledDualAnchorReceipts[key]
				parts = append(parts,
					"receipt|"+key+"|"+
						receipt.Sender+"|"+
						receipt.Recipient+"|"+
						receipt.OldRoot+"|"+
						receipt.ShadowRoot+"|"+
						strconv.FormatBool(settled))
			}
		}
	}
	h := sha256.Sum256([]byte(stringsJoin(parts)))
	return h[:]
}

func shadowCapsuleDigest(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	cp := make([]message.ShadowCapsule, len(capsules))
	copy(cp, capsules)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].Addr == cp[j].Addr {
			return cp[i].TargetShard < cp[j].TargetShard
		}
		return cp[i].Addr < cp[j].Addr
	})
	for _, c := range cp {
		parts = append(parts, c.Addr+"|"+c.Balance+"|"+hex.EncodeToString(c.CodeHash)+"|"+hex.EncodeToString(c.StorageRoot)+"|"+hex.EncodeToString(c.DebtRoot))
	}
	return stableHashStrings(parts)
}
func partitionDigestForCapsules(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+strconv.FormatUint(c.CurrentShard, 10)+"|"+strconv.FormatUint(c.TargetShard, 10))
	}
	return stableHashStrings(parts)
}
func balanceDigestForCapsules(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+c.Balance+"|"+strconv.FormatUint(c.Nonce, 10))
	}
	return stableHashStrings(parts)
}
func uniqueAddrDigestForCapsules(capsules []message.ShadowCapsule) string {
	seen := map[string]bool{}
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		if seen[c.Addr] {
			continue
		}
		seen[c.Addr] = true
		parts = append(parts, c.Addr)
	}
	return stableHashStrings(parts)
}
func debtWitnessDigestForCapsules(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+hex.EncodeToString(c.DebtRoot))
	}
	return stableHashStrings(parts)
}
func freezeWitnessDigestForCapsules(epochTag, fromShard, toShard uint64, sourceStateRoot, freezeStateRoot string, capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+strconv.FormatUint(epochTag, 10)+"|"+strconv.FormatUint(fromShard, 10)+"|"+strconv.FormatUint(toShard, 10)+"|"+sourceStateRoot+"|"+freezeStateRoot)
	}
	return stableHashStrings(parts)
}
func expectedRVCInputs(rvc *message.ReshardingValidityCertificate) []string {
	return expectedStrictRVCPublicInputs(rvc)
}

func buildBatchRVC(pbftNode *PbftConsensusNode, epochTag, fromShard, toShard uint64, capsules []message.ShadowCapsule, sourceStateRoot, freezeStateRoot string) *message.ReshardingValidityCertificate {
	capsDigest := shadowCapsuleDigest(capsules)
	partDigest := partitionDigestForCapsules(capsules)
	balDigest := balanceDigestForCapsules(capsules)
	uniqueDigest := uniqueAddrDigestForCapsules(capsules)
	debtDigest := debtWitnessDigestForCapsules(capsules)
	freezeDigest := freezeWitnessDigestForCapsules(epochTag, fromShard, toShard, sourceStateRoot, freezeStateRoot, capsules)
	idBase := []string{"ZKSCAR", strconv.FormatUint(epochTag, 10), strconv.FormatUint(fromShard, 10), strconv.FormatUint(toShard, 10), sourceStateRoot, freezeStateRoot, capsDigest, partDigest, balDigest, uniqueDigest, debtDigest, freezeDigest, strconv.Itoa(len(capsules))}
	certID := stableHashStrings(idBase)
	for i := range capsules {
		capsules[i].RVCID = certID
	}
	rvc := &message.ReshardingValidityCertificate{Algorithm: "ZKSCAR", ProtocolVersion: "zkscar-rvc-v3", CircuitVersion: "rvc-semantic-groth16-v1", EpochTag: epochTag, FromShard: fromShard, ToShard: toShard, CertificateID: certID, SourceStateRoot: sourceStateRoot, SourceStateRootType: "mpt-state-root", FreezeStateRoot: freezeStateRoot, FreezeStateRootType: "mpt-state-root", TargetShadowRoot: "", TargetShadowRootType: "pending-shadow-root", PartitionDigest: partDigest, CapsuleDigest: capsDigest, BalanceDigest: balDigest, UniqueAddrDigest: uniqueDigest, DebtWitnessDigest: debtDigest, FreezeWitnessDigest: freezeDigest, BatchSize: uint64(len(capsules)), VerifierKeyID: "zkscar-rvc-groth16-v1"}
	stateWitnesses, err := buildStateWitnessesForBatch(pbftNode, rvc, capsules)
	if err != nil {
		log.Panic(err)
	}
	rvc.StateWitnesses = stateWitnesses
	rvc.WitnessBundleHash = witnessBundleHash(rvc)
	semanticWitnesses, err := buildSemanticWitnessesFromRVC(rvc, capsules)
	if err != nil {
		log.Panic(err)
	}
	rvc.SemanticWitnesses = semanticWitnesses
	rvc.SemanticWitnessDigest = buildSemanticWitnessDigest(rvc.EpochTag, rvc.FromShard, rvc.ToShard, rvc.BatchSize, buildWitnessBundleBinding(rvc.WitnessBundleHash), buildCertificateBinding(rvc.CertificateID), semanticWitnesses)
	rvc.PublicInputs = expectedRVCInputs(rvc)
	proofSystem, verifierKeyID, proofBytes, proofDigest, proofMode := zkBackend.BuildRVCProof(rvc)
	rvc.ProofSystem, rvc.VerifierKeyID, rvc.ProofBytes, rvc.ProofDigest, rvc.ProofMode = proofSystem, verifierKeyID, proofBytes, proofDigest, proofMode
	if rvc.ProofSystem == "" || rvc.VerifierKeyID == "" || len(rvc.ProofBytes) == 0 || rvc.ProofDigest == "" {
		log.Panic("strict RVC proof generation failed")
	}
	return rvc
}

func validateRVCBatch(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil || rvc.Algorithm != "ZKSCAR" || uint64(len(capsules)) != rvc.BatchSize {
		return false
	}
	if rvc.ProtocolVersion == "" || rvc.CircuitVersion == "" || rvc.WitnessBundleHash == "" || rvc.SemanticWitnessDigest == "" {
		return false
	}
	if rvc.SourceStateRoot == "" || rvc.SourceStateRootType == "" || rvc.FreezeStateRoot == "" || rvc.FreezeStateRootType == "" {
		return false
	}
	seen := map[string]bool{}
	for _, cap := range capsules {
		if cap.RVCID != "" && cap.RVCID != rvc.CertificateID {
			return false
		}
		if cap.CurrentShard != rvc.FromShard || cap.TargetShard != rvc.ToShard || cap.EpochTag != rvc.EpochTag {
			return false
		}
		if seen[cap.Addr] {
			return false
		}
		seen[cap.Addr] = true
	}
	if shadowCapsuleDigest(capsules) != rvc.CapsuleDigest || partitionDigestForCapsules(capsules) != rvc.PartitionDigest || balanceDigestForCapsules(capsules) != rvc.BalanceDigest || uniqueAddrDigestForCapsules(capsules) != rvc.UniqueAddrDigest || debtWitnessDigestForCapsules(capsules) != rvc.DebtWitnessDigest {
		return false
	}
	if freezeWitnessDigestForCapsules(rvc.EpochTag, rvc.FromShard, rvc.ToShard, rvc.SourceStateRoot, rvc.FreezeStateRoot, capsules) != rvc.FreezeWitnessDigest {
		return false
	}
	if !validateStateWitnesses(rvc, capsules) || !validateSemanticWitnessDigest(rvc, capsules) {
		return false
	}
	expectedInputs := expectedRVCInputs(rvc)
	if len(rvc.PublicInputs) != len(expectedInputs) {
		return false
	}
	for i := range expectedInputs {
		if rvc.PublicInputs[i] != expectedInputs[i] {
			return false
		}
	}
	return zkBackend.VerifyRVCProof(rvc)
}

func validateAccountTransferRVCs(atm *message.AccountTransferMsg) bool {
	if atm.Algorithm != "ZKSCAR" {
		return true
	}
	if len(atm.RVCs) == 0 {
		return false
	}
	grouped := groupShadowCapsulesByRVCID(atm.ShadowCapsules)
	for _, rvc := range atm.RVCs {
		caps := grouped[rvc.CertificateID]
		if len(caps) == 0 || !validateRVCBatch(rvc, caps) {
			return false
		}
	}
	return true
}
func receiptKey(txHash []byte) string { return hex.EncodeToString(txHash) }
func buildDualAnchorReceipts(txs []*core.Transaction, fromShard, toShard uint64, epochTag uint64, oldRoot string, rvcID string) []message.DualAnchorReceipt {
	out := make([]message.DualAnchorReceipt, 0, len(txs))
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		out = append(out, message.DualAnchorReceipt{TxHash: append([]byte(nil), tx.TxHash...), Sender: string(tx.Sender), Recipient: string(tx.Recipient), OldRoot: oldRoot, ShadowRoot: "", OldRootType: "mpt-state-root", ShadowRootType: "pending-shadow-root", RVCID: rvcID, FromShard: fromShard, ToShard: toShard, EpochTag: epochTag})
	}
	return out
}
func bindShadowRootToReceipts(receipts []message.DualAnchorReceipt, shadowRoot string) []message.DualAnchorReceipt {
	if len(receipts) == 0 {
		return receipts
	}
	out := make([]message.DualAnchorReceipt, len(receipts))
	copy(out, receipts)
	for i := range out {
		out[i].ShadowRoot = shadowRoot
		out[i].ShadowRootType = "mpt-state-root"
	}
	return out
}
func indexReceiptForAddress(cdm *dataSupport.Data_supportCLPA, addr, key string) {
	if addr == "" {
		return
	}
	if _, ok := cdm.AddressReceiptIndex[addr]; !ok {
		cdm.AddressReceiptIndex[addr] = make(map[string]bool)
	}
	cdm.AddressReceiptIndex[addr][key] = true
}
func indexDualAnchorReceipts(cdm *dataSupport.Data_supportCLPA, receipts []message.DualAnchorReceipt) {
	for _, receipt := range receipts {
		rc := receipt
		key := receiptKey(receipt.TxHash)
		cdm.DualAnchorReceiptPool[key] = &rc
		indexReceiptForAddress(cdm, receipt.Sender, key)
		indexReceiptForAddress(cdm, receipt.Recipient, key)
		if _, ok := cdm.SettledDualAnchorReceipts[key]; !ok {
			cdm.SettledDualAnchorReceipts[key] = false
		}
	}
}
func markDualAnchorReceiptSettled(cdm *dataSupport.Data_supportCLPA, txHash []byte) {
	if len(txHash) == 0 {
		return
	}
	key := receiptKey(txHash)
	if _, ok := cdm.DualAnchorReceiptPool[key]; ok {
		cdm.SettledDualAnchorReceipts[key] = true
	}
}
func markDualAnchorReceiptSettledForTx(cdm *dataSupport.Data_supportCLPA, tx *core.Transaction) {
	if tx == nil {
		return
	}
	markDualAnchorReceiptSettled(cdm, tx.TxHash)
	if len(tx.RawTxHash) > 0 {
		markDualAnchorReceiptSettled(cdm, tx.RawTxHash)
	}
}
func markDualAnchorReceiptsSettledForBlock(cdm *dataSupport.Data_supportCLPA, txs []*core.Transaction) {
	for _, tx := range txs {
		markDualAnchorReceiptSettledForTx(cdm, tx)
	}
}
func txTouchesAddress(tx *core.Transaction, addr string) bool {
	if tx == nil || addr == "" {
		return false
	}
	return string(tx.Sender) == addr || string(tx.Recipient) == addr || string(tx.OriginalSender) == addr || string(tx.FinalRecipient) == addr
}
func txWriteKey(tx *core.Transaction) string {
	if tx == nil {
		return ""
	}
	hashHex := hex.EncodeToString(tx.TxHash)
	if hashHex == "" {
		hashHex = hex.EncodeToString(tx.RawTxHash)
	}
	if hashHex == "" {
		hashHex = stableHashStrings([]string{string(tx.Sender), string(tx.Recipient), string(tx.OriginalSender), string(tx.FinalRecipient)})
	}
	return stringsJoin([]string{"tx", hashHex, string(tx.Sender), string(tx.Recipient), string(tx.OriginalSender), string(tx.FinalRecipient)})
}
func indexPostCutoverWrite(cdm *dataSupport.Data_supportCLPA, addr, key string) {
	if cdm == nil || addr == "" || key == "" {
		return
	}
	if _, ok := cdm.PostCutoverWriteSet[addr]; !ok {
		cdm.PostCutoverWriteSet[addr] = make(map[string]bool)
	}
	cdm.PostCutoverWriteSet[addr][key] = true
}
func recordPostCutoverWritesForBlock(cdm *dataSupport.Data_supportCLPA, txs []*core.Transaction) {
	if cdm == nil || len(txs) == 0 {
		return
	}
	monitored := map[string]bool{}
	for addr := range cdm.SourceCustodyState {
		monitored[addr] = true
	}
	for addr := range cdm.RetiredAccounts {
		monitored[addr] = true
	}
	if len(monitored) == 0 {
		return
	}
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		key := txWriteKey(tx)
		for addr := range monitored {
			if txTouchesAddress(tx, addr) {
				indexPostCutoverWrite(cdm, addr, key)
			}
		}
	}
}
func computeDebtRootCleared(cdm *dataSupport.Data_supportCLPA, addr string) bool {
	keys, ok := cdm.AddressReceiptIndex[addr]
	if !ok || len(keys) == 0 {
		return true
	}
	for key := range keys {
		if !cdm.SettledDualAnchorReceipts[key] {
			return false
		}
	}
	return true
}
func computePostCutoverWriteCount(cdm *dataSupport.Data_supportCLPA, addr string) uint64 {
	keys, ok := cdm.PostCutoverWriteSet[addr]
	if !ok {
		return 0
	}
	return uint64(len(keys))
}
func canRetireAddress(cdm *dataSupport.Data_supportCLPA, addr string) bool {
	if !cdm.HydratedAccounts[addr] {
		return false
	}
	if computePostCutoverWriteCount(cdm, addr) != 0 {
		return false
	}
	return computeDebtRootCleared(cdm, addr)
}
func collectRetirementCandidatesForBlock(txs []*core.Transaction) []string {
	set := map[string]bool{}
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		set[string(tx.Sender)] = true
		set[string(tx.Recipient)] = true
		if string(tx.OriginalSender) != "" {
			set[string(tx.OriginalSender)] = true
		}
		if string(tx.FinalRecipient) != "" {
			set[string(tx.FinalRecipient)] = true
		}
	}
	out := make([]string, 0, len(set))
	for addr := range set {
		out = append(out, addr)
	}
	sort.Strings(out)
	return out
}
func evaluateRetirementCandidatesForBlock(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, txs []*core.Transaction, epochTag uint64) {
	addrs := collectRetirementCandidatesForBlock(txs)
	for _, addr := range addrs {
		maybeSendRetirementProof(pbftNode, cdm, addr, epochTag)
	}
}
func chunkHash(payload []byte) string { h := sha256.Sum256(payload); return hex.EncodeToString(h[:]) }
func splitStateIntoChunks(state *core.AccountState, chunkSize uint64) ([][]byte, string, []string, map[uint64][]string) {
	if chunkSize == 0 {
		chunkSize = 128
	}
	raw := state.Encode()
	if len(raw) == 0 {
		root, paths := buildChunkMerkleRoot([]string{""})
		return [][]byte{{}}, root, []string{""}, paths
	}
	chunks := make([][]byte, 0)
	hashes := make([]string, 0)
	for start := 0; start < len(raw); start += int(chunkSize) {
		end := start + int(chunkSize)
		if end > len(raw) {
			end = len(raw)
		}
		cp := append([]byte(nil), raw[start:end]...)
		chunks = append(chunks, cp)
		hashes = append(hashes, chunkHash(cp))
	}
	root, paths := buildChunkMerkleRoot(hashes)
	return chunks, root, hashes, paths
}
func currentStateHeight(pbftNode *PbftConsensusNode) uint64 {
	if pbftNode == nil || pbftNode.CurChain == nil || pbftNode.CurChain.CurrentBlock == nil {
		return 0
	}
	return uint64(pbftNode.CurChain.CurrentBlock.Header.Number)
}
func shouldIssueHydrationNow(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, addr string) bool {
	if cdm == nil || addr == "" {
		return false
	}
	if cdm.HydratedAccounts[addr] {
		return false
	}
	if _, ok := cdm.PendingHydrationRequests[addr]; ok {
		return false
	}
	installHeight, ok := cdm.ShadowInstallHeight[addr]
	if !ok || params.ZKSCARHydrationDelayRounds <= 0 {
		return true
	}
	return currentStateHeight(pbftNode) >= installHeight+uint64(params.ZKSCARHydrationDelayRounds)
}
func requestHydrationChunk(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, cap *message.ShadowCapsule, chunkIndex uint64, expectedCommitment string) {
	if pbftNode.NodeID != uint64(pbftNode.view.Load()) {
		return
	}
	chunkSize := cdm.PendingHydrationChunkSize
	if chunkSize == 0 {
		chunkSize = 128
	}
	req := &message.HydrationRequest{Addr: cap.Addr, EpochTag: cap.EpochTag, FromShard: cap.CurrentShard, ToShard: cap.TargetShard, Requester: pbftNode.ShardID, NeedFull: true, ChunkIndex: chunkIndex, ChunkSize: chunkSize, ExpectedCommitment: expectedCommitment}
	cdm.PendingHydrationRequests[cap.Addr] = req
	b, err := json.Marshal(req)
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CHydrationRequest, b)
	for nid := uint64(0); nid < pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
		networks.TcpDial(msg, pbftNode.ip_nodeTable[cap.CurrentShard][nid])
	}
}
func issueHydrationRequests(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, caps []message.ShadowCapsule) {
	if pbftNode.NodeID != uint64(pbftNode.view.Load()) {
		return
	}
	for _, cap := range caps {
		if !shouldIssueHydrationNow(pbftNode, cdm, cap.Addr) {
			continue
		}
		cp := cap
		requestHydrationChunk(pbftNode, cdm, &cp, 0, "")
	}
}
func issueDeferredHydrationRequests(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA) {
	if pbftNode == nil || cdm == nil {
		return
	}
	caps := make([]message.ShadowCapsule, 0)
	for addr, cap := range cdm.ShadowCapsulePool {
		if cap == nil || cdm.HydratedAccounts[addr] {
			continue
		}
		caps = append(caps, *cap)
	}
	issueHydrationRequests(pbftNode, cdm, caps)
}
func handleHydrationRequestCommon(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, req *message.HydrationRequest) {
	if pbftNode.NodeID != uint64(pbftNode.view.Load()) {
		return
	}
	state, ok := cdm.SourceCustodyState[req.Addr]
	if !ok || state == nil {
		return
	}
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = cdm.PendingHydrationChunkSize
	}
	chunks, commitment, hashes, siblingPaths := splitStateIntoChunks(state, chunkSize)
	if req.ExpectedCommitment != "" && req.ExpectedCommitment != commitment {
		return
	}
	if req.ChunkIndex >= uint64(len(chunks)) {
		return
	}
	idx := req.ChunkIndex
	payload := chunks[idx]
	proofSystem, chunkProof := zkBackend.BuildChunkProof(commitment, hashes[idx], idx, uint64(len(chunks)), siblingPaths[idx])
	if proofSystem == "" || chunkProof == "" {
		log.Panic("strict chunk proof generation failed")
	}
	data := &message.HydrationData{Addr: req.Addr, EpochTag: req.EpochTag, FromShard: req.FromShard, ToShard: req.ToShard, ChunkIndex: idx, ChunkTotal: uint64(len(chunks)), ChunkPayload: payload, ChunkHash: hashes[idx], StateCommitment: commitment, ProofSystem: proofSystem, ChunkProof: chunkProof, IsFinal: idx+1 == uint64(len(chunks))}
	b, err := json.Marshal(data)
	if err != nil {
		log.Panic(err)
	}
	msg := message.MergeMessage(message.CHydrationData, b)
	for nid := uint64(0); nid < pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
		networks.TcpDial(msg, pbftNode.ip_nodeTable[req.ToShard][nid])
	}
}
func handleHydrationDataCommon(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, data *message.HydrationData) {
	if chunkHash(data.ChunkPayload) != data.ChunkHash {
		return
	}
	if !zkBackend.VerifyChunkProof(data.ProofSystem, "zkscar-chunk-groth16-v1", data.StateCommitment, data.ChunkHash, data.ChunkProof, data.ChunkIndex, data.ChunkTotal) {
		return
	}
	if _, ok := cdm.PendingHydrationChunks[data.Addr]; !ok {
		cdm.PendingHydrationChunks[data.Addr] = make(map[uint64][]byte)
	}
	if oldCommit, ok := cdm.PendingHydrationCommitments[data.Addr]; ok && oldCommit != data.StateCommitment {
		return
	}
	cdm.PendingHydrationCommitments[data.Addr] = data.StateCommitment
	cdm.PendingHydrationChunkTotal[data.Addr] = data.ChunkTotal
	cdm.PendingHydrationChunks[data.Addr][data.ChunkIndex] = append([]byte(nil), data.ChunkPayload...)
	received := uint64(len(cdm.PendingHydrationChunks[data.Addr]))
	total := cdm.PendingHydrationChunkTotal[data.Addr]
	if received == total {
		fullBytes := make([]byte, 0)
		for i := uint64(0); i < total; i++ {
			chunk, ok := cdm.PendingHydrationChunks[data.Addr][i]
			if !ok {
				return
			}
			fullBytes = append(fullBytes, chunk...)
		}
		fullState := core.DecodeAS(fullBytes)
		cur := pbftNode.CurChain.GetAccountState(data.Addr)
		full := cur.ApplyHydration(fullState, data.EpochTag)

		// 阶段二共识安全版：
		// hydration 完成后只缓存 full state / hydrated 标记，
		// 不直接 PutAccountState 到 CurChain，避免各节点因消息到达时序不同而推进本地空状态块。
		cdm.HydratedStateCache[data.Addr] = full
		cdm.HydratedAccounts[data.Addr] = true

		delete(cdm.PendingHydrationData, data.Addr)
		delete(cdm.PendingHydrationRequests, data.Addr)
		delete(cdm.PendingHydrationChunks, data.Addr)
		delete(cdm.PendingHydrationChunkTotal, data.Addr)
		delete(cdm.PendingHydrationCommitments, data.Addr)
		delete(cdm.ShadowInstallHeight, data.Addr)
		return
	}
	nextIndex := data.ChunkIndex + 1
	if nextIndex < total {
		cap, ok := cdm.ShadowCapsulePool[data.Addr]
		if ok && cap != nil {
			requestHydrationChunk(pbftNode, cdm, cap, nextIndex, data.StateCommitment)
		}
	}
}
func handleRetirementProofCommon(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, proof *message.RetirementProof) {
	if proof == nil || !proof.Hydrated || !proof.DebtRootCleared {
		return
	}
	cap, ok := cdm.ShadowCapsulePool[proof.Addr]
	if !ok || cap == nil {
		return
	}
	if cap.RVCID != proof.RVCID || cap.CurrentShard != proof.FromShard || cap.TargetShard != proof.ToShard || cap.EpochTag != proof.EpochTag {
		return
	}
	if _, ok := cdm.RVCPool[proof.RVCID]; !ok {
		return
	}
	if _, ok := cdm.SourceCustodyState[proof.Addr]; !ok {
		return
	}
	if !validateRetirementProofAgainstState(cdm, proof, cap) || !zkBackend.VerifyRetirementProof(proof) {
		return
	}

	// 阶段二共识安全版：
	// retirement proof 只缓存，不直接 DeleteAccounts，
	// 避免 PBFT 外的链状态删除再次导致不同节点 CurrentBlock 偏移。
	cdm.RetirementProofPool[proof.Addr] = proof
	cdm.RetiredAccounts[proof.Addr] = true
	delete(cdm.ShadowInstallHeight, proof.Addr)
}
func maybeSendRetirementProof(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, addr string, epochTag uint64) {
	_ = pbftNode
	_ = cdm
	_ = addr
	_ = epochTag

	// 阶段二验收版本中，retirement 不再主动发送。
	// 当前目标是稳定展示“先切换 shadow account、后补齐 hydration”，
	// 暂不让 retirement 进入消息面和状态面，避免再次引入 PBFT 外链状态变更。
	return
}
func applyPendingHydration(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, currentRound uint64) {
	_ = currentRound

	// 阶段二共识安全版：
	// 这里只保留“延迟 hydration 请求”的调度，不再在 propose 前消费
	// PendingHydrationData 并写 CurChain。
	issueDeferredHydrationRequests(pbftNode, cdm)
}

func (cphm *CLPAPbftInsideExtraHandleMod) sendPartitionReady() {
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady[cphm.pbftNode.ShardID] = true
	cphm.cdm.P_ReadyLock.Unlock()

	cphm.cdm.ReadySeqLock.Lock()
	cphm.cdm.ReadySeq[cphm.pbftNode.ShardID] = cphm.pbftNode.sequenceID
	cphm.cdm.ReadySeqLock.Unlock()

	pr := message.PartitionReady{FromShard: cphm.pbftNode.ShardID, NowSeqID: cphm.pbftNode.sequenceID}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	sendMsg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := uint64(0); sid < cphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < cphm.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
			if sid == cphm.pbftNode.ShardID && nid == cphm.pbftNode.NodeID {
				continue
			}
			go networks.TcpDial(sendMsg, cphm.pbftNode.ip_nodeTable[sid][nid])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}
func (cphm *CLPAPbftInsideExtraHandleMod) getPartitionReady() bool {
	cphm.cdm.P_ReadyLock.Lock()
	defer cphm.cdm.P_ReadyLock.Unlock()
	for sid := uint64(0); sid < cphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
		if !cphm.cdm.PartitionReady[sid] {
			return false
		}
	}
	return true
}
func (cphm *CLPAPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	accountToFetch := make([]string, 0)
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	meta := latestPartitionMeta(cphm.cdm)
	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	if meta != nil && meta.Algorithm == "ZKSCAR" && len(accountToFetch) > 0 {
		materialized := cphm.pbftNode.CurChain.MaterializeAccountsIfMissing(accountToFetch, asFetched)
		if len(materialized) > 0 {
			cphm.pbftNode.pl.Plog.Printf("ZK-SCAR: materialized %d source accounts before RVC proof generation\n", len(materialized))
			asFetched = cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
		}
	}
	cphm.pbftNode.CurChain.Txpool.GetLocked()
	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := map[string]bool{}
		asSend := make([]*core.AccountState, 0)
		shadowCapsules := make([]message.ShadowCapsule, 0)
		sourceStateRoot := ""
		if meta != nil && meta.Algorithm == "ZKSCAR" {
			sourceStateRoot = stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
		}
		for idx, addr := range accountToFetch {
			if cphm.cdm.ModifiedMap[lastMapid][addr] == i {
				baseState := asFetched[idx]
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				if meta != nil && meta.Algorithm == "ZKSCAR" {
					cphm.cdm.SourceCustodyState[addr] = baseState.Clone()
					cphm.pbftNode.CurChain.FreezeAccount(addr, meta.EpochTag)
					tmpl := templateCapsule(meta, addr)
					debtRoot := debtRootForAddr(cphm.pbftNode.CurChain.Txpool.TxQueue, cphm.cdm, addr)
					cap := message.ShadowCapsule{Addr: addr, CurrentShard: cphm.pbftNode.ShardID, TargetShard: i, Balance: baseState.Balance.String(), Nonce: baseState.Nonce, CodeHash: append([]byte(nil), baseState.CodeHash...), StorageRoot: append([]byte(nil), baseState.StorageRoot...), DebtRoot: debtRoot, EpochTag: meta.EpochTag}
					if tmpl != nil {
						cap.Degree, cap.Hotness, cap.LocalityGain = tmpl.Degree, tmpl.Hotness, tmpl.LocalityGain
					}
					shadowCapsules = append(shadowCapsules, cap)
				} else {
					asSend = append(asSend, baseState.Clone())
				}
			}
		}
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			_, ok1 := addrSet[string(ptx.Sender)]
			_, ok2 := addrSet[string(ptx.Recipient)]
			condition1 := ok1 && !ptx.Relayed
			condition2 := ok2 && ptx.Relayed
			if condition1 || condition2 {
				txSend = append(txSend, ptx)
			} else {
				cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
				firstPtr++
			}
		}
		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]
		if meta != nil && meta.Algorithm == "ZKSCAR" {
			if len(shadowCapsules) > 0 {
				freezeStateRoot := stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
				rvc := buildBatchRVC(cphm.pbftNode, meta.EpochTag, cphm.pbftNode.ShardID, i, shadowCapsules, sourceStateRoot, freezeStateRoot)
				for _, cap := range shadowCapsules {
					shadowState := cphm.cdm.SourceCustodyState[cap.Addr].BuildShadowState(meta.EpochTag, cphm.pbftNode.ShardID, i, cap.DebtRoot, rvc.CertificateID)
					asSend = append(asSend, shadowState)
				}
				ast := message.AccountStateAndTx{Addrs: addrSend, AccountState: asSend, ShadowCapsules: shadowCapsules, FromShard: cphm.pbftNode.ShardID, Txs: txSend, Algorithm: "ZKSCAR", Stage: "shadow", DualReceipts: buildDualAnchorReceipts(txSend, cphm.pbftNode.ShardID, i, meta.EpochTag, sourceStateRoot, rvc.CertificateID), RVC: rvc}
				aByte, err := json.Marshal(ast)
				if err != nil {
					log.Panic(err)
				}
				for nid := uint64(0); nid < cphm.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
					networks.TcpDial(message.MergeMessage(message.AccountState_and_TX, aByte), cphm.pbftNode.ip_nodeTable[i][nid])
				}
			} else {
				ast := message.AccountStateAndTx{Addrs: addrSend, AccountState: asSend, FromShard: cphm.pbftNode.ShardID, Txs: txSend, Algorithm: "ZKSCAR", Stage: "shadow"}
				aByte, err := json.Marshal(ast)
				if err != nil {
					log.Panic(err)
				}
				for nid := uint64(0); nid < cphm.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
					networks.TcpDial(message.MergeMessage(message.AccountState_and_TX, aByte), cphm.pbftNode.ip_nodeTable[i][nid])
				}
			}
		} else {
			ast := message.AccountStateAndTx{Addrs: addrSend, AccountState: asSend, FromShard: cphm.pbftNode.ShardID, Txs: txSend}
			aByte, err := json.Marshal(ast)
			if err != nil {
				log.Panic(err)
			}
			for nid := uint64(0); nid < cphm.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
				networks.TcpDial(message.MergeMessage(message.AccountState_and_TX, aByte), cphm.pbftNode.ip_nodeTable[i][nid])
			}
		}
	}
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}
func (cphm *CLPAPbftInsideExtraHandleMod) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}
func (cphm *CLPAPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	shadowCapsules := make([]message.ShadowCapsule, 0)
	dualReceipts := make([]message.DualAnchorReceipt, 0)
	rvcs := make([]*message.ReshardingValidityCertificate, 0)
	algorithm := "CLPA"
	stage := ""
	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)
		shadowCapsules = append(shadowCapsules, at.ShadowCapsules...)
		dualReceipts = append(dualReceipts, at.DualReceipts...)
		if at.RVC != nil {
			rvcs = append(rvcs, at.RVC)
		}
		if at.Algorithm != "" {
			algorithm = at.Algorithm
		}
		if at.Stage != "" {
			stage = at.Stage
		}
	}
	cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(cphm.cdm.ReceivedNewTx)
	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{ModifiedMap: cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound], Addrs: atmaddr, AccountState: atmAs, ShadowCapsules: shadowCapsules, DualReceipts: dualReceipts, RVCs: rvcs, Algorithm: algorithm, Stage: stage, ATid: uint64(len(cphm.cdm.ModifiedMap))}
	atmbyte := atm.Encode()
	return true, &message.Request{RequestType: message.PartitionReq, Msg: message.RawMessage{Content: atmbyte}, ReqTime: time.Now()}
}
func (cphm *CLPAPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	if atm.Algorithm == "ZKSCAR" && !validateAccountTransferRVCs(atm) {
		log.Panic("ZK-SCAR RVC validation failed")
	}
	for key, val := range atm.ModifiedMap {
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())
	if atm.Algorithm == "ZKSCAR" {
		shadowRoot := stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
		atm.DualReceipts = bindShadowRootToReceipts(atm.DualReceipts, shadowRoot)
		groupedCaps := groupShadowCapsulesByRVCID(atm.ShadowCapsules)
		for _, rvc := range atm.RVCs {
			caps := groupedCaps[rvc.CertificateID]
			rvc.TargetShadowRoot = shadowRoot
			rvc.TargetShadowRootType = "mpt-state-root"
			shadowWitnesses, err := buildShadowWitnessesForInstalledAccounts(cphm.pbftNode, rvc, caps)
			if err != nil {
				log.Panic(err)
			}
			rvc.ShadowWitnesses = shadowWitnesses
			if !validateInstalledShadowAccounts(cphm.pbftNode, rvc, caps) {
				log.Panic("ZK-SCAR shadow account installation validation failed")
			}
			cphm.cdm.RVCPool[rvc.CertificateID] = rvc
		}
		for _, cap := range atm.ShadowCapsules {
			cp := cap
			cphm.cdm.ShadowCapsulePool[cap.Addr] = &cp
			cphm.cdm.OwnershipTransferred[cap.Addr] = true
			cphm.cdm.HydratedAccounts[cap.Addr] = false
			cphm.cdm.ShadowInstallHeight[cap.Addr] = currentStateHeight(cphm.pbftNode)
		}
		indexDualAnchorReceipts(cphm.cdm, atm.DualReceipts)
		issueHydrationRequests(cphm.pbftNode, cphm.cdm, atm.ShadowCapsules)
	}
	if uint64(len(cphm.cdm.ModifiedMap)) != atm.ATid {
		cphm.cdm.ModifiedMap = append(cphm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	cphm.cdm.AccountTransferRound = atm.ATid
	cphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cphm.cdm.PartitionOn = false
	cphm.cdm.CollectLock.Lock()
	cphm.cdm.CollectOver = false
	cphm.cdm.CollectLock.Unlock()
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady = make(map[uint64]bool)
	cphm.cdm.P_ReadyLock.Unlock()
	cphm.cdm.ReadySeqLock.Lock()
	cphm.cdm.ReadySeq = make(map[uint64]uint64)
	cphm.cdm.ReadySeqLock.Unlock()
}
