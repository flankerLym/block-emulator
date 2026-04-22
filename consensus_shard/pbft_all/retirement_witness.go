package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"math/big"
	"sort"
	"strconv"
)

type retirementWitnessBundle struct {
	SettledReceiptKeys     []string `json:"settled_receipt_keys"`
	OutstandingReceiptKeys []string `json:"outstanding_receipt_keys"`
	PostCutoverWriteKeys   []string `json:"post_cutover_write_keys"`
}

func uniqueSortedStrings(items []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func collectReceiptKeysForAddr(cdm *dataSupport.Data_supportCLPA, addr string) ([]string, []string) {
	if cdm == nil {
		return nil, nil
	}
	keys, ok := cdm.AddressReceiptIndex[addr]
	if !ok {
		return []string{}, []string{}
	}
	settled := make([]string, 0)
	outstanding := make([]string, 0)
	for key := range keys {
		if cdm.SettledDualAnchorReceipts[key] {
			settled = append(settled, key)
		} else {
			outstanding = append(outstanding, key)
		}
	}
	return uniqueSortedStrings(settled), uniqueSortedStrings(outstanding)
}

func collectPostCutoverWriteKeysForAddr(cdm *dataSupport.Data_supportCLPA, addr string) []string {
	if cdm == nil {
		return []string{}
	}
	keys, ok := cdm.PostCutoverWriteSet[addr]
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return uniqueSortedStrings(out)
}

func buildRetirementDebtWitnessDigest(settledKeys, outstandingKeys []string) string {
	vals := make([]*big.Int, 0, 2*(len(settledKeys)+len(outstandingKeys)))
	for _, key := range uniqueSortedStrings(settledKeys) {
		vals = append(vals, fieldFromString(key), big.NewInt(1))
	}
	for _, key := range uniqueSortedStrings(outstandingKeys) {
		vals = append(vals, fieldFromString(key), big.NewInt(0))
	}
	return mimcChain(vals).String()
}

func buildNoWriteWitnessDigest(writeKeys []string) string {
	keys := uniqueSortedStrings(writeKeys)
	vals := make([]*big.Int, 0, len(keys))
	for _, key := range keys {
		vals = append(vals, fieldFromString("write|"+key))
	}
	return mimcChain(vals).String()
}

func buildRetirementWitnessDigest(addressBinding string, epochTag, fromShard, toShard, settledCount, outstandingCount, writeCount uint64, debtDigest, noWriteDigest, rvcBinding string) string {
	vals := []*big.Int{
		fieldFromDecimalString(addressBinding),
		new(big.Int).SetUint64(epochTag),
		new(big.Int).SetUint64(fromShard),
		new(big.Int).SetUint64(toShard),
		new(big.Int).SetUint64(settledCount),
		new(big.Int).SetUint64(outstandingCount),
		new(big.Int).SetUint64(writeCount),
		fieldFromDecimalString(debtDigest),
		fieldFromDecimalString(noWriteDigest),
		fieldFromDecimalString(rvcBinding),
	}
	return mimcChain(vals).String()
}

func buildRetirementWitnessBundle(cdm *dataSupport.Data_supportCLPA, addr string) *retirementWitnessBundle {
	settled, outstanding := collectReceiptKeysForAddr(cdm, addr)
	writes := collectPostCutoverWriteKeysForAddr(cdm, addr)
	return &retirementWitnessBundle{
		SettledReceiptKeys:     settled,
		OutstandingReceiptKeys: outstanding,
		PostCutoverWriteKeys:   writes,
	}
}

func expectedRetirementPublicInputs(proof *message.RetirementProof) []string {
	hydratedFlag := "0"
	if proof.Hydrated {
		hydratedFlag = "1"
	}
	debtRootClearedFlag := "0"
	if proof.DebtRootCleared {
		debtRootClearedFlag = "1"
	}
	return []string{
		proof.AddressBinding,
		strconv.FormatUint(proof.EpochTag, 10),
		strconv.FormatUint(proof.FromShard, 10),
		strconv.FormatUint(proof.ToShard, 10),
		hydratedFlag,
		debtRootClearedFlag,
		strconv.FormatUint(proof.SettledReceiptCount, 10),
		strconv.FormatUint(proof.OutstandingReceiptCount, 10),
		strconv.FormatUint(proof.PostCutoverWriteCount, 10),
		proof.DebtWitnessDigest,
		proof.NoWriteWitnessDigest,
		proof.RetirementWitnessDigest,
		proof.RVCBinding,
	}
}

func validateRetirementProofAgainstState(cdm *dataSupport.Data_supportCLPA, proof *message.RetirementProof, cap *message.ShadowCapsule) bool {
	if cdm == nil || proof == nil || cap == nil {
		return false
	}
	if proof.ProtocolVersion != "zkscar-retirement-v1" || proof.CircuitVersion != "retirement-finality-groth16-v1" {
		return false
	}
	if proof.VerifierKeyID != "zkscar-retirement-groth16-v1" {
		return false
	}
	if proof.Addr != cap.Addr || proof.RVCID != cap.RVCID {
		return false
	}
	settled, outstanding := collectReceiptKeysForAddr(cdm, proof.Addr)
	writes := collectPostCutoverWriteKeysForAddr(cdm, proof.Addr)
	addressBinding := buildCertificateBinding(proof.Addr)
	rvcBinding := buildCertificateBinding(proof.RVCID)
	debtDigest := buildRetirementDebtWitnessDigest(settled, outstanding)
	noWriteDigest := buildNoWriteWitnessDigest(writes)
	retirementDigest := buildRetirementWitnessDigest(
		addressBinding,
		proof.EpochTag,
		proof.FromShard,
		proof.ToShard,
		uint64(len(settled)),
		uint64(len(outstanding)),
		uint64(len(writes)),
		debtDigest,
		noWriteDigest,
		rvcBinding,
	)
	if proof.AddressBinding != addressBinding || proof.RVCBinding != rvcBinding {
		return false
	}
	if proof.SettledReceiptCount != uint64(len(settled)) || proof.OutstandingReceiptCount != uint64(len(outstanding)) || proof.PostCutoverWriteCount != uint64(len(writes)) {
		return false
	}
	if proof.DebtWitnessDigest != debtDigest || proof.NoWriteWitnessDigest != noWriteDigest || proof.RetirementWitnessDigest != retirementDigest {
		return false
	}
	if !proof.Hydrated || !proof.DebtRootCleared || proof.OutstandingReceiptCount != 0 || proof.PostCutoverWriteCount != 0 {
		return false
	}
	expectedInputs := expectedRetirementPublicInputs(proof)
	if len(expectedInputs) != len(proof.PublicInputs) {
		return false
	}
	for i := range expectedInputs {
		if expectedInputs[i] != proof.PublicInputs[i] {
			return false
		}
	}
	return true
}
