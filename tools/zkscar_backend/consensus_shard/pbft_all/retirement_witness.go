package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/tools/zkscar_backend/consensus_shard/pbft_all/dataSupport"
	"math/big"
	"sort"
)

const (
	retirementProtocolVersion = "zkscar-retirement-v1"
	retirementCircuitVersion  = "retirement-finality-groth16-v1"
	retirementVerifierKeyID   = "zkscar-retirement-groth16-v1"
	retirementMaxReceipts     = 64
	retirementMaxWrites       = 32
)

type retirementReceiptWitness struct {
	Key     string `json:"key"`
	Settled bool   `json:"settled"`
}

func sortedStringKeys(set map[string]bool) []string {
	if len(set) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(set))
	for k, ok := range set {
		if ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func collectRetirementReceiptWitnesses(cdm *dataSupport.Data_supportCLPA, addr string) []retirementReceiptWitness {
	keys, ok := cdm.AddressReceiptIndex[addr]
	if !ok || len(keys) == 0 {
		return []retirementReceiptWitness{}
	}
	sorted := sortedStringKeys(keys)
	out := make([]retirementReceiptWitness, 0, len(sorted))
	for _, key := range sorted {
		out = append(out, retirementReceiptWitness{
			Key:     key,
			Settled: cdm.SettledDualAnchorReceipts[key],
		})
	}
	return out
}

func buildRetirementDebtWitnessDigest(epochTag, fromShard, toShard uint64, accountBinding, rvcBinding string, receipts []retirementReceiptWitness) string {
	vals := []*big.Int{
		new(big.Int).SetUint64(epochTag),
		new(big.Int).SetUint64(fromShard),
		new(big.Int).SetUint64(toShard),
		fieldFromDecimalString(accountBinding),
		fieldFromDecimalString(rvcBinding),
	}
	for i := 0; i < retirementMaxReceipts; i++ {
		active := big.NewInt(0)
		keyField := big.NewInt(0)
		settled := big.NewInt(0)
		if i < len(receipts) {
			active = big.NewInt(1)
			keyField = fieldFromString(receipts[i].Key)
			if receipts[i].Settled {
				settled = big.NewInt(1)
			}
		}
		vals = append(vals, active, keyField, settled)
	}
	return mimcChain(vals).String()
}

func buildRetirementNoWriteWitnessDigest(epochTag, fromShard, toShard uint64, accountBinding, rvcBinding string, writeKeys []string) string {
	vals := []*big.Int{
		new(big.Int).SetUint64(epochTag),
		new(big.Int).SetUint64(fromShard),
		new(big.Int).SetUint64(toShard),
		fieldFromDecimalString(accountBinding),
		fieldFromDecimalString(rvcBinding),
	}
	for i := 0; i < retirementMaxWrites; i++ {
		active := big.NewInt(0)
		keyField := big.NewInt(0)
		if i < len(writeKeys) {
			active = big.NewInt(1)
			keyField = fieldFromString(writeKeys[i])
		}
		vals = append(vals, active, keyField)
	}
	return mimcChain(vals).String()
}

func buildRetirementWitnessDigest(proof *message.RetirementProof) string {
	if proof == nil {
		return ""
	}
	hydrated := big.NewInt(0)
	if proof.Hydrated {
		hydrated = big.NewInt(1)
	}
	debtCleared := big.NewInt(0)
	if proof.DebtRootCleared {
		debtCleared = big.NewInt(1)
	}
	vals := []*big.Int{
		new(big.Int).SetUint64(proof.EpochTag),
		new(big.Int).SetUint64(proof.FromShard),
		new(big.Int).SetUint64(proof.ToShard),
		hydrated,
		debtCleared,
		new(big.Int).SetUint64(proof.SettledReceiptCount),
		new(big.Int).SetUint64(proof.OutstandingReceiptCount),
		new(big.Int).SetUint64(proof.PostCutoverWriteCount),
		fieldFromDecimalString(proof.DebtWitnessDigest),
		fieldFromDecimalString(proof.NoWriteWitnessDigest),
		fieldFromDecimalString(proof.AccountBinding),
		fieldFromDecimalString(proof.RVCBinding),
	}
	return mimcChain(vals).String()
}

func buildRetirementPublicInputs(proof *message.RetirementProof) []string {
	hydrated := "0"
	if proof.Hydrated {
		hydrated = "1"
	}
	debtCleared := "0"
	if proof.DebtRootCleared {
		debtCleared = "1"
	}
	return []string{
		uintToString(proof.EpochTag),
		uintToString(proof.FromShard),
		uintToString(proof.ToShard),
		hydrated,
		debtCleared,
		uintToString(proof.SettledReceiptCount),
		uintToString(proof.OutstandingReceiptCount),
		uintToString(proof.PostCutoverWriteCount),
		proof.DebtWitnessDigest,
		proof.NoWriteWitnessDigest,
		proof.RetirementWitnessDigest,
		proof.AccountBinding,
		proof.RVCBinding,
	}
}

func uintToString(v uint64) string {
	return new(big.Int).SetUint64(v).String()
}

func buildRetirementProofEnvelope(cdm *dataSupport.Data_supportCLPA, cap *message.ShadowCapsule, addr string, epochTag uint64) (*message.RetirementProof, []retirementReceiptWitness, bool) {
	if cdm == nil || cap == nil || addr == "" {
		return nil, nil, false
	}
	receipts := collectRetirementReceiptWitnesses(cdm, addr)
	if len(receipts) > retirementMaxReceipts {
		return nil, nil, false
	}
	accountBinding := hashTextToFieldString(addr)
	rvcBinding := buildCertificateBinding(cap.RVCID)
	outstanding := uint64(0)
	settled := uint64(0)
	for _, receipt := range receipts {
		if receipt.Settled {
			settled++
		} else {
			outstanding++
		}
	}
	writeKeys := []string{}
	proof := &message.RetirementProof{
		Addr:                    addr,
		EpochTag:                epochTag,
		FromShard:               cap.CurrentShard,
		ToShard:                 cap.TargetShard,
		Hydrated:                cdm.HydratedAccounts[addr],
		DebtRootCleared:         outstanding == 0,
		RVCID:                   cap.RVCID,
		ProtocolVersion:         retirementProtocolVersion,
		CircuitVersion:          retirementCircuitVersion,
		VerifierKeyID:           retirementVerifierKeyID,
		AccountBinding:          accountBinding,
		RVCBinding:              rvcBinding,
		SettledReceiptCount:     settled,
		OutstandingReceiptCount: outstanding,
		PostCutoverWriteCount:   0,
		DebtWitnessDigest:       buildRetirementDebtWitnessDigest(epochTag, cap.CurrentShard, cap.TargetShard, accountBinding, rvcBinding, receipts),
		NoWriteWitnessDigest:    buildRetirementNoWriteWitnessDigest(epochTag, cap.CurrentShard, cap.TargetShard, accountBinding, rvcBinding, writeKeys),
	}
	proof.RetirementWitnessDigest = buildRetirementWitnessDigest(proof)
	proof.PublicInputs = buildRetirementPublicInputs(proof)
	return proof, receipts, true
}
