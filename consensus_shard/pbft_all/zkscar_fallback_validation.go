package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/hex"
	"log"
)

func buildIncomingAccountStateIndex(addrs []string, states []*core.AccountState) (map[string]*core.AccountState, bool) {
	if len(addrs) != len(states) {
		log.Printf("ZK-SCAR fallback: addrs/state length mismatch addrs=%d states=%d", len(addrs), len(states))
		return nil, false
	}
	index := make(map[string]*core.AccountState, len(addrs))
	for i, addr := range addrs {
		if addr == "" {
			log.Printf("ZK-SCAR fallback: empty addr at index=%d", i)
			return nil, false
		}
		if _, ok := index[addr]; ok {
			log.Printf("ZK-SCAR fallback: duplicate incoming state addr=%s", addr)
			return nil, false
		}
		index[addr] = states[i]
	}
	return index, true
}

func logShadowStateMismatch(prefix string, state *core.AccountState, cap message.ShadowCapsule, rvcID string) {
	if state == nil {
		log.Printf("%s: state missing for addr=%s cert=%s", prefix, cap.Addr, rvcID)
		return
	}
	if !state.IsShadow() {
		log.Printf("%s: state is not shadow addr=%s cert=%s transferred=%v pending=%v hydrated=%v retired=%v",
			prefix, cap.Addr, rvcID, state.OwnershipTransferred, state.PendingHydration, state.Hydrated, state.Retired)
	}
	if state.Balance == nil {
		log.Printf("%s: nil balance addr=%s cert=%s", prefix, cap.Addr, rvcID)
		return
	}
	if state.Balance.String() != cap.Balance {
		log.Printf("%s: balance mismatch addr=%s cert=%s state=%s cap=%s", prefix, cap.Addr, rvcID, state.Balance.String(), cap.Balance)
	}
	if state.Nonce != cap.Nonce {
		log.Printf("%s: nonce mismatch addr=%s cert=%s state=%d cap=%d", prefix, cap.Addr, rvcID, state.Nonce, cap.Nonce)
	}
	if hex.EncodeToString(state.CodeHash) != hex.EncodeToString(cap.CodeHash) {
		log.Printf("%s: codeHash mismatch addr=%s cert=%s", prefix, cap.Addr, rvcID)
	}
	if hex.EncodeToString(state.StorageRoot) != hex.EncodeToString(cap.StorageRoot) {
		log.Printf("%s: storageRoot mismatch addr=%s cert=%s", prefix, cap.Addr, rvcID)
	}
	if hex.EncodeToString(state.DebtRoot) != hex.EncodeToString(cap.DebtRoot) {
		log.Printf("%s: debtRoot mismatch addr=%s cert=%s state=%s cap=%s",
			prefix, cap.Addr, rvcID, hex.EncodeToString(state.DebtRoot), hex.EncodeToString(cap.DebtRoot))
	}
	if state.EpochTag != cap.EpochTag || state.SourceShard != cap.CurrentShard || state.TargetShard != cap.TargetShard {
		log.Printf("%s: epoch/shard mismatch addr=%s cert=%s stateEpoch=%d capEpoch=%d stateSrc=%d capSrc=%d stateDst=%d capDst=%d",
			prefix, cap.Addr, rvcID, state.EpochTag, cap.EpochTag, state.SourceShard, cap.CurrentShard, state.TargetShard, cap.TargetShard)
	}
	if state.LastRVC != rvcID {
		log.Printf("%s: LastRVC mismatch addr=%s state=%s cert=%s", prefix, cap.Addr, state.LastRVC, rvcID)
	}
}

func normalizeIncomingZKSCARTransfer(atm *message.AccountTransferMsg) {
	if atm == nil || atm.Algorithm != "ZKSCAR" {
		return
	}
	if atm.ModifiedMap == nil {
		atm.ModifiedMap = make(map[string]uint64)
	}
	for _, cap := range atm.ShadowCapsules {
		if cap.Addr == "" {
			continue
		}
		if target, ok := atm.ModifiedMap[cap.Addr]; !ok {
			atm.ModifiedMap[cap.Addr] = cap.TargetShard
			log.Printf("ZK-SCAR normalize: backfilled modifiedMap addr=%s target=%d cert=%s", cap.Addr, cap.TargetShard, cap.RVCID)
		} else if target != cap.TargetShard {
			log.Printf("ZK-SCAR normalize: corrected modifiedMap addr=%s oldTarget=%d newTarget=%d cert=%s", cap.Addr, target, cap.TargetShard, cap.RVCID)
			atm.ModifiedMap[cap.Addr] = cap.TargetShard
		}
	}
}

func validateIncomingZKSCARTransferFallback(atm *message.AccountTransferMsg) bool {
	if atm == nil {
		log.Printf("ZK-SCAR fallback: nil account transfer msg")
		return false
	}
	if atm.Algorithm != "ZKSCAR" {
		return true
	}
	if len(atm.ShadowCapsules) == 0 {
		log.Printf("ZK-SCAR fallback: empty shadow capsule batch")
		return false
	}
	if len(atm.RVCs) == 0 {
		log.Printf("ZK-SCAR fallback: empty RVC list")
		return false
	}

	stateIndex, ok := buildIncomingAccountStateIndex(atm.Addrs, atm.AccountState)
	if !ok {
		return false
	}

	rvcByID := make(map[string]*message.ReshardingValidityCertificate, len(atm.RVCs))
	for _, rvc := range atm.RVCs {
		if rvc == nil {
			log.Printf("ZK-SCAR fallback: nil rvc in batch")
			return false
		}
		if rvc.CertificateID == "" {
			log.Printf("ZK-SCAR fallback: empty certificate id")
			return false
		}
		if _, exists := rvcByID[rvc.CertificateID]; exists {
			log.Printf("ZK-SCAR fallback: duplicate certificate id=%s", rvc.CertificateID)
			return false
		}
		rvcByID[rvc.CertificateID] = rvc
	}

	groupedCaps := groupShadowCapsulesByRVCID(atm.ShadowCapsules)
	seenCaps := make(map[string]bool, len(atm.ShadowCapsules))

	for _, cap := range atm.ShadowCapsules {
		if cap.Addr == "" {
			log.Printf("ZK-SCAR fallback: empty capsule addr")
			return false
		}
		if seenCaps[cap.Addr] {
			log.Printf("ZK-SCAR fallback: duplicate capsule addr=%s", cap.Addr)
			return false
		}
		seenCaps[cap.Addr] = true

		rvc, ok := rvcByID[cap.RVCID]
		if !ok {
			log.Printf("ZK-SCAR fallback: capsule addr=%s references missing cert=%s", cap.Addr, cap.RVCID)
			return false
		}
		if cap.CurrentShard != rvc.FromShard || cap.TargetShard != rvc.ToShard || cap.EpochTag != rvc.EpochTag {
			log.Printf("ZK-SCAR fallback: capsule metadata mismatch addr=%s cert=%s capSrc=%d rvcSrc=%d capDst=%d rvcDst=%d capEpoch=%d rvcEpoch=%d",
				cap.Addr, cap.RVCID, cap.CurrentShard, rvc.FromShard, cap.TargetShard, rvc.ToShard, cap.EpochTag, rvc.EpochTag)
			return false
		}
		if target, ok := atm.ModifiedMap[cap.Addr]; !ok || target != cap.TargetShard {
			log.Printf("ZK-SCAR fallback: modifiedMap mismatch addr=%s cert=%s mapTarget=%d capTarget=%d present=%v",
				cap.Addr, cap.RVCID, target, cap.TargetShard, ok)
			return false
		}
		state := stateIndex[cap.Addr]
		if !shadowStateMatchesCapsule(state, cap, cap.RVCID) {
			logShadowStateMismatch("ZK-SCAR fallback incoming-state mismatch", state, cap, cap.RVCID)
			return false
		}
	}

	for _, rvc := range atm.RVCs {
		caps := groupedCaps[rvc.CertificateID]
		if len(caps) == 0 {
			log.Printf("ZK-SCAR fallback: no capsules grouped for cert=%s", rvc.CertificateID)
			return false
		}
		if rvc.Algorithm != "" && rvc.Algorithm != "ZKSCAR" {
			log.Printf("ZK-SCAR fallback: algorithm mismatch cert=%s got=%s", rvc.CertificateID, rvc.Algorithm)
			return false
		}
		if rvc.BatchSize != 0 && uint64(len(caps)) != rvc.BatchSize {
			log.Printf("ZK-SCAR fallback: batch size mismatch cert=%s caps=%d batch=%d", rvc.CertificateID, len(caps), rvc.BatchSize)
			return false
		}
	}

	for _, receipt := range atm.DualReceipts {
		if receipt.RVCID == "" {
			continue
		}
		rvc, ok := rvcByID[receipt.RVCID]
		if !ok {
			log.Printf("ZK-SCAR fallback: receipt references missing cert=%s", receipt.RVCID)
			return false
		}
		if receipt.FromShard != rvc.FromShard || receipt.ToShard != rvc.ToShard || receipt.EpochTag != rvc.EpochTag {
			log.Printf("ZK-SCAR fallback: dual receipt mismatch cert=%s from=%d/%d to=%d/%d epoch=%d/%d",
				receipt.RVCID, receipt.FromShard, rvc.FromShard, receipt.ToShard, rvc.ToShard, receipt.EpochTag, rvc.EpochTag)
			return false
		}
	}

	return true
}

func validateInstalledShadowAccountsFallback(pbftNode *PbftConsensusNode, rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if pbftNode == nil || pbftNode.CurChain == nil || rvc == nil {
		log.Printf("ZK-SCAR installed fallback: nil node/chain/rvc")
		return false
	}
	if len(capsules) == 0 {
		log.Printf("ZK-SCAR installed fallback: empty capsule batch cert=%s", rvc.CertificateID)
		return false
	}
	for _, cap := range capsules {
		current := pbftNode.CurChain.GetAccountState(cap.Addr)
		if !shadowStateMatchesCapsule(current, cap, rvc.CertificateID) {
			logShadowStateMismatch("ZK-SCAR installed fallback mismatch", current, cap, rvc.CertificateID)
			return false
		}
	}
	return true
}
