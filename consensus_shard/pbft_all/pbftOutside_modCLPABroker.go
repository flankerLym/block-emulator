package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

type CLPABrokerOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (cbom *CLPABrokerOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CSeqIDinfo:
		cbom.handleSeqIDinfos(content)
	case message.CInject:
		cbom.handleInjectTx(content)
	case message.CPartitionMsg:
		cbom.handlePartitionMsg(content)
	case message.CAccountTransferMsg_broker:
		cbom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		cbom.handlePartitionReady(content)
	case message.CAccountHydration:
		cbom.handleAccountHydrationMsg(content)
	case message.CRetirementProof:
		cbom.handleRetirementProofMsg(content)
	default:
	}
	return true
}

func (cbom *CLPABrokerOutsideModule) handleSeqIDinfos(content []byte) {
	sii := new(message.SeqIDinfo)
	err := json.Unmarshal(content, sii)
	if err != nil {
		log.Panic(err)
	}
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : has received SeqIDinfo from shard %d, the senderSeq is %d\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, sii.SenderShardID, sii.SenderSeq)
	cbom.pbftNode.seqMapLock.Lock()
	cbom.pbftNode.seqIDMap[sii.SenderShardID] = sii.SenderSeq
	cbom.pbftNode.seqMapLock.Unlock()
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : has handled SeqIDinfo msg\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID)
}

func (cbom *CLPABrokerOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	cbom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, len(it.Txs))
}

func (cbom *CLPABrokerOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	if pm.Algorithm == "" {
		pm.Algorithm = "CLPA"
	}
	cbom.cdm.ModifiedMap = append(cbom.cdm.ModifiedMap, pm.PartitionModified)
	cbom.cdm.PartitionMeta = append(cbom.cdm.PartitionMeta, *pm)
	for _, cap := range pm.ShadowCapsules {
		cp := cap
		cbom.cdm.ShadowCapsulePool[cap.Addr] = &cp
	}
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message, alg=%s, epochTag=%d\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, pm.Algorithm, pm.EpochTag)
	cbom.cdm.PartitionOn = true
}

func (cbom *CLPABrokerOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	cbom.cdm.P_ReadyLock.Lock()
	cbom.cdm.PartitionReady[pr.FromShard] = true
	cbom.cdm.P_ReadyLock.Unlock()

	cbom.pbftNode.seqMapLock.Lock()
	cbom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	cbom.pbftNode.seqMapLock.Unlock()

	cbom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

func (cbom *CLPABrokerOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	cbom.cdm.AccountStateTx[at.FromShard] = at
	if at.Algorithm == "ZKSCAR" {
		if at.RVC != nil {
			cbom.cdm.RVCPool[at.RVC.CertificateID] = at.RVC
		}
		for _, cap := range at.ShadowCapsules {
			cp := cap
			cbom.cdm.ShadowCapsulePool[cap.Addr] = &cp
		}
		for _, receipt := range at.DualReceipts {
			rc := receipt
			cbom.cdm.DualAnchorReceiptPool[string(receipt.TxHash)] = &rc
		}
	}
	cbom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, at.FromShard)

	if len(cbom.cdm.AccountStateTx) == int(cbom.pbftNode.pbftChainConfig.ShardNums)-1 {
		cbom.cdm.CollectLock.Lock()
		cbom.cdm.CollectOver = true
		cbom.cdm.CollectLock.Unlock()
		cbom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID)
	}
}

func (cbom *CLPABrokerOutsideModule) handleAccountHydrationMsg(content []byte) {
	hm := new(message.AccountHydrationMsg)
	err := json.Unmarshal(content, hm)
	if err != nil {
		log.Panic(err)
	}
	if hm.Algorithm != "ZKSCAR" || hm.ToShard != cbom.pbftNode.ShardID {
		return
	}
	if hm.RVC == nil || !validateRVCBatch(hm.RVC, hm.ShadowCapsules) {
		log.Panic("invalid ZK-SCAR hydration message")
	}

	finalAddrs := make([]string, 0, len(hm.Addrs))
	finalStates := make([]*core.AccountState, 0, len(hm.Addrs))
	for i, addr := range hm.Addrs {
		if i >= len(hm.AccountState) || hm.AccountState[i] == nil {
			continue
		}
		fullState := hm.AccountState[i].FinalizeHydration(hm.EpochTag)
		fullState.LastRVC = hm.RVC.CertificateID
		fullState.OwnershipTransferred = true
		fullState.PendingHydration = false
		fullState.Hydrated = true
		if cap, ok := cbom.cdm.ShadowCapsulePool[addr]; ok {
			fullState.SourceShard = cap.CurrentShard
			fullState.TargetShard = cap.TargetShard
			fullState.DebtRoot = append([]byte(nil), cap.DebtRoot...)
		}
		finalAddrs = append(finalAddrs, addr)
		finalStates = append(finalStates, fullState)
		cbom.cdm.HydratedAccounts[addr] = true
	}
	if len(finalAddrs) == 0 {
		return
	}

	cbom.pbftNode.CurChain.UpsertAccountsFull(finalAddrs, finalStates)

	if cbom.pbftNode.NodeID == uint64(cbom.pbftNode.view.Load()) {
		rp := buildRetirementProof(hm.EpochTag, hm.FromShard, hm.ToShard, finalAddrs, hm.RVC.CertificateID)
		rb, err := json.Marshal(rp)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CRetirementProof, rb)
		broadcastToShard(cbom.pbftNode, hm.FromShard, sendMsg)
	}
}

func (cbom *CLPABrokerOutsideModule) handleRetirementProofMsg(content []byte) {
	rp := new(message.RetirementProof)
	err := json.Unmarshal(content, rp)
	if err != nil {
		log.Panic(err)
	}
	if rp.Algorithm != "ZKSCAR" || rp.FromShard != cbom.pbftNode.ShardID {
		return
	}
	if !validateRetirementProof(rp) {
		log.Panic("invalid retirement proof")
	}

	retireAddrs := make([]string, 0, len(rp.Addrs))
	for _, addr := range rp.Addrs {
		if cbom.pbftNode.CurChain.Get_PartitionMap(addr) != cbom.pbftNode.ShardID {
			retireAddrs = append(retireAddrs, addr)
		}
	}
	if len(retireAddrs) > 0 {
		cbom.pbftNode.CurChain.DeleteAccounts(retireAddrs)
	}
	cbom.cdm.RetirementProofPool[rp.RVCID] = rp
}
