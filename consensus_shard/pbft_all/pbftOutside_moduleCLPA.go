package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

type CLPARelayOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (crom *CLPARelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		crom.handleRelay(content)
	case message.CRelayWithProof:
		crom.handleRelayWithProof(content)
	case message.CInject:
		crom.handleInjectTx(content)
	case message.CPartitionMsg:
		crom.handlePartitionMsg(content)
	case message.AccountState_and_TX:
		crom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		crom.handlePartitionReady(content)
	case message.CAccountHydration:
		crom.handleAccountHydrationMsg(content)
	case message.CRetirementProof:
		crom.handleRetirementProofMsg(content)
	default:
	}
	return true
}

func (crom *CLPARelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *CLPARelayOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	} else {
		crom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *CLPARelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, len(it.Txs))
}

func (crom *CLPARelayOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	if pm.Algorithm == "" {
		pm.Algorithm = "CLPA"
	}
	crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pm.PartitionModified)
	crom.cdm.PartitionMeta = append(crom.cdm.PartitionMeta, *pm)
	for _, cap := range pm.ShadowCapsules {
		cp := cap
		crom.cdm.ShadowCapsulePool[cap.Addr] = &cp
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message, alg=%s, epochTag=%d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, pm.Algorithm, pm.EpochTag)
	crom.cdm.PartitionOn = true
}

func (crom *CLPARelayOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	crom.cdm.P_ReadyLock.Lock()
	crom.cdm.PartitionReady[pr.FromShard] = true
	crom.cdm.P_ReadyLock.Unlock()

	crom.pbftNode.seqMapLock.Lock()
	crom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	crom.pbftNode.seqMapLock.Unlock()

	crom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

func (crom *CLPARelayOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	crom.cdm.AccountStateTx[at.FromShard] = at
	if at.Algorithm == "ZKSCAR" {
		if at.RVC != nil {
			crom.cdm.RVCPool[at.RVC.CertificateID] = at.RVC
		}
		for _, cap := range at.ShadowCapsules {
			cp := cap
			crom.cdm.ShadowCapsulePool[cap.Addr] = &cp
		}
		for _, receipt := range at.DualReceipts {
			rc := receipt
			crom.cdm.DualAnchorReceiptPool[string(receipt.TxHash)] = &rc
		}
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, at.FromShard)

	if len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1 {
		crom.cdm.CollectLock.Lock()
		crom.cdm.CollectOver = true
		crom.cdm.CollectLock.Unlock()
		crom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	}
}

func (crom *CLPARelayOutsideModule) handleAccountHydrationMsg(content []byte) {
	hm := new(message.AccountHydrationMsg)
	err := json.Unmarshal(content, hm)
	if err != nil {
		log.Panic(err)
	}
	if hm.Algorithm != "ZKSCAR" || hm.ToShard != crom.pbftNode.ShardID {
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
		if cap, ok := crom.cdm.ShadowCapsulePool[addr]; ok {
			fullState.SourceShard = cap.CurrentShard
			fullState.TargetShard = cap.TargetShard
			fullState.DebtRoot = append([]byte(nil), cap.DebtRoot...)
		}
		finalAddrs = append(finalAddrs, addr)
		finalStates = append(finalStates, fullState)
		crom.cdm.HydratedAccounts[addr] = true
	}
	if len(finalAddrs) == 0 {
		return
	}

	crom.pbftNode.CurChain.UpsertAccountsFull(finalAddrs, finalStates)

	if crom.pbftNode.NodeID == uint64(crom.pbftNode.view.Load()) {
		rp := buildRetirementProof(hm.EpochTag, hm.FromShard, hm.ToShard, finalAddrs, hm.RVC.CertificateID)
		rb, err := json.Marshal(rp)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CRetirementProof, rb)
		broadcastToShard(crom.pbftNode, hm.FromShard, sendMsg)
	}
}

func (crom *CLPARelayOutsideModule) handleRetirementProofMsg(content []byte) {
	rp := new(message.RetirementProof)
	err := json.Unmarshal(content, rp)
	if err != nil {
		log.Panic(err)
	}
	if rp.Algorithm != "ZKSCAR" || rp.FromShard != crom.pbftNode.ShardID {
		return
	}
	if !validateRetirementProof(rp) {
		log.Panic("invalid retirement proof")
	}

	retireAddrs := make([]string, 0, len(rp.Addrs))
	for _, addr := range rp.Addrs {
		if crom.pbftNode.CurChain.Get_PartitionMap(addr) != crom.pbftNode.ShardID {
			retireAddrs = append(retireAddrs, addr)
		}
	}
	if len(retireAddrs) > 0 {
		crom.pbftNode.CurChain.DeleteAccounts(retireAddrs)
	}
	crom.cdm.RetirementProofPool[rp.RVCID] = rp
}
