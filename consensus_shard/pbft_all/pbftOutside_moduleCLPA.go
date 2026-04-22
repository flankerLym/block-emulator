package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
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
	case message.CHydrationRequest:
		crom.handleHydrationRequest(content)
	case message.CHydrationData:
		crom.handleHydrationData(content)
	case message.CRetirementProof:
		crom.handleRetirementProof(content)
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
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
}

func (crom *CLPARelayOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	}
	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
}

func (crom *CLPARelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
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
		// IMPORTANT:
		// The rest of the ZK-SCAR pipeline indexes receipts with receiptKey(txHash),
		// which is hex-encoding based. Using string(receipt.TxHash) here creates a
		// different key-space and breaks later debt / retirement lookups.
		// Reuse the same helper to keep the indexing contract consistent.
		indexDualAnchorReceipts(crom.cdm, at.DualReceipts)
	}
	if len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1 {
		crom.cdm.CollectLock.Lock()
		crom.cdm.CollectOver = true
		crom.cdm.CollectLock.Unlock()
	}
}

func (crom *CLPARelayOutsideModule) handleHydrationRequest(content []byte) {
	req := new(message.HydrationRequest)
	if err := json.Unmarshal(content, req); err != nil {
		log.Panic(err)
	}
	handleHydrationRequestCommon(crom.pbftNode, crom.cdm, req)
}

func (crom *CLPARelayOutsideModule) handleHydrationData(content []byte) {
	data := new(message.HydrationData)
	if err := json.Unmarshal(content, data); err != nil {
		log.Panic(err)
	}
	handleHydrationDataCommon(crom.pbftNode, crom.cdm, data)
}

func (crom *CLPARelayOutsideModule) handleRetirementProof(content []byte) {
	proof := new(message.RetirementProof)
	if err := json.Unmarshal(content, proof); err != nil {
		log.Panic(err)
	}
	handleRetirementProofCommon(crom.pbftNode, crom.cdm, proof)
}
