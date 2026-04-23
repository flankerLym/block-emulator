package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
)

type CLPABrokerOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (cbom *CLPABrokerOutsideModule) forwardWithinShard(msgType message.MessageType, content []byte) {
	if cbom.pbftNode.NodeID != 0 {
		return
	}
	msgSend := message.MergeMessage(msgType, content)
	for nid := uint64(0); nid < cbom.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
		if nid == cbom.pbftNode.NodeID {
			continue
		}
		networks.TcpDial(msgSend, cbom.pbftNode.ip_nodeTable[cbom.pbftNode.ShardID][nid])
	}
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
	case message.CHydrationRequest:
		cbom.handleHydrationRequest(content)
	case message.CHydrationData:
		cbom.handleHydrationData(content)
	case message.CRetirementProof:
		cbom.handleRetirementProof(content)
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
	cbom.pbftNode.seqMapLock.Lock()
	cbom.pbftNode.seqIDMap[sii.SenderShardID] = sii.SenderSeq
	cbom.pbftNode.seqMapLock.Unlock()
}

func (cbom *CLPABrokerOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	cbom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	cbom.forwardWithinShard(message.CInject, content)
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

	// 阶段二控制面去重：
	// 现在 supervisor 直接向所有节点广播 partition map，而 node0 仍会做 shard 内转发。
	// 为避免 nodes 1..n 收到两份完全相同的 partition msg 后重复 append ModifiedMap，
	// 这里按 (Algorithm, EpochTag) 做幂等处理。
	if len(cbom.cdm.PartitionMeta) > 0 {
		last := cbom.cdm.PartitionMeta[len(cbom.cdm.PartitionMeta)-1]
		if last.Algorithm == pm.Algorithm && last.EpochTag == pm.EpochTag {
			cbom.cdm.PartitionOn = true
			return
		}
	}

	cbom.cdm.ModifiedMap = append(cbom.cdm.ModifiedMap, pm.PartitionModified)
	cbom.cdm.PartitionMeta = append(cbom.cdm.PartitionMeta, *pm)
	for _, cap := range pm.ShadowCapsules {
		cp := cap
		cbom.cdm.ShadowCapsulePool[cap.Addr] = &cp
	}
	cbom.cdm.PartitionOn = true
	cbom.forwardWithinShard(message.CPartitionMsg, content)
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
		indexDualAnchorReceipts(cbom.cdm, at.DualReceipts)
	}
	if len(cbom.cdm.AccountStateTx) == int(cbom.pbftNode.pbftChainConfig.ShardNums)-1 {
		cbom.cdm.CollectLock.Lock()
		cbom.cdm.CollectOver = true
		cbom.cdm.CollectLock.Unlock()
	}
}

func (cbom *CLPABrokerOutsideModule) handleHydrationRequest(content []byte) {
	req := new(message.HydrationRequest)
	if err := json.Unmarshal(content, req); err != nil {
		log.Panic(err)
	}
	handleHydrationRequestCommon(cbom.pbftNode, cbom.cdm, req)
}

func sameBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (cbom *CLPABrokerOutsideModule) handleHydrationData(content []byte) {
	data := new(message.HydrationData)
	if err := json.Unmarshal(content, data); err != nil {
		log.Panic(err)
	}

	cbom.cdm.HydrationLock.Lock()
	defer cbom.cdm.HydrationLock.Unlock()

	if cbom.cdm.HydratedAccounts[data.Addr] {
		return
	}

	if chunkMap, ok := cbom.cdm.PendingHydrationChunks[data.Addr]; ok {
		if old, exists := chunkMap[data.ChunkIndex]; exists {
			if sameBytes(old, data.ChunkPayload) {
				return
			}
			return
		}
	}

	handleHydrationDataCommon(cbom.pbftNode, cbom.cdm, data)
}

func (cbom *CLPABrokerOutsideModule) handleRetirementProof(content []byte) {
	proof := new(message.RetirementProof)
	if err := json.Unmarshal(content, proof); err != nil {
		log.Panic(err)
	}
	handleRetirementProofCommon(cbom.pbftNode, cbom.cdm, proof)
}
