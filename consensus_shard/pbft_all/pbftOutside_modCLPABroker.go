package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
)

// This module used in the blockChain using Broker mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
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

	// messages about CLPA
	case message.CPartitionMsg:
		cbom.handlePartitionMsg(content)
	case message.CAccountTransferMsg_broker:
		cbom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		cbom.handlePartitionReady(content)
	case message.CShadowCapsule:
		cbom.handleShadowCapsule(content)
	default:
	}
	return true
}

// receive SeqIDinfo
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

func (cbom *CLPABrokerOutsideModule) desiredShardForInject(tx *core.Transaction) uint64 {
	if tx.RawTxHash != nil {
		if tx.FinalRecipient == tx.Recipient {
			return cbom.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
		}
		if tx.OriginalSender == tx.Sender {
			return cbom.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
		}
	}
	if tx.SenderIsBroker {
		return cbom.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
	}
	return cbom.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
}

func (cbom *CLPABrokerOutsideModule) forwardInjectBatch(toShard uint64, txs []*core.Transaction) {
	if len(txs) == 0 {
		return
	}
	it := message.InjectTxs{
		Txs:       txs,
		ToShardID: toShard,
	}
	itByte, err := json.Marshal(it)
	if err != nil {
		log.Panic(err)
	}
	sendMsg := message.MergeMessage(message.CInject, itByte)
	go networks.TcpDial(sendMsg, cbom.pbftNode.ip_nodeTable[toShard][0])
}

func (cbom *CLPABrokerOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}

	localTxs := make([]*core.Transaction, 0, len(it.Txs))
	forward := make(map[uint64][]*core.Transaction)
	for _, tx := range it.Txs {
		target := cbom.desiredShardForInject(tx)
		if target == cbom.pbftNode.ShardID {
			localTxs = append(localTxs, tx)
		} else {
			forward[target] = append(forward[target], tx)
		}
	}
	if len(localTxs) > 0 {
		cbom.pbftNode.CurChain.Txpool.AddTxs2Pool(localTxs)
	}
	for sid, txs := range forward {
		cbom.forwardInjectBatch(sid, txs)
	}
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : handled injected txs msg, local=%d forwarded=%d \n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, len(localTxs), len(it.Txs)-len(localTxs))
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (cbom *CLPABrokerOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	cbom.cdm.ModifiedMap = append(cbom.cdm.ModifiedMap, pm.PartitionModified)
	cbom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID)
	cbom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
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

// when the message from other shard arriving, it should be added into the message pool
func (cbom *CLPABrokerOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	cbom.cdm.AccountStateTx[at.FromShard] = at
	cbom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, at.FromShard)

	if len(cbom.cdm.AccountStateTx) == int(cbom.pbftNode.pbftChainConfig.ShardNums)-1 {
		cbom.cdm.CollectLock.Lock()
		cbom.cdm.CollectOver = true
		cbom.cdm.CollectLock.Unlock()
		cbom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID)
	}
}

func (cbom *CLPABrokerOutsideModule) handleShadowCapsule(content []byte) {
	batch := new(message.ShadowCapsuleBatch)
	err := json.Unmarshal(content, batch)
	if err != nil {
		log.Panic(err)
	}
	for _, capsule := range batch.Capsules {
		cbom.pbftNode.CurChain.InstallShadowCapsule(
			capsule.Addr,
			capsule.SourceShard,
			capsule.TargetShard,
			capsule.Balance,
			capsule.Nonce,
			capsule.CodeHash,
			capsule.StorageRoot,
			capsule.EpochTag,
		)
	}
	cbom.pbftNode.pl.Plog.Printf("S%dN%d installed %d shadow capsules for epoch %d\n", cbom.pbftNode.ShardID, cbom.pbftNode.NodeID, len(batch.Capsules), batch.EpochTag)
}
