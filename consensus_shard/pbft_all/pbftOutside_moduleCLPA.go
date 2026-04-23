package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
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

	// messages about CLPA
	case message.CPartitionMsg:
		crom.handlePartitionMsg(content)
	case message.AccountState_and_TX:
		crom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		crom.handlePartitionReady(content)
	case message.CShadowCapsule:
		crom.handleShadowCapsule(content)
	default:
	}
	return true
}

func (crom *CLPARelayOutsideModule) desiredShardForInject(tx *core.Transaction) uint64 {
	return crom.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
}

func (crom *CLPARelayOutsideModule) desiredShardForRelay(tx *core.Transaction) uint64 {
	return crom.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
}

func (crom *CLPARelayOutsideModule) forwardInjectBatch(toShard uint64, txs []*core.Transaction) {
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
	go networks.TcpDial(sendMsg, crom.pbftNode.ip_nodeTable[toShard][0])
}

// receive relay transaction, which is for cross shard txs
func (crom *CLPARelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)

	localTxs := make([]*core.Transaction, 0, len(relay.Txs))
	forward := make(map[uint64][]*core.Transaction)
	for _, tx := range relay.Txs {
		target := crom.desiredShardForRelay(tx)
		if target == crom.pbftNode.ShardID {
			localTxs = append(localTxs, tx)
		} else {
			forward[target] = append(forward[target], tx)
		}
	}
	if len(localTxs) > 0 {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(localTxs)
	}
	for sid, txs := range forward {
		relayMsg := message.Relay{
			Txs:           txs,
			SenderShardID: relay.SenderShardID,
			SenderSeq:     relay.SenderSeq,
		}
		rByte, err := json.Marshal(relayMsg)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CRelay, rByte)
		go networks.TcpDial(sendMsg, crom.pbftNode.ip_nodeTable[sid][0])
	}

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
	// validate the proofs of txs
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			crom.pbftNode.pl.Plog.Println("Err: wrong proof!")
			return
		}
	}

	localTxs := make([]*core.Transaction, 0, len(rwp.Txs))
	localProofs := make([]chain.TxProofResult, 0, len(rwp.TxProofs))
	forwardTxs := make(map[uint64][]*core.Transaction)
	forwardProofs := make(map[uint64][]chain.TxProofResult)

	for i, tx := range rwp.Txs {
		target := crom.desiredShardForRelay(tx)
		if target == crom.pbftNode.ShardID {
			localTxs = append(localTxs, tx)
			localProofs = append(localProofs, rwp.TxProofs[i])
		} else {
			forwardTxs[target] = append(forwardTxs[target], tx)
			forwardProofs[target] = append(forwardProofs[target], rwp.TxProofs[i])
		}
	}
	if len(localTxs) > 0 {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(localTxs)
	}
	for sid, txs := range forwardTxs {
		msg := message.RelayWithProof{
			Txs:           txs,
			TxProofs:      forwardProofs[sid],
			SenderShardID: rwp.SenderShardID,
			SenderSeq:     rwp.SenderSeq,
		}
		rByte, err := json.Marshal(msg)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CRelayWithProof, rByte)
		go networks.TcpDial(sendMsg, crom.pbftNode.ip_nodeTable[sid][0])
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

	localTxs := make([]*core.Transaction, 0, len(it.Txs))
	forward := make(map[uint64][]*core.Transaction)
	for _, tx := range it.Txs {
		target := crom.desiredShardForInject(tx)
		if target == crom.pbftNode.ShardID {
			localTxs = append(localTxs, tx)
		} else {
			forward[target] = append(forward[target], tx)
		}
	}
	if len(localTxs) > 0 {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(localTxs)
	}
	for sid, txs := range forward {
		crom.forwardInjectBatch(sid, txs)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : handled injected txs msg, local=%d forwarded=%d \n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, len(localTxs), len(it.Txs)-len(localTxs))
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (crom *CLPARelayOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pm.PartitionModified)
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	crom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
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

// when the message from other shard arriving, it should be added into the message pool
func (crom *CLPARelayOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	crom.cdm.AccountStateTx[at.FromShard] = at
	crom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, at.FromShard)

	if len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1 {
		crom.cdm.CollectLock.Lock()
		crom.cdm.CollectOver = true
		crom.cdm.CollectLock.Unlock()
		crom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	}
}

func (crom *CLPARelayOutsideModule) handleShadowCapsule(content []byte) {
	batch := new(message.ShadowCapsuleBatch)
	err := json.Unmarshal(content, batch)
	if err != nil {
		log.Panic(err)
	}
	for _, capsule := range batch.Capsules {
		crom.pbftNode.CurChain.InstallShadowCapsule(
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
	crom.pbftNode.pl.Plog.Printf("S%dN%d installed %d shadow capsules for epoch %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, len(batch.Capsules), batch.EpochTag)
}
