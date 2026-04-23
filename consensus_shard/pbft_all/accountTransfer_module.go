// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod) sendPartitionReady() {
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady[cphm.pbftNode.ShardID] = true
	cphm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: cphm.pbftNode.ShardID,
		NowSeqID:  cphm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(cphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod) getPartitionReady() bool {
	cphm.cdm.P_ReadyLock.Lock()
	defer cphm.cdm.P_ReadyLock.Unlock()
	cphm.pbftNode.seqMapLock.Lock()
	defer cphm.pbftNode.seqMapLock.Unlock()
	cphm.cdm.ReadySeqLock.Lock()
	defer cphm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range cphm.pbftNode.seqIDMap {
		if rval, ok := cphm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(cphm.cdm.PartitionReady) == int(cphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

func (cphm *CLPAPbftInsideExtraHandleMod) phaseEpochTag() uint64 {
	return cphm.cdm.AccountTransferRound + 1
}

func (cphm *CLPAPbftInsideExtraHandleMod) sendShadowCapsules(accountToFetch []string, asFetched []*core.AccountState, modified map[string]uint64) {
	epochTag := cphm.phaseEpochTag()
	batches := make(map[uint64][]message.ExecutionShadowCapsule)

	for idx, addr := range accountToFetch {
		target := modified[addr]
		as := asFetched[idx]

		cphm.pbftNode.CurChain.InstallOwnershipHandoff(addr, cphm.pbftNode.ShardID, target, epochTag)

		capsule := message.ExecutionShadowCapsule{
			Addr:        addr,
			SourceShard: cphm.pbftNode.ShardID,
			TargetShard: target,
			Balance:     as.Balance,
			Nonce:       as.Nonce,
			CodeHash:    as.CodeHash,
			StorageRoot: as.StorageRoot,
			EpochTag:    epochTag,
		}
		batches[target] = append(batches[target], capsule)
	}

	for sid, caps := range batches {
		batch := message.ShadowCapsuleBatch{
			FromShard: cphm.pbftNode.ShardID,
			ToShard:   sid,
			EpochTag:  epochTag,
			Capsules:  caps,
		}
		bByte, err := json.Marshal(batch)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CShadowCapsule, bByte)
		for nid := uint64(0); nid < cphm.pbftNode.pbftChainConfig.Nodes_perShard; nid++ {
			go networks.TcpDial(sendMsg, cphm.pbftNode.ip_nodeTable[sid][nid])
		}
	}
}

// send the transactions and the accountState to other leaders
func (cphm *CLPAPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	// phase-1: install execution capsules at the target shard first.
	cphm.sendShadowCapsules(accountToFetch, asFetched, cphm.cdm.ModifiedMap[lastMapid])

	// phase-2: later full reconciliation still ships the complete state + pending txs.
	cphm.pbftNode.CurChain.Txpool.GetLocked()
	cphm.pbftNode.pl.Plog.Println("The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		for idx, addr := range accountToFetch {
			if cphm.cdm.ModifiedMap[lastMapid][addr] == i {
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			_, ok1 := addrSet[ptx.Sender]
			condition1 := ok1 && !ptx.Relayed
			_, ok2 := addrSet[ptx.Recipient]
			condition2 := ok2 && ptx.Relayed
			if condition1 || condition2 {
				txSend = append(txSend, ptx)
			} else {
				cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
				firstPtr++
			}
		}
		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		cphm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", i)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cphm.pbftNode.ShardID,
			Txs:          txSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[i][0])
		cphm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", i)
	}
	cphm.pbftNode.pl.Plog.Println("after sending, The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

// fetch collect infos
func (cphm *CLPAPbftInsideExtraHandleMod) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

// propose a partition message
func (cphm *CLPAPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	// add all data in pool into the set
	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)
	}
	// propose, send all txs to other nodes in shard
	cphm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(cphm.cdm.ReceivedNewTx))
	for _, tx := range cphm.cdm.ReceivedNewTx {
		if !tx.Relayed && cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Sender] != cphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
		if tx.Relayed && cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][tx.Recipient] != cphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
	}
	cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(cphm.cdm.ReceivedNewTx)
	cphm.pbftNode.pl.Plog.Println("The size of txpool: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:  cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(cphm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (cphm *CLPAPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	cleanupAddrs := make([]string, 0, len(atm.ModifiedMap))
	for key, val := range atm.ModifiedMap {
		cnt++
		cleanupAddrs = append(cleanupAddrs, key)
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)

	// phase-2 finalization: only materialize accounts that have not already
	// executed via shadow takeover; if a target shard has already served traffic
	// and materialized a fresher trie state, do not overwrite it with an old snapshot.
	toAddrs := make([]string, 0, len(atm.Addrs))
	toStates := make([]*core.AccountState, 0, len(atm.AccountState))
	for idx, addr := range atm.Addrs {
		if cphm.pbftNode.CurChain.Get_PartitionMap(addr) != cphm.pbftNode.ShardID {
			continue
		}
		if cphm.pbftNode.CurChain.HasAccountState(addr) {
			cphm.pbftNode.pl.Plog.Printf("preserve materialized local state for %s during phase-2 reconciliation\n", addr)
			continue
		}
		toAddrs = append(toAddrs, addr)
		toStates = append(toStates, atm.AccountState[idx])
	}
	cphm.pbftNode.pl.Plog.Printf("%d addrs to add in phase-2\n", len(toAddrs))
	cphm.pbftNode.CurChain.AddAccounts(toAddrs, toStates, cphm.pbftNode.view.Load())
	cphm.pbftNode.CurChain.RemoveShadowAccounts(cleanupAddrs)

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

	cphm.pbftNode.CurChain.PrintBlockChain()
}
