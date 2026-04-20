// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) sendPartitionReady() {
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
			networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) getPartitionReady() bool {
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

// send the transactions and the accountState to other leaders
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	txsBeCross := make([]*core.Transaction, 0) // the transactions which will be cross-shard tx because of re-partition
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	meta := latestPartitionMeta(cphm.cdm)

	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	// send the accounts to other shards
	cphm.pbftNode.CurChain.Txpool.GetLocked()
	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		hydrationAddrs := make([]string, 0)
		hydrationStates := make([]*core.AccountState, 0)
		shadowCapsules := make([]message.ShadowCapsule, 0)

		for idx, addr := range accountToFetch {
			if cphm.cdm.ModifiedMap[lastMapid][addr] == i {
				baseState := asFetched[idx]
				addrSend = append(addrSend, addr)
				addrSet[addr] = true

				if meta != nil && meta.Algorithm == "ZKSCAR" {
					tmpl := templateCapsule(meta, addr)
					debtRoot := debtRootForAddr(cphm.pbftNode.CurChain.Txpool.TxQueue, addr)
					shadowState := baseState.BuildShadowState(meta.EpochTag, cphm.pbftNode.ShardID, i, debtRoot, "")
					asSend = append(asSend, shadowState)

					hydrationAddrs = append(hydrationAddrs, addr)
					hydrationStates = append(hydrationStates, baseState.FinalizeHydration(meta.EpochTag))

					cap := message.ShadowCapsule{
						Addr:         addr,
						CurrentShard: cphm.pbftNode.ShardID,
						TargetShard:  i,
						Balance:      baseState.Balance.String(),
						Nonce:        baseState.Nonce,
						CodeHash:     append([]byte(nil), baseState.CodeHash...),
						StorageRoot:  append([]byte(nil), baseState.StorageRoot...),
						DebtRoot:     debtRoot,
						EpochTag:     meta.EpochTag,
					}
					if tmpl != nil {
						cap.Degree = tmpl.Degree
						cap.Hotness = tmpl.Hotness
						cap.LocalityGain = tmpl.LocalityGain
					}
					shadowCapsules = append(shadowCapsules, cap)
				} else {
					asSend = append(asSend, baseState)
				}
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			// whether should be transfer or not
			beSend := false
			beRemoved := false
			_, ok1 := addrSet[ptx.Sender]
			_, ok2 := addrSet[ptx.Recipient]
			if ptx.RawTxHash == nil { // if this tx is an inner-shard tx...
				if ptx.HasBroker {
					if ptx.SenderIsBroker {
						beSend = ok2
						beRemoved = ok2
					} else {
						beRemoved = ok1
						beSend = ok1
					}
				} else if ok1 || ok2 { // if the inner-shard tx should be transferred.
					txsBeCross = append(txsBeCross, ptx)
					beRemoved = true
				}
				// all inner-shard tx should not be added into the account transfer message
			} else if ptx.FinalRecipient == ptx.Recipient {
				beSend = ok2
				beRemoved = ok2
			} else if ptx.OriginalSender == ptx.Sender {
				beRemoved = ok1
				beSend = ok1
			}

			if beSend {
				txSend = append(txSend, ptx)
			}
			if !beRemoved {
				cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
				firstPtr++
			}
		}
		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		cphm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", i)
		ast := message.AccountStateAndTx{
			Addrs:          addrSend,
			AccountState:   asSend,
			HydrationAddrs: hydrationAddrs,
			HydrationState: hydrationStates,
			ShadowCapsules: shadowCapsules,
			FromShard:      cphm.pbftNode.ShardID,
			Txs:            txSend,
		}
		if meta != nil && meta.Algorithm == "ZKSCAR" {
			ast.Algorithm = "ZKSCAR"
			ast.Stage = "shadow"
			receipts := buildDualAnchorReceipts(txSend, cphm.pbftNode.ShardID, i, meta.EpochTag)
			ast.DualReceipts = receipts
			ast.RVC = buildBatchRVC(meta.EpochTag, cphm.pbftNode.ShardID, i, shadowCapsules)
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.CAccountTransferMsg_broker, aByte)
		networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[i][0])
		cphm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", i)
	}
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()

	// send these txs to supervisor
	i2ctx := message.InnerTx2CrossTx{
		Txs: txsBeCross,
	}
	icByte, err := json.Marshal(i2ctx)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CInner2CrossTx, icByte)
	networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
}

// fetch collect infos
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

// propose a partition message
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	receivedHydrationState := make(map[string]*core.AccountState)
	shadowCapsules := make([]message.ShadowCapsule, 0)
	dualReceipts := make([]message.DualAnchorReceipt, 0)
	rvcs := make([]*message.ReshardingValidityCertificate, 0)
	algorithm := "CLPA"
	stage := ""
	// add all data in pool into the set
	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		for i, addr := range at.HydrationAddrs {
			if i < len(at.HydrationState) {
				receivedHydrationState[addr] = at.HydrationState[i]
			}
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)
		shadowCapsules = append(shadowCapsules, at.ShadowCapsules...)
		dualReceipts = append(dualReceipts, at.DualReceipts...)
		if at.RVC != nil {
			rvcs = append(rvcs, at.RVC)
		}
		if at.Algorithm != "" {
			algorithm = at.Algorithm
		}
		if at.Stage != "" {
			stage = at.Stage
		}
	}
	// propose, send all txs to other nodes in shard
	cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(cphm.cdm.ReceivedNewTx)

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	hydAddrs := make([]string, 0)
	hydStates := make([]*core.AccountState, 0)
	for key, val := range receivedHydrationState {
		hydAddrs = append(hydAddrs, key)
		hydStates = append(hydStates, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:    cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		Addrs:          atmaddr,
		AccountState:   atmAs,
		HydrationAddrs: hydAddrs,
		HydrationState: hydStates,
		ShadowCapsules: shadowCapsules,
		DualReceipts:   dualReceipts,
		RVCs:           rvcs,
		Algorithm:      algorithm,
		Stage:          stage,
		ATid:           uint64(len(cphm.cdm.ModifiedMap)),
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
func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) accountTransfer_do(atm *message.AccountTransferMsg) {
	if atm.Algorithm == "ZKSCAR" && !validateAccountTransferRVCs(atm) {
		log.Panic("ZK-SCAR RVC validation failed")
	}
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())

	if atm.Algorithm == "ZKSCAR" {
		for _, rvc := range atm.RVCs {
			cphm.cdm.RVCPool[rvc.CertificateID] = rvc
		}
		for _, cap := range atm.ShadowCapsules {
			cp := cap
			cphm.cdm.ShadowCapsulePool[cap.Addr] = &cp
			cphm.cdm.OwnershipTransferred[cap.Addr] = true
			cphm.cdm.HydratedAccounts[cap.Addr] = false
		}
		for _, receipt := range atm.DualReceipts {
			rc := receipt
			cphm.cdm.DualAnchorReceiptPool[string(receipt.TxHash)] = &rc
		}
		for i, addr := range atm.HydrationAddrs {
			if i >= len(atm.HydrationState) {
				continue
			}
			cphm.cdm.PendingHydration[addr] = atm.HydrationState[i]
			cphm.cdm.PendingHydrationRound[addr] = atm.ATid + uint64(params.ZKSCARHydrationDelayRounds)
		}
		applyPendingHydration(cphm.pbftNode, cphm.cdm, atm.ATid)
	}

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
