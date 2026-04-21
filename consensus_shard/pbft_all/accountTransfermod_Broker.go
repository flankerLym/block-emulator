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

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) sendAccounts_and_Txs() {
	accountToFetch := make([]string, 0)
	txsBeCross := make([]*core.Transaction, 0)
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	meta := latestPartitionMeta(cphm.cdm)

	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	cphm.cdm.OutgoingHydration = make(map[uint64]*message.AccountHydrationMsg)

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

					cap := message.ShadowCapsule{
						Addr:         addr,
						CurrentShard: cphm.pbftNode.ShardID,
						TargetShard:  i,
						Balance:      baseState.Balance.String(),
						Nonce:        baseState.Nonce,
						CodeHash:     append([]byte(nil), baseState.CodeHash...),
						StorageRoot:  append([]byte(nil), baseState.StorageRoot...),
						DebtRoot:     append([]byte(nil), debtRoot...),
						EpochTag:     meta.EpochTag,
					}
					if tmpl != nil {
						cap.Degree = tmpl.Degree
						cap.Hotness = tmpl.Hotness
						cap.LocalityGain = tmpl.LocalityGain
					}
					shadowCapsules = append(shadowCapsules, cap)

					shadowState := baseState.BuildShadowState(meta.EpochTag, cphm.pbftNode.ShardID, i, debtRoot, "")
					asSend = append(asSend, shadowState)

					hydrationAddrs = append(hydrationAddrs, addr)
					hydrationStates = append(hydrationStates, baseState.FinalizeHydration(meta.EpochTag))
				} else {
					asSend = append(asSend, baseState.Clone())
				}
			}
		}

		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			beSend := false
			beRemoved := false
			_, ok1 := addrSet[ptx.Sender]
			_, ok2 := addrSet[ptx.Recipient]
			if ptx.RawTxHash == nil {
				if ptx.HasBroker {
					if ptx.SenderIsBroker {
						beSend = ok2
						beRemoved = ok2
					} else {
						beRemoved = ok1
						beSend = ok1
					}
				} else if ok1 || ok2 {
					txsBeCross = append(txsBeCross, ptx)
					beRemoved = true
				}
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
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cphm.pbftNode.ShardID,
			Txs:          txSend,
		}
		if meta != nil && meta.Algorithm == "ZKSCAR" {
			ast.Algorithm = "ZKSCAR"
			ast.Stage = "shadow"
			ast.DualReceipts = buildDualAnchorReceipts(txSend, cphm.pbftNode.ShardID, i, meta.EpochTag)
			if len(shadowCapsules) > 0 {
				rvc := buildBatchRVC(meta.EpochTag, cphm.pbftNode.ShardID, i, shadowCapsules)
				for idx := range asSend {
					asSend[idx].LastRVC = rvc.CertificateID
				}
				for idx := range hydrationStates {
					hydrationStates[idx].LastRVC = rvc.CertificateID
				}
				ast.RVC = rvc
				ast.ShadowCapsules = shadowCapsules
				cphm.cdm.OutgoingHydration[i] = &message.AccountHydrationMsg{
					Algorithm:      "ZKSCAR",
					EpochTag:       meta.EpochTag,
					FromShard:      cphm.pbftNode.ShardID,
					ToShard:        i,
					Addrs:          append([]string(nil), hydrationAddrs...),
					AccountState:   hydrationStates,
					ShadowCapsules: shadowCapsules,
					RVC:            rvc,
					Stage:          "hydration",
				}
			}
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

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	receivedHydrationState := make(map[string]*core.AccountState)
	shadowCapsules := make([]message.ShadowCapsule, 0)
	dualReceipts := make([]message.DualAnchorReceipt, 0)
	rvcs := make([]*message.ReshardingValidityCertificate, 0)
	algorithm := "CLPA"
	stage := ""

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

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) accountTransfer_do(atm *message.AccountTransferMsg) {
	if atm.Algorithm == "ZKSCAR" && !validateAccountTransferRVCs(atm) {
		log.Panic("ZK-SCAR RVC validation failed")
	}
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	cphm.pbftNode.CurChain.UpsertAccountsFull(atm.Addrs, atm.AccountState)

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

	if atm.Algorithm == "ZKSCAR" && cphm.pbftNode.NodeID == uint64(cphm.pbftNode.view.Load()) {
		for sid, hyd := range cphm.cdm.OutgoingHydration {
			if hyd == nil || len(hyd.Addrs) == 0 {
				continue
			}
			payload := *hyd
			go func(target uint64, hm message.AccountHydrationMsg) {
				time.Sleep(time.Duration(params.Delay+params.JitterRange+200) * time.Millisecond)
				hb, err := json.Marshal(hm)
				if err != nil {
					log.Panic(err)
				}
				sendMsg := message.MergeMessage(message.CAccountHydration, hb)
				broadcastToShard(cphm.pbftNode, target, sendMsg)
			}(sid, payload)
		}
	}
	cphm.cdm.OutgoingHydration = make(map[uint64]*message.AccountHydrationMsg)

	cphm.pbftNode.CurChain.PrintBlockChain()
}
