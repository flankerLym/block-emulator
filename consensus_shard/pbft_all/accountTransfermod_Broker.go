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

	pr := message.PartitionReady{FromShard: cphm.pbftNode.ShardID, NowSeqID: cphm.pbftNode.sequenceID}
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
	cphm.pbftNode.CurChain.Txpool.GetLocked()

	for i := uint64(0); i < cphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == cphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		shadowCapsules := make([]message.ShadowCapsule, 0)
		sourceStateRoot := ""
		if meta != nil && meta.Algorithm == "ZKSCAR" {
			sourceStateRoot = stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
		}

		for idx, addr := range accountToFetch {
			if cphm.cdm.ModifiedMap[lastMapid][addr] == i {
				baseState := asFetched[idx]
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				if meta != nil && meta.Algorithm == "ZKSCAR" {
					cphm.cdm.SourceCustodyState[addr] = baseState.Clone()
					cphm.pbftNode.CurChain.FreezeAccount(addr, meta.EpochTag)
					tmpl := templateCapsule(meta, addr)
					debtRoot := debtRootForAddr(cphm.pbftNode.CurChain.Txpool.TxQueue, cphm.cdm, addr)
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
			_, ok1 := addrSet[string(ptx.Sender)]
			_, ok2 := addrSet[string(ptx.Recipient)]
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
			} else if string(ptx.FinalRecipient) == string(ptx.Recipient) {
				beSend = ok2
				beRemoved = ok2
			} else if string(ptx.OriginalSender) == string(ptx.Sender) {
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

		if meta != nil && meta.Algorithm == "ZKSCAR" {
			if len(shadowCapsules) > 0 {
				freezeStateRoot := stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
				rvc := buildBatchRVC(meta.EpochTag, cphm.pbftNode.ShardID, i, shadowCapsules, sourceStateRoot, freezeStateRoot)
				for _, cap := range shadowCapsules {
					shadowState := cphm.cdm.SourceCustodyState[cap.Addr].BuildShadowState(meta.EpochTag, cphm.pbftNode.ShardID, i, cap.DebtRoot, rvc.CertificateID)
					asSend = append(asSend, shadowState)
				}
				ast := message.AccountStateAndTx{
					Addrs:          addrSend,
					AccountState:   asSend,
					ShadowCapsules: shadowCapsules,
					FromShard:      cphm.pbftNode.ShardID,
					Txs:            txSend,
					Algorithm:      "ZKSCAR",
					Stage:          "shadow",
					DualReceipts:   buildDualAnchorReceipts(txSend, cphm.pbftNode.ShardID, i, meta.EpochTag, sourceStateRoot, rvc.CertificateID),
					RVC:            rvc,
				}
				aByte, err := json.Marshal(ast)
				if err != nil {
					log.Panic(err)
				}
				networks.TcpDial(message.MergeMessage(message.CAccountTransferMsg_broker, aByte), cphm.pbftNode.ip_nodeTable[i][0])
			} else {
				ast := message.AccountStateAndTx{
					Addrs:        addrSend,
					AccountState: asSend,
					FromShard:    cphm.pbftNode.ShardID,
					Txs:          txSend,
					Algorithm:    "ZKSCAR",
					Stage:        "shadow",
				}
				aByte, err := json.Marshal(ast)
				if err != nil {
					log.Panic(err)
				}
				networks.TcpDial(message.MergeMessage(message.CAccountTransferMsg_broker, aByte), cphm.pbftNode.ip_nodeTable[i][0])
			}
		} else {
			ast := message.AccountStateAndTx{
				Addrs:        addrSend,
				AccountState: asSend,
				FromShard:    cphm.pbftNode.ShardID,
				Txs:          txSend,
			}
			aByte, err := json.Marshal(ast)
			if err != nil {
				log.Panic(err)
			}
			networks.TcpDial(message.MergeMessage(message.CAccountTransferMsg_broker, aByte), cphm.pbftNode.ip_nodeTable[i][0])
		}
	}

	i2ctx := message.InnerTx2CrossTx{Txs: txsBeCross}
	icByte, err := json.Marshal(i2ctx)
	if err != nil {
		log.Panic()
	}
	networks.TcpDial(message.MergeMessage(message.CInner2CrossTx, icByte), cphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) proposePartition() (bool, *message.Request) {
	shadowCapsules := make([]message.ShadowCapsule, 0)
	dualReceipts := make([]message.DualAnchorReceipt, 0)
	rvcs := make([]*message.ReshardingValidityCertificate, 0)
	algorithm := "CLPA"
	stage := ""
	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
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
	atm := message.AccountTransferMsg{
		ModifiedMap:    cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		Addrs:          atmaddr,
		AccountState:   atmAs,
		ShadowCapsules: shadowCapsules,
		DualReceipts:   dualReceipts,
		RVCs:           rvcs,
		Algorithm:      algorithm,
		Stage:          stage,
		ATid:           uint64(len(cphm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	return true, &message.Request{
		RequestType: message.PartitionReq,
		Msg:         message.RawMessage{Content: atmbyte},
		ReqTime:     time.Now(),
	}
}

func (cphm *CLPAPbftInsideExtraHandleMod_forBroker) accountTransfer_do(atm *message.AccountTransferMsg) {
	if atm.Algorithm == "ZKSCAR" && !validateAccountTransferRVCs(atm) {
		log.Panic("ZK-SCAR RVC validation failed")
	}
	for key, val := range atm.ModifiedMap {
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())
	if atm.Algorithm == "ZKSCAR" {
		if !validateInstalledShadowAccounts(cphm.pbftNode, atm.ShadowCapsules) {
			log.Panic("ZK-SCAR shadow account installation validation failed")
		}
		shadowRoot := stateRootHex(cphm.pbftNode.CurChain.CurrentBlock.Header.StateRoot)
		atm.DualReceipts = bindShadowRootToReceipts(atm.DualReceipts, shadowRoot)
		for _, rvc := range atm.RVCs {
			rvc.TargetShadowRoot = shadowRoot
			rvc.TargetShadowRootType = "mpt-state-root"
			cphm.cdm.RVCPool[rvc.CertificateID] = rvc
		}
		for _, cap := range atm.ShadowCapsules {
			cp := cap
			cphm.cdm.ShadowCapsulePool[cap.Addr] = &cp
			cphm.cdm.OwnershipTransferred[cap.Addr] = true
			cphm.cdm.HydratedAccounts[cap.Addr] = false
			cphm.cdm.ShadowInstallHeight[cap.Addr] = currentStateHeight(cphm.pbftNode)
		}
		indexDualAnchorReceipts(cphm.cdm, atm.DualReceipts)
		issueHydrationRequests(cphm.pbftNode, cphm.cdm, atm.ShadowCapsules)
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
}
