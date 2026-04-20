// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
	"time"
)

func stableHashStrings(items []string) string {
	cp := make([]string, len(items))
	copy(cp, items)
	sort.Strings(cp)
	h := sha256.Sum256([]byte(stringsJoin(cp)))
	return hex.EncodeToString(h[:])
}

func stringsJoin(items []string) string {
	if len(items) == 0 {
		return ""
	}
	out := items[0]
	for i := 1; i < len(items); i++ {
		out += "|" + items[i]
	}
	return out
}

func latestPartitionMeta(cdm *dataSupport.Data_supportCLPA) *message.PartitionModifiedMap {
	if len(cdm.PartitionMeta) == 0 {
		return nil
	}
	return &cdm.PartitionMeta[len(cdm.PartitionMeta)-1]
}

func templateCapsule(meta *message.PartitionModifiedMap, addr string) *message.ShadowCapsule {
	if meta == nil {
		return nil
	}
	for _, cap := range meta.ShadowCapsules {
		if cap.Addr == addr {
			cp := cap
			return &cp
		}
	}
	return nil
}

func debtRootForAddr(txs []*core.Transaction, addr string) []byte {
	parts := make([]string, 0)
	for _, tx := range txs {
		if tx.Sender == addr || tx.Recipient == addr {
			parts = append(parts, hex.EncodeToString(tx.TxHash))
		}
	}
	h := sha256.Sum256([]byte(stringsJoin(parts)))
	return h[:]
}

func shadowCapsuleDigest(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	cp := make([]message.ShadowCapsule, len(capsules))
	copy(cp, capsules)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].Addr == cp[j].Addr {
			return cp[i].TargetShard < cp[j].TargetShard
		}
		return cp[i].Addr < cp[j].Addr
	})
	for _, c := range cp {
		parts = append(parts,
			c.Addr+
				"|"+c.Balance+
				"|"+hex.EncodeToString(c.CodeHash)+
				"|"+hex.EncodeToString(c.StorageRoot)+
				"|"+hex.EncodeToString(c.DebtRoot))
	}
	return stableHashStrings(parts)
}

func partitionDigestForCapsules(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+hex.EncodeToString([]byte{byte(c.TargetShard)}))
	}
	return stableHashStrings(parts)
}

func balanceDigestForCapsules(capsules []message.ShadowCapsule) string {
	parts := make([]string, 0, len(capsules))
	for _, c := range capsules {
		parts = append(parts, c.Addr+"|"+c.Balance+"|"+hex.EncodeToString([]byte{byte(c.Nonce)}))
	}
	return stableHashStrings(parts)
}

func buildBatchRVC(epochTag, fromShard, toShard uint64, capsules []message.ShadowCapsule) *message.ReshardingValidityCertificate {
	capsDigest := shadowCapsuleDigest(capsules)
	partDigest := partitionDigestForCapsules(capsules)
	balDigest := balanceDigestForCapsules(capsules)
	idBase := []string{
		"ZKSCAR",
		hex.EncodeToString([]byte{byte(epochTag)}),
		hex.EncodeToString([]byte{byte(fromShard)}),
		hex.EncodeToString([]byte{byte(toShard)}),
		capsDigest,
		partDigest,
		balDigest,
	}
	certID := stableHashStrings(idBase)
	for i := range capsules {
		capsules[i].RVCID = certID
	}
	return &message.ReshardingValidityCertificate{
		Algorithm:       "ZKSCAR",
		EpochTag:        epochTag,
		FromShard:       fromShard,
		ToShard:         toShard,
		CertificateID:   certID,
		PartitionDigest: partDigest,
		CapsuleDigest:   capsDigest,
		BalanceDigest:   balDigest,
		Proof:           "pseudo-rvc:" + certID,
	}
}

func validateRVCBatch(rvc *message.ReshardingValidityCertificate, capsules []message.ShadowCapsule) bool {
	if rvc == nil {
		return false
	}
	if shadowCapsuleDigest(capsules) != rvc.CapsuleDigest {
		return false
	}
	if partitionDigestForCapsules(capsules) != rvc.PartitionDigest {
		return false
	}
	if balanceDigestForCapsules(capsules) != rvc.BalanceDigest {
		return false
	}
	return true
}

func validateAccountTransferRVCs(atm *message.AccountTransferMsg) bool {
	if atm.Algorithm != "ZKSCAR" {
		return true
	}
	if len(atm.RVCs) == 0 {
		return false
	}
	grouped := make(map[string][]message.ShadowCapsule)
	for _, cap := range atm.ShadowCapsules {
		grouped[cap.RVCID] = append(grouped[cap.RVCID], cap)
	}
	for _, rvc := range atm.RVCs {
		caps := grouped[rvc.CertificateID]
		if len(caps) == 0 {
			return false
		}
		if !validateRVCBatch(rvc, caps) {
			return false
		}
	}
	return true
}

func buildDualAnchorReceipts(txs []*core.Transaction, fromShard, toShard uint64, epochTag uint64) []message.DualAnchorReceipt {
	out := make([]message.DualAnchorReceipt, 0, len(txs))
	for _, tx := range txs {
		oldRoot := stableHashStrings([]string{
			"old",
			hex.EncodeToString(tx.TxHash),
			tx.Sender,
			tx.Recipient,
			hex.EncodeToString([]byte{byte(fromShard)}),
			hex.EncodeToString([]byte{byte(epochTag)}),
		})
		shadowRoot := stableHashStrings([]string{
			"shadow",
			hex.EncodeToString(tx.TxHash),
			tx.Sender,
			tx.Recipient,
			hex.EncodeToString([]byte{byte(toShard)}),
			hex.EncodeToString([]byte{byte(epochTag)}),
		})
		out = append(out, message.DualAnchorReceipt{
			TxHash:     append([]byte(nil), tx.TxHash...),
			Sender:     tx.Sender,
			Recipient:  tx.Recipient,
			OldRoot:    oldRoot,
			ShadowRoot: shadowRoot,
			FromShard:  fromShard,
			ToShard:    toShard,
			EpochTag:   epochTag,
		})
	}
	return out
}

func applyPendingHydration(pbftNode *PbftConsensusNode, cdm *dataSupport.Data_supportCLPA, currentRound uint64) {
	addrs := make([]string, 0)
	states := make([]*core.AccountState, 0)
	for addr, readyRound := range cdm.PendingHydrationRound {
		if readyRound > currentRound {
			continue
		}
		if st, ok := cdm.PendingHydration[addr]; ok {
			full := st.FinalizeHydration(currentRound)
			addrs = append(addrs, addr)
			states = append(states, full)
			delete(cdm.PendingHydration, addr)
			delete(cdm.PendingHydrationRound, addr)
			cdm.HydratedAccounts[addr] = true
		}
	}
	if len(addrs) > 0 {
		pbftNode.CurChain.AddAccounts(addrs, states, pbftNode.view.Load())
	}
}

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

// send the transactions and the accountState to other leaders
func (cphm *CLPAPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
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
	cphm.pbftNode.pl.Plog.Println("The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
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
func (cphm *CLPAPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
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
	cphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
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
