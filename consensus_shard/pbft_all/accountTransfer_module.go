// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"crypto/sha256"
	"encoding/json"
	"log"
	"strconv"
	"time"
)

func hashBytes(parts ...[]byte) []byte {
	h := sha256.New()
	for _, p := range parts {
		if len(p) > 0 {
			_, _ = h.Write(p)
		}
	}
	return h.Sum(nil)
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (cphm *CLPAPbftInsideExtraHandleMod) buildShadowCapsules(
	addrs []string,
	states []*core.AccountState,
	sourceShard, targetShard, epochTag uint64,
) []*message.ShadowCapsule {
	res := make([]*message.ShadowCapsule, 0, len(addrs))
	for idx, addr := range addrs {
		var (
			balance     = "0"
			nonce       uint64
			codeHash    []byte
			storageRoot []byte
		)
		if idx < len(states) && states[idx] != nil {
			nonce = states[idx].Nonce
			if states[idx].Balance != nil {
				balance = states[idx].Balance.String()
			}
			codeHash = cloneBytes(states[idx].CodeHash)
			storageRoot = cloneBytes(states[idx].StorageRoot)
		}

		debtRoot := hashBytes(
			[]byte("debt"),
			[]byte(addr),
			[]byte(strconv.FormatUint(sourceShard, 10)),
			[]byte(strconv.FormatUint(targetShard, 10)),
			[]byte(strconv.FormatUint(epochTag, 10)),
		)
		capsuleRoot := hashBytes(
			[]byte(addr),
			[]byte(balance),
			[]byte(strconv.FormatUint(nonce, 10)),
			codeHash,
			storageRoot,
			debtRoot,
			[]byte(strconv.FormatUint(epochTag, 10)),
		)

		res = append(res, &message.ShadowCapsule{
			Addr:         addr,
			CurrentShard: sourceShard,
			TargetShard:  targetShard,
			Balance:      balance,
			Nonce:        nonce,
			CodeHash:     codeHash,
			StorageRoot:  storageRoot,
			DebtRoot:     debtRoot,
			EpochTag:     epochTag,
			CapsuleRoot:  capsuleRoot,
		})
	}
	return res
}

func (cphm *CLPAPbftInsideExtraHandleMod) buildDualAnchorReceipts(
	txs []*core.Transaction,
	sourceShard, targetShard, epochTag uint64,
) []*message.DualAnchorReceipt {
	res := make([]*message.DualAnchorReceipt, 0, len(txs))
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		oldRoot := hashBytes(
			[]byte("old-root"),
			tx.TxHash,
			[]byte(strconv.FormatUint(sourceShard, 10)),
			[]byte(strconv.FormatUint(epochTag, 10)),
		)
		shadowRoot := hashBytes(
			[]byte("shadow-root"),
			tx.TxHash,
			[]byte(strconv.FormatUint(targetShard, 10)),
			[]byte(strconv.FormatUint(epochTag, 10)),
		)
		res = append(res, &message.DualAnchorReceipt{
			TxHash:       cloneBytes(tx.TxHash),
			OldShardRoot: oldRoot,
			ShadowRoot:   shadowRoot,
			FromShard:    sourceShard,
			ToShard:      targetShard,
			EpochTag:     epochTag,
		})
	}
	return res
}

func (cphm *CLPAPbftInsideExtraHandleMod) buildRVC(
	sourceShard, targetShard, epochTag uint64,
	capsules []*message.ShadowCapsule,
	receipts []*message.DualAnchorReceipt,
) *message.ReshardingValidityCertificate {
	if len(capsules) == 0 && len(receipts) == 0 {
		return nil
	}

	addrs := make([]string, 0, len(capsules))
	capsuleDigestSeed := make([]byte, 0)
	for _, sc := range capsules {
		if sc == nil {
			continue
		}
		addrs = append(addrs, sc.Addr)
		capsuleDigestSeed = append(capsuleDigestSeed, sc.Hash()...)
	}
	receiptDigestSeed := make([]byte, 0)
	for _, r := range receipts {
		if r == nil {
			continue
		}
		receiptDigestSeed = append(receiptDigestSeed, r.Hash()...)
	}

	capsuleDigest := hashBytes(capsuleDigestSeed)
	receiptDigest := hashBytes(receiptDigestSeed)
	issuedAt := time.Now().UnixNano()
	proof := hashBytes(
		[]byte("pseudo-rvc"),
		capsuleDigest,
		receiptDigest,
		[]byte(strconv.FormatInt(issuedAt, 10)),
	)

	return &message.ReshardingValidityCertificate{
		SourceShard:   sourceShard,
		TargetShard:   targetShard,
		EpochTag:      epochTag,
		AccountAddrs:  addrs,
		CapsuleCount:  len(addrs),
		CapsuleDigest: capsuleDigest,
		ReceiptDigest: receiptDigest,
		IssuedAt:      issuedAt,
		Proof:         proof,
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
	accountToFetch := make([]string, 0)
	lastMapid := len(cphm.cdm.ModifiedMap) - 1
	for key, val := range cphm.cdm.ModifiedMap[lastMapid] {
		if val != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(key) == cphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	cphm.pbftNode.CurChain.Txpool.GetLocked()
	cphm.pbftNode.pl.Plog.Println("The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	epochTag := uint64(len(cphm.cdm.ModifiedMap))

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

		shadowCapsules := cphm.buildShadowCapsules(addrSend, asSend, cphm.pbftNode.ShardID, i, epochTag)
		receipts := cphm.buildDualAnchorReceipts(txSend, cphm.pbftNode.ShardID, i, epochTag)
		cert := cphm.buildRVC(cphm.pbftNode.ShardID, i, epochTag, shadowCapsules, receipts)

		cphm.pbftNode.pl.Plog.Printf(
			"The txSend to shard %d is generated, shadowCapsules=%d, receipts=%d\n",
			i, len(shadowCapsules), len(receipts),
		)

		ast := message.AccountStateAndTx{
			Addrs:          addrSend,
			AccountState:   asSend,
			FromShard:      cphm.pbftNode.ShardID,
			Txs:            txSend,
			ShadowCapsules: shadowCapsules,
			Certificate:    cert,
			Receipts:       receipts,
			Phase:          "ownership-transfer",
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

	// 先清空当前轮的 ZK-SCAR 元数据缓冲
	cphm.cdm.PendingShadowCapsules = make(map[string]*message.ShadowCapsule)
	cphm.cdm.PendingCertificates = make([]*message.ReshardingValidityCertificate, 0)
	cphm.cdm.PendingDualAnchors = make(map[string]*message.DualAnchorReceipt)
	cphm.cdm.HydrationQueue = make(map[string]bool)

	for _, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)

		for _, sc := range at.ShadowCapsules {
			if sc == nil {
				continue
			}
			cphm.cdm.PendingShadowCapsules[sc.Addr] = sc
			cphm.cdm.HydrationQueue[sc.Addr] = true
		}
		if at.Certificate != nil {
			cphm.cdm.PendingCertificates = append(cphm.cdm.PendingCertificates, at.Certificate)
		}
		for _, rc := range at.Receipts {
			if rc == nil {
				continue
			}
			cphm.cdm.PendingDualAnchors[string(rc.TxHash)] = rc
		}
	}

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

	capsules := make([]*message.ShadowCapsule, 0, len(cphm.cdm.PendingShadowCapsules))
	for _, sc := range cphm.cdm.PendingShadowCapsules {
		capsules = append(capsules, sc)
	}
	receipts := make([]*message.DualAnchorReceipt, 0, len(cphm.cdm.PendingDualAnchors))
	for _, rc := range cphm.cdm.PendingDualAnchors {
		receipts = append(receipts, rc)
	}

	atm := message.AccountTransferMsg{
		ModifiedMap:    cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		Addrs:          atmaddr,
		AccountState:   atmAs,
		ATid:           uint64(len(cphm.cdm.ModifiedMap)),
		ShadowCapsules: capsules,
		Certificates:   cphm.cdm.PendingCertificates,
		Receipts:       receipts,
		MigrationPhase: "ownership-transfer",
		HydrationAddrs: atmaddr,
	}
	cphm.pbftNode.pl.Plog.Printf(
		"ZK-SCAR proposePartition: incomingShadowCapsules=%d, certificates=%d, receipts=%d\n",
		len(capsules), len(atm.Certificates), len(receipts),
	)

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

// all nodes in a shard will do account Transfer, to sync the state trie
func (cphm *CLPAPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)

	validCerts := 0
	for _, cert := range atm.Certificates {
		if cert != nil && cert.VerifyBasic() {
			validCerts++
			cphm.cdm.PendingCertificates = append(cphm.cdm.PendingCertificates, cert)
		}
	}
	for _, rc := range atm.Receipts {
		if rc != nil {
			cphm.cdm.PendingDualAnchors[string(rc.TxHash)] = rc
		}
	}
	for _, sc := range atm.ShadowCapsules {
		if sc == nil {
			continue
		}
		sc.OwnershipTransferred = true
		cphm.cdm.PendingShadowCapsules[sc.Addr] = sc
	}

	cphm.pbftNode.pl.Plog.Printf(
		"ZK-SCAR accountTransfer_do: shadowCapsules=%d, validCertificates=%d, receipts=%d\n",
		len(atm.ShadowCapsules), validCerts, len(atm.Receipts),
	)

	cphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())

	// 当前工程仍然复用完整账户同步，因此这里直接把 hydration 标记完成。
	for _, addr := range atm.HydrationAddrs {
		if sc, ok := cphm.cdm.PendingShadowCapsules[addr]; ok {
			sc.HydrationFinished = true
			cphm.cdm.PendingShadowCapsules[addr] = sc
		}
		delete(cphm.cdm.HydrationQueue, addr)
	}

	if uint64(len(cphm.cdm.ModifiedMap)) != atm.ATid {
		cphm.cdm.ModifiedMap = append(cphm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	cphm.cdm.AccountTransferRound = atm.ATid
	cphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cphm.cdm.PartitionOn = false

	if len(cphm.cdm.HydrationQueue) == 0 {
		cphm.cdm.MigrationPhase = "hydrated"
	} else {
		cphm.cdm.MigrationPhase = atm.MigrationPhase
	}

	cphm.cdm.CollectLock.Lock()
	cphm.cdm.CollectOver = false
	cphm.cdm.CollectLock.Unlock()

	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady = make(map[uint64]bool)
	cphm.cdm.P_ReadyLock.Unlock()

	cphm.pbftNode.CurChain.PrintBlockChain()
}
