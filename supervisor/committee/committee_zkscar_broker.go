package committee

import (
	"blockEmulator/broker"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ZK-SCAR + Broker committee operations.
// 交易发送、broker 两阶段处理、分片消息格式全部复用 CLPA_Broker 现有链路，
// 仅替换 supervisor 侧的分片映射计算逻辑。
type ZKSCARCommitteeMod_Broker struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	curEpoch              int32
	zkscarLock            sync.Mutex
	zkscarGraph           *partition.ZKSCARState
	modifiedMap           map[string]uint64
	zkscarLastRunningTime time.Time
	zkscarFreq            int

	broker             *broker.Broker
	brokerConfirm1Pool map[string]*message.Mag1Confirm
	brokerConfirm2Pool map[string]*message.Mag2Confirm
	brokerTxPool       []*core.Transaction
	brokerModuleLock   sync.Mutex

	sl *supervisor_log.SupervisorLog

	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
}

func NewZKSCARCommitteeMod_Broker(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, reconfigFrequency int) *ZKSCARCommitteeMod_Broker {
	zg := new(partition.ZKSCARState)
	zg.Init_ZKSCARState(
		params.ZKSCARHotnessWeight,
		params.ZKSCARBalanceWeight,
		params.ZKSCARStabilityBias,
		params.ZKSCARMaxIterations,
		params.ShardNum,
	)

	brk := new(broker.Broker)
	brk.NewBroker(nil)

	return &ZKSCARCommitteeMod_Broker{
		csvPath:               csvFilePath,
		dataTotalNum:          dataNum,
		batchDataNum:          batchNum,
		nowDataNum:            0,
		zkscarGraph:           zg,
		modifiedMap:           make(map[string]uint64),
		zkscarFreq:            reconfigFrequency,
		zkscarLastRunningTime: time.Time{},
		brokerConfirm1Pool:    make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool:    make(map[string]*message.Mag2Confirm),
		brokerTxPool:          make([]*core.Transaction, 0),
		broker:                brk,
		IpNodeTable:           Ip_nodeTable,
		Ss:                    Ss,
		sl:                    sl,
		curEpoch:              0,
	}
}

func (zcm *ZKSCARCommitteeMod_Broker) HandleOtherMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	if msgType != message.CInner2CrossTx {
		return
	}
	itct := new(message.InnerTx2CrossTx)
	err := json.Unmarshal(content, itct)
	if err != nil {
		log.Panic(err)
	}
	itxs := zcm.dealTxByBroker(itct.Txs)
	zcm.txSending(itxs)
}

func (zcm *ZKSCARCommitteeMod_Broker) fetchModifiedMap(key string) uint64 {
	if val, ok := zcm.modifiedMap[key]; ok {
		return val
	}
	return uint64(utils.Addr2Shard(key))
}

func (zcm *ZKSCARCommitteeMod_Broker) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				sendMsg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(sendMsg, zcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}

		if idx == len(txlist) {
			break
		}

		tx := txlist[idx]
		zcm.zkscarLock.Lock()
		senderSid := zcm.fetchModifiedMap(tx.Sender)
		if zcm.broker.IsBroker(tx.Sender) {
			senderSid = zcm.fetchModifiedMap(tx.Recipient)
		}
		zcm.zkscarLock.Unlock()

		sendToShard[senderSid] = append(sendToShard[senderSid], tx)
	}
}

func (zcm *ZKSCARCommitteeMod_Broker) MsgSendingControl() {
	txfile, err := os.Open(zcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()

	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0)
	zkscarCnt := 0

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		if tx, ok := data2tx(data, uint64(zcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			zcm.nowDataNum++
		} else {
			continue
		}

		if len(txlist) == int(zcm.batchDataNum) || zcm.nowDataNum == zcm.dataTotalNum {
			if zcm.zkscarLastRunningTime.IsZero() {
				zcm.zkscarLastRunningTime = time.Now()
			}

			itx := zcm.dealTxByBroker(txlist)
			zcm.txSending(itx)

			txlist = make([]*core.Transaction, 0)
			zcm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !zcm.zkscarLastRunningTime.IsZero() &&
			time.Since(zcm.zkscarLastRunningTime) >= time.Duration(zcm.zkscarFreq)*time.Second {

			zcm.zkscarLock.Lock()
			zkscarCnt++

			mmap, _ := zcm.zkscarGraph.ZKSCAR_Partition()
			pm := zcm.buildPartitionMeta(mmap, uint64(zkscarCnt))
			zcm.partitionMapSend(pm)
			for key, val := range mmap {
				zcm.modifiedMap[key] = val
			}
			zcm.zkscarReset()
			zcm.zkscarLock.Unlock()

			for atomic.LoadInt32(&zcm.curEpoch) != int32(zkscarCnt) {
				time.Sleep(time.Second)
			}
			zcm.zkscarLastRunningTime = time.Now()
			zcm.sl.Slog.Println("Next ZK-SCAR epoch begins.")
		}

		if zcm.nowDataNum == zcm.dataTotalNum {
			break
		}
	}

	for !zcm.Ss.GapEnough() {
		time.Sleep(time.Second)

		if params.ShardNum > 1 && time.Since(zcm.zkscarLastRunningTime) >= time.Duration(zcm.zkscarFreq)*time.Second {
			zcm.zkscarLock.Lock()
			zkscarCnt++

			mmap, _ := zcm.zkscarGraph.ZKSCAR_Partition()
			pm := zcm.buildPartitionMeta(mmap, uint64(zkscarCnt))
			zcm.partitionMapSend(pm)
			for key, val := range mmap {
				zcm.modifiedMap[key] = val
			}
			zcm.zkscarReset()
			zcm.zkscarLock.Unlock()

			for atomic.LoadInt32(&zcm.curEpoch) != int32(zkscarCnt) {
				time.Sleep(time.Second)
			}
			zcm.zkscarLastRunningTime = time.Now()
			zcm.sl.Slog.Println("Next ZK-SCAR epoch begins.")
		}
	}
}

func (zcm *ZKSCARCommitteeMod_Broker) buildPartitionMeta(m map[string]uint64, epochTag uint64) message.PartitionModifiedMap {
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
		Algorithm:         "ZKSCAR",
		EpochTag:          epochTag,
		ShadowCapsules:    make([]message.ShadowCapsule, 0),
	}
	for _, cap := range zcm.zkscarGraph.SnapshotShadowCapsules() {
		pm.ShadowCapsules = append(pm.ShadowCapsules, message.ShadowCapsule{
			Addr:         cap.Addr,
			CurrentShard: uint64(cap.CurrentShard),
			TargetShard:  uint64(cap.TargetShard),
			Degree:       cap.Degree,
			Hotness:      cap.Hotness,
			LocalityGain: cap.LocalityGain,
			EpochTag:     epochTag,
		})
	}
	return pm
}

func (zcm *ZKSCARCommitteeMod_Broker) partitionMapSend(pm message.PartitionModifiedMap) {
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic(err)
	}

	sendMsg := message.MergeMessage(message.CPartitionMsg, pmByte)
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(sendMsg, zcm.IpNodeTable[i][0])
	}
	zcm.sl.Slog.Println("Supervisor: all ZK-SCAR partition map messages have been sent.")
}

func (zcm *ZKSCARCommitteeMod_Broker) zkscarReset() {
	zcm.zkscarGraph = new(partition.ZKSCARState)
	zcm.zkscarGraph.Init_ZKSCARState(
		params.ZKSCARHotnessWeight,
		params.ZKSCARBalanceWeight,
		params.ZKSCARStabilityBias,
		params.ZKSCARMaxIterations,
		params.ShardNum,
	)
	for key, val := range zcm.modifiedMap {
		zcm.zkscarGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (zcm *ZKSCARCommitteeMod_Broker) HandleBlockInfo(b *message.BlockInfoMsg) {
	zcm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)

	if atomic.CompareAndSwapInt32(&zcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		zcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}

	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	zcm.createConfirm(txs)

	zcm.zkscarLock.Lock()
	for _, tx := range b.InnerShardTxs {
		if tx.HasBroker {
			continue
		}
		zcm.zkscarGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, b1tx := range b.Broker1Txs {
		zcm.zkscarGraph.AddEdge(partition.Vertex{Addr: b1tx.OriginalSender}, partition.Vertex{Addr: b1tx.FinalRecipient})
	}
	zcm.zkscarLock.Unlock()
}

func (zcm *ZKSCARCommitteeMod_Broker) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.Mag1Confirm, 0)
	confirm2s := make([]*message.Mag2Confirm, 0)

	zcm.brokerModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := zcm.brokerConfirm1Pool[string(tx.TxHash)]; ok {
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := zcm.brokerConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	zcm.brokerModuleLock.Unlock()

	if len(confirm1s) != 0 {
		zcm.handleTx1ConfirmMag(confirm1s)
	}
	if len(confirm2s) != 0 {
		zcm.handleTx2ConfirmMag(confirm2s)
	}
}

func (zcm *ZKSCARCommitteeMod_Broker) dealTxByBroker(txs []*core.Transaction) (itxs []*core.Transaction) {
	itxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)

	for _, tx := range txs {
		zcm.zkscarLock.Lock()
		rSid := zcm.fetchModifiedMap(tx.Recipient)
		sSid := zcm.fetchModifiedMap(tx.Sender)
		zcm.zkscarLock.Unlock()

		if rSid != sSid && !zcm.broker.IsBroker(tx.Recipient) && !zcm.broker.IsBroker(tx.Sender) {
			brokerRawMeg := &message.BrokerRawMeg{
				Tx:     tx,
				Broker: zcm.broker.BrokerAddress[0],
			}
			brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
		} else {
			if zcm.broker.IsBroker(tx.Recipient) || zcm.broker.IsBroker(tx.Sender) {
				tx.HasBroker = true
				tx.SenderIsBroker = zcm.broker.IsBroker(tx.Sender)
			}
			itxs = append(itxs, tx)
		}
	}

	if len(brokerRawMegs) != 0 {
		zcm.handleBrokerRawMag(brokerRawMegs)
	}
	return itxs
}

func (zcm *ZKSCARCommitteeMod_Broker) handleBrokerType1Mes(brokerType1Megs []*message.BrokerType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range brokerType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce, time.Now())
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient
		tx1.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx1.RawTxHash, ctx.TxHash)
		tx1s = append(tx1s, tx1)

		confirm1 := &message.Mag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash,
		}
		zcm.brokerModuleLock.Lock()
		zcm.brokerConfirm1Pool[string(tx1.TxHash)] = confirm1
		zcm.brokerModuleLock.Unlock()
	}

	zcm.txSending(tx1s)
	fmt.Println("BrokerType1Mes received by shard, add brokerTx1 len", len(tx1s))
}

func (zcm *ZKSCARCommitteeMod_Broker) handleBrokerType2Mes(brokerType2Megs []*message.BrokerType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce, time.Now())
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx2.RawTxHash, ctx.TxHash)
		tx2s = append(tx2s, tx2)

		confirm2 := &message.Mag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		zcm.brokerModuleLock.Lock()
		zcm.brokerConfirm2Pool[string(tx2.TxHash)] = confirm2
		zcm.brokerModuleLock.Unlock()
	}

	zcm.txSending(tx2s)
	fmt.Println("broker tx2 add to pool len", len(tx2s))
}

func (zcm *ZKSCARCommitteeMod_Broker) getBrokerRawMagDigest(r *message.BrokerRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

func (zcm *ZKSCARCommitteeMod_Broker) handleBrokerRawMag(brokerRawMags []*message.BrokerRawMeg) {
	b := zcm.broker
	brokerType1Mags := make([]*message.BrokerType1Meg, 0)

	fmt.Println("broker receive ctx", len(brokerRawMags))
	zcm.brokerModuleLock.Lock()
	for _, meg := range brokerRawMags {
		b.BrokerRawMegs[string(zcm.getBrokerRawMagDigest(meg))] = meg
		brokerType1Mag := &message.BrokerType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	zcm.brokerModuleLock.Unlock()

	zcm.handleBrokerType1Mes(brokerType1Mags)
}

func (zcm *ZKSCARCommitteeMod_Broker) handleTx1ConfirmMag(mag1confirms []*message.Mag1Confirm) {
	brokerType2Mags := make([]*message.BrokerType2Meg, 0)
	b := zcm.broker

	fmt.Println("receive confirm brokerTx1 len", len(mag1confirms))
	zcm.brokerModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		rawMeg := mag1confirm.RawMeg
		if _, ok := b.BrokerRawMegs[string(zcm.getBrokerRawMagDigest(rawMeg))]; !ok {
			fmt.Println("raw message is not existed, tx1 confirms failure!")
			continue
		}

		b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))
		brokerType2Mag := &message.BrokerType2Meg{
			Broker: zcm.broker.BrokerAddress[0],
			RawMeg: rawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	zcm.brokerModuleLock.Unlock()

	zcm.handleBrokerType2Mes(brokerType2Mags)
}

func (zcm *ZKSCARCommitteeMod_Broker) handleTx2ConfirmMag(mag2confirms []*message.Mag2Confirm) {
	b := zcm.broker
	fmt.Println("receive confirm brokerTx2 len", len(mag2confirms))

	num := 0
	zcm.brokerModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		rawMeg := mag2confirm.RawMeg
		b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)], string(mag2confirm.Tx2Hash))
		if len(b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)]) == 2 {
			num++
		} else {
			fmt.Println(len(b.RawTx2BrokerTx[string(rawMeg.Tx.TxHash)]))
		}
	}
	zcm.brokerModuleLock.Unlock()

	fmt.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
}
