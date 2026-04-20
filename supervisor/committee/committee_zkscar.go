package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ZK-SCAR committee operations.
// 注意：这里故意复用 CLPA 的消息链路和 worker 侧处理逻辑，
// 仅替换 supervisor 侧"如何计算新的账户->分片映射"。
type ZKSCARCommitteeModule struct {
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

	sl *supervisor_log.SupervisorLog

	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
}

func NewZKSCARCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, reconfigFrequency int) *ZKSCARCommitteeModule {
	zg := new(partition.ZKSCARState)
	zg.Init_ZKSCARState(0.35, 0.25, 1.0, 50, params.ShardNum)

	return &ZKSCARCommitteeModule{
		csvPath:               csvFilePath,
		dataTotalNum:          dataNum,
		batchDataNum:          batchNum,
		nowDataNum:            0,
		zkscarGraph:           zg,
		modifiedMap:           make(map[string]uint64),
		zkscarFreq:            reconfigFrequency,
		zkscarLastRunningTime: time.Time{},
		IpNodeTable:           Ip_nodeTable,
		Ss:                    Ss,
		sl:                    sl,
		curEpoch:              0,
	}
}

func (zcm *ZKSCARCommitteeModule) HandleOtherMessage([]byte) {}

func (zcm *ZKSCARCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := zcm.modifiedMap[key]; ok {
		return val
	}
	return uint64(utils.Addr2Shard(key))
}

func (zcm *ZKSCARCommitteeModule) txSending(txlist []*core.Transaction) {
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
		senderSid := zcm.fetchModifiedMap(tx.Sender)
		sendToShard[senderSid] = append(sendToShard[senderSid], tx)
	}
}

func (zcm *ZKSCARCommitteeModule) MsgSendingControl() {
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

			zcm.txSending(txlist)
			txlist = make([]*core.Transaction, 0)
			zcm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !zcm.zkscarLastRunningTime.IsZero() &&
			time.Since(zcm.zkscarLastRunningTime) >= time.Duration(zcm.zkscarFreq)*time.Second {

			zcm.zkscarLock.Lock()
			zkscarCnt++

			mmap, crossEdges := zcm.zkscarGraph.ZKSCAR_Partition()
			zcm.partitionMapSend(mmap)
			for key, val := range mmap {
				zcm.modifiedMap[key] = val
			}

			zcm.sl.Slog.Printf(
				"ZK-SCAR reconfig epoch=%d modifiedAccounts=%d shadowCapsules=%d crossShardEdges=%d\n",
				zkscarCnt, len(mmap), len(zcm.zkscarGraph.ShadowCapsules), crossEdges,
			)

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

			mmap, crossEdges := zcm.zkscarGraph.ZKSCAR_Partition()
			zcm.partitionMapSend(mmap)
			for key, val := range mmap {
				zcm.modifiedMap[key] = val
			}

			zcm.sl.Slog.Printf(
				"ZK-SCAR reconfig epoch=%d modifiedAccounts=%d shadowCapsules=%d crossShardEdges=%d\n",
				zkscarCnt, len(mmap), len(zcm.zkscarGraph.ShadowCapsules), crossEdges,
			)

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

func (zcm *ZKSCARCommitteeModule) partitionMapSend(m map[string]uint64) {
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
		Algorithm:         "ZKSCAR",
		Epoch:             uint64(atomic.LoadInt32(&zcm.curEpoch) + 1),
		CapsuleCount:      len(zcm.zkscarGraph.ShadowCapsules),
		ProofType:         "pseudo-rvc",
	}
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

func (zcm *ZKSCARCommitteeModule) zkscarReset() {
	zcm.zkscarGraph = new(partition.ZKSCARState)
	zcm.zkscarGraph.Init_ZKSCARState(0.35, 0.25, 1.0, 50, params.ShardNum)
	for key, val := range zcm.modifiedMap {
		zcm.zkscarGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (zcm *ZKSCARCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	zcm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)

	if atomic.CompareAndSwapInt32(&zcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		zcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}

	if b.BlockBodyLength == 0 {
		return
	}

	zcm.zkscarLock.Lock()
	for _, tx := range b.InnerShardTxs {
		zcm.zkscarGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		zcm.zkscarGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	zcm.zkscarLock.Unlock()
}
