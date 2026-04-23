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

// CLPA committee operations
type CLPACommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int
	adaptiveStats       *adaptiveReconfigStats

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string
}

func NewCLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *CLPACommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)
	return &CLPACommitteeModule{
		csvPath:             csvFilePath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		adaptiveStats:       newAdaptiveReconfigStats(),
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
	}
}

func (ccm *CLPACommitteeModule) HandleOtherMessage([]byte) {}

func (ccm *CLPACommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := ccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (ccm *CLPACommitteeModule) tryReconfigure(clpaCnt *int) {
	if params.ShardNum <= 1 || ccm.clpaLastRunningTime.IsZero() {
		return
	}

	interval := effectiveReconfigInterval(ccm.clpaFreq)
	if time.Since(ccm.clpaLastRunningTime) < interval {
		return
	}

	if params.AdaptiveReconfigEnabled != 0 {
		decision := ccm.adaptiveStats.makeDecision(ccm.clpaGraph)
		if !decision.ShouldTrigger {
			ccm.sl.Slog.Printf("ABR-Shard skip reconfiguration: interval=%s %s\n", interval.String(), decision.Reason)
			ccm.clpaLastRunningTime = time.Now()
			return
		}
		ccm.sl.Slog.Printf("ABR-Shard accepts reconfiguration: interval=%s %s\n", interval.String(), decision.Reason)
	} else {
		ccm.sl.Slog.Printf("Fixed-interval reconfiguration triggered (interval=%s).\n", interval.String())
	}

	ccm.clpaLock.Lock()
	backupState := new(partition.CLPAState)
	backupState.CopyCLPA(*ccm.clpaGraph)

	ccm.clpaGraph.ComputeEdges2Shard()
	beforeCross := ccm.clpaGraph.CrossShardEdgeNum
	mmap, _ := ccm.clpaGraph.CLPA_Partition()
	rawMoveCount := len(mmap)

	if params.MigrationBudget > 0 {
		mmap = limitPartitionByBudget(ccm.clpaGraph, mmap, ccm.fetchModifiedMap, params.MigrationBudget)
	}
	ccm.clpaGraph.ComputeEdges2Shard()
	afterCross := ccm.clpaGraph.CrossShardEdgeNum

	if rawMoveCount == 0 || len(mmap) == 0 {
		ccm.adaptiveStats.LastImprovement = 0
		ccm.clpaGraph = backupState
		ccm.clpaLock.Unlock()
		ccm.clpaLastRunningTime = time.Now()
		ccm.sl.Slog.Printf("ABR-Shard produced no executable migration (raw=%d, selected=%d, interval=%s).\n", rawMoveCount, len(mmap), interval.String())
		return
	}

	improvement := 0.0
	if beforeCross > 0 {
		improvement = float64(beforeCross-afterCross) / float64(beforeCross)
		if improvement < 0 {
			improvement = 0
		}
	}
	ccm.adaptiveStats.LastImprovement = improvement

	(*clpaCnt)++
	ccm.clpaMapSend(mmap)
	for key, val := range mmap {
		ccm.modifiedMap[key] = val
	}
	ccm.clpaReset()
	ccm.clpaLock.Unlock()

	ccm.adaptiveStats.resetWindow()

	ccm.sl.Slog.Printf(
		"ABR-Shard migration selected=%d raw=%d budget=%d crossEdges:%d->%d improvement=%.4f interval=%s\n",
		len(mmap), rawMoveCount, params.MigrationBudget, beforeCross, afterCross, improvement, interval.String(),
	)

	for atomic.LoadInt32(&ccm.curEpoch) != int32(*clpaCnt) {
		time.Sleep(time.Second)
	}
	ccm.clpaLastRunningTime = time.Now()
	ccm.sl.Slog.Println("Next CLPA epoch begins. ")
}

func (ccm *CLPACommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
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
				go networks.TcpDial(sendMsg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := ccm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (ccm *CLPACommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			ccm.nowDataNum++
		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(ccm.batchDataNum) || ccm.nowDataNum == ccm.dataTotalNum {
			// set the algorithm timer begins
			if ccm.clpaLastRunningTime.IsZero() {
				ccm.clpaLastRunningTime = time.Now()
			}
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
		}

		ccm.tryReconfigure(&clpaCnt)

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		ccm.tryReconfigure(&clpaCnt)
	}
}

func (ccm *CLPACommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	sendMsg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(sendMsg, ccm.IpNodeTable[i][0])
	}
	ccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (ccm *CLPACommitteeModule) clpaReset() {
	ccm.clpaGraph = new(partition.CLPAState)
	ccm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (ccm *CLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}

	latency := time.Duration(0)
	if !b.CommitTime.IsZero() && !b.ProposeTime.IsZero() && b.CommitTime.After(b.ProposeTime) {
		latency = b.CommitTime.Sub(b.ProposeTime)
	}
	crossCount := len(b.Relay1Txs) + len(b.Relay2Txs)
	ccm.adaptiveStats.observeBlock(b.SenderShardID, b.BlockBodyLength, crossCount, latency)

	ccm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	ccm.clpaLock.Unlock()
}
