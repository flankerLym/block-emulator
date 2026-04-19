package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/partition/reshard"
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

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// reshard components
	ReshardCfg reshard.ReshardConfig
	Strategy   reshard.Strategy
	Verifier   reshard.Verifier
}

func NewCLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int, reshardCfg reshard.ReshardConfig, strategy reshard.Strategy, verifier reshard.Verifier) *CLPACommitteeModule {
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
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		ReshardCfg:          reshardCfg,
		Strategy:            strategy,
		Verifier:            verifier,
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
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
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

		if params.ShardNum > 1 && !ccm.clpaLastRunningTime.IsZero() && time.Since(ccm.clpaLastRunningTime) >= time.Duration(ccm.clpaFreq)*time.Second {
			ccm.clpaLock.Lock()
			clpaCnt++

			if ccm.ReshardCfg.Enabled {
				// 使用新的重分片策略
				snapshot := ccm.buildSystemSnapshot()
				plan := ccm.Strategy.BuildPlan(snapshot, ccm.ReshardCfg)

				if plan.Triggered {
					ok := ccm.Verifier.Verify(snapshot, &plan, ccm.ReshardCfg)
					if ok {
						ccm.applyReshardPlan(plan)
					} else {
						ccm.sl.Slog.Printf("[Reshard] verification failed: %s", plan.VerificationMsg)
					}
				} else {
					ccm.sl.Slog.Printf("[Reshard] not triggered: %s", plan.Reason)
					// 如果重分片未触发，使用原来的CLPA逻辑
					mmap, _ := ccm.clpaGraph.CLPA_Partition()
					ccm.clpaMapSend(mmap)
					for key, val := range mmap {
						ccm.modifiedMap[key] = val
					}
					ccm.clpaReset()
				}
			} else {
				// 使用原来的CLPA逻辑
				mmap, _ := ccm.clpaGraph.CLPA_Partition()
				ccm.clpaMapSend(mmap)
				for key, val := range mmap {
					ccm.modifiedMap[key] = val
				}
				ccm.clpaReset()
			}

			ccm.clpaLock.Unlock()

			for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			ccm.clpaLastRunningTime = time.Now()
			ccm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if params.ShardNum > 1 && time.Since(ccm.clpaLastRunningTime) >= time.Duration(ccm.clpaFreq)*time.Second {
			ccm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := ccm.clpaGraph.CLPA_Partition()
			ccm.clpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.clpaReset()
			ccm.clpaLock.Unlock()

			for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			ccm.sl.Slog.Println("Next CLPA epoch begins. ")
			ccm.clpaLastRunningTime = time.Now()
		}
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
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, ccm.IpNodeTable[i][0])
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

func (ccm *CLPACommitteeModule) buildSystemSnapshot() reshard.SystemSnapshot {
	var shardMetrics []reshard.ShardMetrics
	for i := 0; i < params.ShardNum; i++ {
		shardMetrics = append(shardMetrics, reshard.ShardMetrics{
			ShardID:        uint64(i),
			Load:           1.0, // 简化处理，实际应根据真实负载计算
			QueueLen:       0,
			CrossTxRate:    0.5, // 简化处理
			HotspotScore:   0.3, // 简化处理
			RiskScore:      0.2, // 简化处理
			LatencyScore:   0.1, // 简化处理
			LastUpdateUnix: time.Now().Unix(),
		})
	}

	var accounts []reshard.AccountStat
	for key, val := range ccm.modifiedMap {
		accounts = append(accounts, reshard.AccountStat{
			Account:           key,
			CurrentShard:      val,
			TxFreq:            1.0, // 简化处理
			TxVolume:          1.0, // 简化处理
			RecentActivity:    1.0, // 简化处理
			CrossContribution: 0.5, // 简化处理
			Degree:            2.0, // 简化处理
			Burst:             1.0, // 简化处理
			RiskScore:         0.2, // 简化处理
			MigrationCost:     0.1, // 简化处理
		})
	}

	return reshard.SystemSnapshot{
		Now:          time.Now(),
		ShardMetrics: shardMetrics,
		Accounts:     accounts,
		ShardCount:   params.ShardNum,
	}
}

func (ccm *CLPACommitteeModule) applyReshardPlan(plan reshard.ReshardPlan) {
	mmap := make(map[string]uint64)
	for _, mv := range plan.SelectedMoves {
		mmap[mv.Account] = mv.ToShard
		ccm.modifiedMap[mv.Account] = mv.ToShard
	}
	ccm.clpaMapSend(mmap)
	ccm.clpaReset()
	ccm.sl.Slog.Printf("[Reshard] applied moves=%d verifier=%s verify_ok=%v msg=%s",
		len(plan.SelectedMoves), plan.VerifierMode, plan.VerificationOK, plan.VerificationMsg)
}

func (ccm *CLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	ccm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	ccm.clpaLock.Unlock()
}
