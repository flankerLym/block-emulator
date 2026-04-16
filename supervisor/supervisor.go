// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	// RL actions
	RLActionChan chan committee.RLAction
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()
	d.Ss = signal.NewStopSignal(3 * int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(
			d.Ip_nodeTable, d.Ss, d.sl,
			params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap,
		)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(
			d.Ip_nodeTable, d.Ss, d.sl,
			params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap,
		)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(
			d.Ip_nodeTable, d.Ss, d.sl,
			params.DatasetFile, params.TotalDataSize, params.TxBatchSize,
		)
	default:
		d.comMod = committee.NewRelayCommitteeModule(
			d.Ip_nodeTable, d.Ss, d.sl,
			params.DatasetFile, params.TotalDataSize, params.TxBatchSize,
		)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		default:
		}
	}

	d.RLActionChan = make(chan committee.RLAction, 8)
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}

	// StopSignal check
	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc()
	} else {
		d.Ss.StopGap_Reset()
	}

	d.comMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	go d.comMod.MsgSendingControl()

	for !d.Ss.GapEnough() {
		select {
		case action := <-d.RLActionChan:
			d.sl.Slog.Printf("[RL] receive action: %+v\n", action)
			if err := d.comMod.ApplyRLAction(action); err != nil {
				d.sl.Slog.Printf("[RL] ApplyRLAction failed: %v\n", err)
			}
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	// TxHandling is end
	for !d.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
	}

	// send stop message
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
		}
	}

	// make sure all stop messages are sent.
	time.Sleep(time.Duration(params.Delay+params.JitterRange+3) * time.Millisecond)

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	d.StartRLActionServer()
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}

type rlActionRequest struct {
	// 旧版 Python: {"action": 0/1/2}
	Action *int `json:"action"`

	// 新版 Python: {"action_id":..., "action_name":..., "shard_id":..., "params":...}
	ActionID   *int                   `json:"action_id"`
	ActionName string                 `json:"action_name"`
	ShardID    int                    `json:"shard_id"`
	Params     map[string]interface{} `json:"params"`
}

func normalizeRLAction(req rlActionRequest) committee.RLAction {
	act := committee.RLAction{
		ActionID:   0,
		ActionName: "noop",
		ShardID:    req.ShardID,
		Params:     req.Params,
	}

	// 优先使用新版 action_name
	if req.ActionName != "" {
		act.ActionName = req.ActionName
		if req.ActionID != nil {
			act.ActionID = *req.ActionID
		}
		return act
	}

	// 兼容旧版 {"action": x}
	if req.Action != nil {
		switch *req.Action {
		case 0:
			act.ActionID = 0
			act.ActionName = "noop"
		case 1:
			// 兼容你当前旧 Python 语义：1=合并
			act.ActionID = 1
			act.ActionName = "merge_shard"
		case 2:
			// 兼容你当前旧 Python 语义：2=拆分
			act.ActionID = 2
			act.ActionName = "split_shard"
		default:
			act.ActionID = *req.Action
			act.ActionName = "noop"
		}
		return act
	}

	// 兜底：只有 action_id，没有 action_name
	if req.ActionID != nil {
		act.ActionID = *req.ActionID
		switch *req.ActionID {
		case 0:
			act.ActionName = "noop"
		case 1:
			act.ActionName = "split_shard"
		case 2:
			act.ActionName = "merge_shard"
		case 3:
			act.ActionName = "trigger_clpa"
		case 4:
			act.ActionName = "enable_broker"
		case 5:
			act.ActionName = "enable_relay"
		case 6:
			act.ActionName = "enter_cooldown"
		default:
			act.ActionName = "noop"
		}
	}

	return act
}

// 强化学习动作接收服务
func (d *Supervisor) StartRLActionServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/rl_action", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req rlActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"bad_request"}`))
			return
		}

		act := normalizeRLAction(req)

		select {
		case d.RLActionChan <- act:
		default:
			d.sl.Slog.Println("[RL] action channel full, drop action")
		}

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	go func() {
		if err := http.ListenAndServe(":9000", mux); err != nil {
			d.sl.Slog.Printf("[RL] action server stopped: %v\n", err)
		}
	}()

	fmt.Println("✅ RL 动作服务已启动：:9000")
}
