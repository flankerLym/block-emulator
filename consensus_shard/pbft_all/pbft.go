// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	RunningNode *shard.Node
	ShardID     uint64
	NodeID      uint64

	CurChain *chain.BlockChain
	db       ethdb.Database

	pbftChainConfig *params.ChainConfig
	ip_nodeTable    map[uint64]map[uint64]string
	node_nums       uint64
	malicious_nums  uint64

	view           atomic.Int32
	lastCommitTime atomic.Int64
	viewChangeMap  map[ViewChangeData]map[uint64]bool
	newViewMap     map[ViewChangeData]map[uint64]bool

	sequenceID        uint64
	stopSignal        atomic.Bool
	pStop             chan uint64
	requestPool       map[string]*message.Request
	cntPrepareConfirm map[string]map[*shard.Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool
	isCommitBordcast  map[string]bool
	isReply           map[string]bool
	height2Digest     map[uint64]string

	pbftStage              atomic.Int32
	pbftLock               sync.Mutex
	conditionalVarpbftLock sync.Cond

	sequenceLock sync.Mutex
	lock         sync.Mutex
	taskForLock  sync.Mutex

	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	pl *pbft_log.PbftLog

	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	ihm ExtraOpInConsensus
	ohm OpInterShards
}

func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := params.DatabaseWrite_path + "mptDB/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stopSignal.Store(false)
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3

	p.view.Store(0)
	p.lastCommitTime.Store(time.Now().Add(time.Second * 5).UnixMilli())
	p.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
	p.newViewMap = make(map[ViewChangeData]map[uint64]bool)

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	p.pbftStage.Store(1)

	return p
}

func normalizeWireFrame(msg []byte) []byte {
	msg = bytes.TrimRight(msg, "\r\n")
	if len(msg) == 0 {
		return nil
	}
	return msg
}

func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msg = normalizeWireFrame(msg)
	if len(msg) == 0 {
		return
	}
	if len(msg) < message.WireHeaderLen() {
		p.pl.Plog.Printf("S%dN%d : dropped malformed short frame, len=%d\n", p.ShardID, p.NodeID, len(msg))
		return
	}

	msgType, content := message.SplitMessage(msg)
	if msgType == message.MessageType("") {
		p.pl.Plog.Printf("S%dN%d : dropped malformed frame with empty message type, len=%d\n", p.ShardID, p.NodeID, len(msg))
		return
	}

	switch msgType {
	case message.CPrePrepare:
		go p.handlePrePrepare(content)
	case message.CPrepare:
		go p.handlePrepare(content)
	case message.CCommit:
		go p.handleCommit(content)

	case message.ViewChangePropose:
		p.handleViewChangeMsg(content)
	case message.NewChange:
		p.handleNewViewMsg(content)

	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)

	case message.CStop:
		p.WaitToStop()

	default:
		go p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.stopSignal.Load() {
			return
		}
		switch err {
		case nil:
			frame := normalizeWireFrame(clientRequest)
			if len(frame) == 0 {
				continue
			}
			p.tcpPoolLock.Lock()
			p.handleMessage(frame)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

func (p *PbftConsensusNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	p.stopSignal.Store(true)
	networks.CloseAllConnInPool()
	p.tcpln.Close()
	p.closePbft()
	p.pl.Plog.Println("handled stop message in TCPListen Routine")
	p.pStop <- 1
}

func (p *PbftConsensusNode) closePbft() {
	p.CurChain.CloseBlockChain()
}
