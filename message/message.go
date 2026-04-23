package message

import (
	"blockEmulator/core"
	"blockEmulator/shard"
	"time"
)

var prefixMSGtypeLen = 30

type MessageType string
type RequestType string

const (
	CPrePrepare        MessageType = "preprepare"
	CPrepare           MessageType = "prepare"
	CCommit            MessageType = "commit"
	CRequestOldrequest MessageType = "requestOldrequest"
	CSendOldrequest    MessageType = "sendOldrequest"
	CStop              MessageType = "stop"

	CRelay          MessageType = "relay"
	CRelayWithProof MessageType = "CRelay&Proof"
	CInject         MessageType = "inject"

	CBlockInfo MessageType = "BlockInfo"
	CSeqIDinfo MessageType = "SequenceID"
)

var (
	BlockRequest RequestType = "Block"
)

type RawMessage struct {
	Content []byte
}

type Request struct {
	RequestType RequestType
	Msg         RawMessage
	ReqTime     time.Time
}

type PrePrepare struct {
	RequestMsg *Request
	Digest     []byte
	SeqID      uint64
}

type Prepare struct {
	Digest     []byte
	SeqID      uint64
	SenderNode *shard.Node
}

type Commit struct {
	Digest     []byte
	SeqID      uint64
	SenderNode *shard.Node
}

type Reply struct {
	MessageID  uint64
	SenderNode *shard.Node
	Result     bool
}

type RequestOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	ServerNode     *shard.Node
	SenderNode     *shard.Node
}

type SendOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	OldRequest     []*Request
	SenderNode     *shard.Node
}

type InjectTxs struct {
	Txs       []*core.Transaction
	ToShardID uint64
}

type BlockInfoMsg struct {
	BlockBodyLength int
	InnerShardTxs   []*core.Transaction
	Epoch           int

	ProposeTime   time.Time
	CommitTime    time.Time
	SenderShardID uint64

	Relay1Txs []*core.Transaction
	Relay2Txs []*core.Transaction

	Broker1Txs []*core.Transaction
	Broker2Txs []*core.Transaction
}

type SeqIDinfo struct {
	SenderShardID uint64
	SenderSeq     uint64
}

func MergeMessage(msgType MessageType, content []byte) []byte {
	b := make([]byte, prefixMSGtypeLen)
	for i, v := range []byte(msgType) {
		b[i] = v
	}
	return append(b, content...)
}

// WireHeaderLen returns the fixed byte length of the message type prefix.
func WireHeaderLen() int {
	return prefixMSGtypeLen
}

func SplitMessage(message []byte) (MessageType, []byte) {
	if len(message) < prefixMSGtypeLen {
		return MessageType(""), nil
	}

	msgTypeBytes := message[:prefixMSGtypeLen]
	msgTypePruned := make([]byte, 0)
	for _, v := range msgTypeBytes {
		if v != byte(0) {
			msgTypePruned = append(msgTypePruned, v)
		}
	}
	content := message[prefixMSGtypeLen:]
	return MessageType(string(msgTypePruned)), content
}
