package message

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"log"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
)

// PartitionModifiedMap 继续兼容旧 CLPA 消息，新增可选的 ZK-SCAR 元信息字段。
// 旧逻辑只读取 PartitionModified，不会受影响。
type PartitionModifiedMap struct {
	PartitionModified map[string]uint64 `json:"PartitionModified"`

	Algorithm    string `json:"Algorithm,omitempty"`
	Epoch        uint64 `json:"Epoch,omitempty"`
	CapsuleCount int    `json:"CapsuleCount,omitempty"`
	ProofType    string `json:"ProofType,omitempty"`
}

// ShadowCapsule 是 ZK-SCAR 中的最小迁移胶囊。
// 当前工程版并未真的用它替代完整账户同步，而是先把它引入协议消息层。
type ShadowCapsule struct {
	Addr         string `json:"Addr"`
	CurrentShard uint64 `json:"CurrentShard"`
	TargetShard  uint64 `json:"TargetShard"`

	Balance     string `json:"Balance"`
	Nonce       uint64 `json:"Nonce"`
	CodeHash    []byte `json:"CodeHash"`
	StorageRoot []byte `json:"StorageRoot"`
	DebtRoot    []byte `json:"DebtRoot"`

	EpochTag uint64 `json:"EpochTag"`

	CapsuleRoot []byte `json:"CapsuleRoot"`

	OwnershipTransferred bool `json:"OwnershipTransferred"`
	HydrationFinished    bool `json:"HydrationFinished"`
}

func (sc *ShadowCapsule) Hash() []byte {
	if sc == nil {
		return nil
	}
	b, err := json.Marshal(sc)
	if err != nil {
		log.Panic(err)
	}
	h := sha256.Sum256(b)
	return h[:]
}

// ReshardingValidityCertificate 是伪 RVC 实现。
// 当前版本 Proof 只是协议占位，不是真正 zk 证明。
type ReshardingValidityCertificate struct {
	SourceShard uint64 `json:"SourceShard"`
	TargetShard uint64 `json:"TargetShard"`
	EpochTag    uint64 `json:"EpochTag"`

	AccountAddrs []string `json:"AccountAddrs"`
	CapsuleCount int      `json:"CapsuleCount"`

	CapsuleDigest []byte `json:"CapsuleDigest"`
	ReceiptDigest []byte `json:"ReceiptDigest"`

	IssuedAt int64  `json:"IssuedAt"`
	Proof    []byte `json:"Proof"`
}

func (rvc *ReshardingValidityCertificate) Hash() []byte {
	if rvc == nil {
		return nil
	}
	b, err := json.Marshal(rvc)
	if err != nil {
		log.Panic(err)
	}
	h := sha256.Sum256(b)
	return h[:]
}

func (rvc *ReshardingValidityCertificate) VerifyBasic() bool {
	if rvc == nil {
		return false
	}
	if len(rvc.AccountAddrs) != rvc.CapsuleCount {
		return false
	}
	if len(rvc.Proof) == 0 {
		return false
	}
	if len(rvc.CapsuleDigest) == 0 {
		return false
	}
	return true
}

// DualAnchorReceipt 是 ZK-SCAR 过渡期"旧根 + 影子根"的简化回执。
type DualAnchorReceipt struct {
	TxHash []byte `json:"TxHash"`

	OldShardRoot []byte `json:"OldShardRoot"`
	ShadowRoot   []byte `json:"ShadowRoot"`

	FromShard uint64 `json:"FromShard"`
	ToShard   uint64 `json:"ToShard"`
	EpochTag  uint64 `json:"EpochTag"`
}

func (dar *DualAnchorReceipt) Hash() []byte {
	if dar == nil {
		return nil
	}
	b, err := json.Marshal(dar)
	if err != nil {
		log.Panic(err)
	}
	h := sha256.Sum256(b)
	return h[:]
}

type AccountTransferMsg struct {
	ModifiedMap  map[string]uint64
	Addrs        []string
	AccountState []*core.AccountState
	ATid         uint64

	ShadowCapsules []*ShadowCapsule
	Certificates   []*ReshardingValidityCertificate
	Receipts       []*DualAnchorReceipt

	MigrationPhase string
	HydrationAddrs []string
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs        []string
	AccountState []*core.AccountState
	Txs          []*core.Transaction
	FromShard    uint64

	ShadowCapsules []*ShadowCapsule
	Certificate    *ReshardingValidityCertificate
	Receipts       []*DualAnchorReceipt
	Phase          string
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}