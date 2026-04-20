package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"

	CPartitionMsg   MessageType = "PartitionModifiedMap"
	CPartitionReady MessageType = "ready for partition"
)

type ShadowCapsule struct {
	Addr         string
	CurrentShard uint64
	TargetShard  uint64
	Degree       int
	Hotness      float64
	LocalityGain float64
	Balance      string
	Nonce        uint64
	CodeHash     []byte
	StorageRoot  []byte
	DebtRoot     []byte
	EpochTag     uint64
	RVCID        string
}

type ReshardingValidityCertificate struct {
	Algorithm       string
	EpochTag        uint64
	FromShard       uint64
	ToShard         uint64
	CertificateID   string
	PartitionDigest string
	CapsuleDigest   string
	BalanceDigest   string
	Proof           string
}

type DualAnchorReceipt struct {
	TxHash     []byte
	Sender     string
	Recipient  string
	OldRoot    string
	ShadowRoot string
	FromShard  uint64
	ToShard    uint64
	EpochTag   uint64
}

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64
	Algorithm         string
	EpochTag          uint64
	ShadowCapsules    []ShadowCapsule
}

type AccountTransferMsg struct {
	ModifiedMap    map[string]uint64
	Addrs          []string
	AccountState   []*core.AccountState
	HydrationAddrs []string
	HydrationState []*core.AccountState
	ShadowCapsules []ShadowCapsule
	DualReceipts   []DualAnchorReceipt
	RVCs           []*ReshardingValidityCertificate
	Algorithm      string
	Stage          string
	ATid           uint64
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs          []string
	AccountState   []*core.AccountState
	HydrationAddrs []string
	HydrationState []*core.AccountState
	ShadowCapsules []ShadowCapsule
	DualReceipts   []DualAnchorReceipt
	RVC            *ReshardingValidityCertificate
	Algorithm      string
	Stage          string
	Txs            []*core.Transaction
	FromShard      uint64
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
