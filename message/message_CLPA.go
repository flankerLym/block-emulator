package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
	"math/big"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
	CShadowCapsule      MessageType = "ShadowCapsule"
)

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64
}

// AccountTransferMsg is the phase-2 reconciliation payload.
// Stage-1 shadow takeover can already hand ownership to the target shard,
// while this message carries the later full-state completion.
type AccountTransferMsg struct {
	ModifiedMap  map[string]uint64
	Addrs        []string
	AccountState []*core.AccountState
	ATid         uint64
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
}

// ExecutionShadowCapsule is the phase-1 execution capsule used for
// ownership-first, state-light takeover. It intentionally carries only
// the minimum executable account semantics needed by the target shard.
type ExecutionShadowCapsule struct {
	Addr        string
	SourceShard uint64
	TargetShard uint64
	Balance     *big.Int
	Nonce       uint64
	CodeHash    []byte
	StorageRoot []byte
	EpochTag    uint64
}

// ShadowCapsuleBatch is the transport unit sent from the source shard to
// every replica in the target shard before the phase-2 full migration commit.
type ShadowCapsuleBatch struct {
	FromShard uint64
	ToShard   uint64
	EpochTag  uint64
	Capsules  []ExecutionShadowCapsule
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
