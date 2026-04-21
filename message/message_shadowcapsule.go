package message

import "blockEmulator/core"

var (
	CHydrationRequest MessageType = "ShadowHydrationRequest"
	CHydrationData    MessageType = "ShadowHydrationData"
	CRetirementProof  MessageType = "ShadowRetirementProof"
)

type HydrationRequest struct {
	Addr      string
	EpochTag  uint64
	FromShard uint64
	ToShard   uint64
	Requester uint64
	NeedFull  bool
}

type HydrationData struct {
	Addr       string
	EpochTag   uint64
	FromShard  uint64
	ToShard    uint64
	FullState  *core.AccountState
	ChunkIndex uint64
	ChunkTotal uint64
	IsFinal    bool
}

type RetirementProof struct {
	Addr            string
	EpochTag        uint64
	FromShard       uint64
	ToShard         uint64
	Hydrated        bool
	DebtRootCleared bool
	RVCID           string
}