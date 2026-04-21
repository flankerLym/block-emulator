package message

import (
	"blockEmulator/core"
)

var (
	CHydrationRequest MessageType = "HydrationRequest"
	CHydrationData    MessageType = "HydrationData"
	CRetirementProof  MessageType = "RetirementProof"
)

type HydrationRequest struct {
	Algorithm string
	EpochTag  uint64
	FromShard uint64
	ToShard   uint64
	Addrs     []string
	RVCID     string
}

type HydrationData struct {
	Algorithm    string
	EpochTag     uint64
	FromShard    uint64
	ToShard      uint64
	Addrs        []string
	AccountState []*core.AccountState
	RVCID        string
}

type RetirementProof struct {
	Algorithm string
	EpochTag  uint64
	FromShard uint64
	ToShard   uint64
	Addrs     []string
	RVCID     string
	Proof     string
}
