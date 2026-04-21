package message

import (
	"blockEmulator/chain"
	"blockEmulator/core"
)

type Relay struct {
	Txs           []*core.Transaction
	SenderShardID uint64
	SenderSeq     uint64
}

type RelayWithProof struct {
	Txs           []*core.Transaction
	TxProofs      []chain.TxProofResult
	SenderShardID uint64
	SenderSeq     uint64
}
