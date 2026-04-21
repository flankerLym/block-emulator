package message

import (
	"blockEmulator/core"
	"blockEmulator/utils"
)

var (
	BrokerRawTx    MessageType = "brokerRawTx"
	BrokerConfirm1 MessageType = "brokerConfirm1"
	BrokerConfirm2 MessageType = "brokerConfirm2"
	BrokerType1    MessageType = "brokerType1"
	BrokerType2    MessageType = "brokerType2"

	CInjectBroker MessageType = "InjectTx_Broker"

	CBrokerTxMap MessageType = "BrokerTxMap"

	CAccountTransferMsg_broker MessageType = "BrokerAS_transfer"
	CInner2CrossTx             MessageType = "innerShardTx_be_crossShard"
)

type BrokerRawMeg struct {
	Tx        *core.Transaction
	Broker    utils.Address
	Hlock     uint64
	Snonce    uint64
	Bnonce    uint64
	Signature []byte
}

type BrokerType1Meg struct {
	RawMeg   *BrokerRawMeg
	Hcurrent uint64
	Broker   utils.Address
}

type Mag1Confirm struct {
	Tx1Hash []byte
	RawMeg  *BrokerRawMeg
}

type BrokerType2Meg struct {
	RawMeg *BrokerRawMeg
	Broker utils.Address
}

type Mag2Confirm struct {
	Tx2Hash []byte
	RawMeg  *BrokerRawMeg
}

type BrokerTxMap struct {
	BrokerTx2Broker12 map[string][]string
}

type InnerTx2CrossTx struct {
	Txs []*core.Transaction
}
