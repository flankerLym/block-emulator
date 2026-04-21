package dataSupport

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"sync"
)

type Data_supportCLPA struct {
	ModifiedMap             []map[string]uint64
	PartitionMeta           []message.PartitionModifiedMap
	AccountTransferRound    uint64
	ReceivedNewAccountState map[string]*core.AccountState
	ReceivedNewTx           []*core.Transaction
	AccountStateTx          map[uint64]*message.AccountStateAndTx
	PartitionOn             bool

	PartitionReady map[uint64]bool
	P_ReadyLock    sync.Mutex

	ReadySeq     map[uint64]uint64
	ReadySeqLock sync.Mutex

	CollectOver bool
	CollectLock sync.Mutex

	OwnershipTransferred map[string]bool
	HydratedAccounts     map[string]bool

	PendingHydration      map[string]*core.AccountState
	PendingHydrationRound map[string]uint64

	RVCPool               map[string]*message.ReshardingValidityCertificate
	DualAnchorReceiptPool map[string]*message.DualAnchorReceipt
	ShadowCapsulePool     map[string]*message.ShadowCapsule
	OutgoingHydration     map[uint64]*message.AccountHydrationMsg
	RetirementProofPool   map[string]*message.RetirementProof
}

func NewCLPADataSupport() *Data_supportCLPA {
	return &Data_supportCLPA{
		ModifiedMap:             make([]map[string]uint64, 0),
		PartitionMeta:           make([]message.PartitionModifiedMap, 0),
		AccountTransferRound:    0,
		ReceivedNewAccountState: make(map[string]*core.AccountState),
		ReceivedNewTx:           make([]*core.Transaction, 0),
		AccountStateTx:          make(map[uint64]*message.AccountStateAndTx),
		PartitionOn:             false,
		PartitionReady:          make(map[uint64]bool),
		CollectOver:             false,
		ReadySeq:                make(map[uint64]uint64),

		OwnershipTransferred:  make(map[string]bool),
		HydratedAccounts:      make(map[string]bool),
		PendingHydration:      make(map[string]*core.AccountState),
		PendingHydrationRound: make(map[string]uint64),
		RVCPool:               make(map[string]*message.ReshardingValidityCertificate),
		DualAnchorReceiptPool: make(map[string]*message.DualAnchorReceipt),
		ShadowCapsulePool:     make(map[string]*message.ShadowCapsule),
		OutgoingHydration:     make(map[uint64]*message.AccountHydrationMsg),
		RetirementProofPool:   make(map[string]*message.RetirementProof),
	}
}
