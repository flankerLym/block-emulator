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

	PendingHydrationRequests map[string]*message.HydrationRequest
	PendingHydrationData     map[string]*message.HydrationData

	PendingHydrationChunks      map[string]map[uint64][]byte
	PendingHydrationChunkTotal  map[string]uint64
	PendingHydrationCommitments map[string]string
	PendingHydrationChunkSize   uint64

	// address -> local block height when the shadow account was first installed.
	// This is used to implement delayed / lazy hydration scheduling.
	ShadowInstallHeight map[string]uint64

	RVCPool               map[string]*message.ReshardingValidityCertificate
	DualAnchorReceiptPool map[string]*message.DualAnchorReceipt
	ShadowCapsulePool     map[string]*message.ShadowCapsule
	RetirementProofPool   map[string]*message.RetirementProof

	// address -> receiptKey -> true
	AddressReceiptIndex map[string]map[string]bool
	// receiptKey -> settled
	SettledDualAnchorReceipts map[string]bool
	// address -> post-cutover write key -> true
	PostCutoverWriteSet map[string]map[string]bool

	SourceCustodyState map[string]*core.AccountState
	RetiredAccounts    map[string]bool
}

func NewCLPADataSupport() *Data_supportCLPA {
	return &Data_supportCLPA{
		ModifiedMap:                 make([]map[string]uint64, 0),
		PartitionMeta:               make([]message.PartitionModifiedMap, 0),
		AccountTransferRound:        0,
		ReceivedNewAccountState:     make(map[string]*core.AccountState),
		ReceivedNewTx:               make([]*core.Transaction, 0),
		AccountStateTx:              make(map[uint64]*message.AccountStateAndTx),
		PartitionOn:                 false,
		PartitionReady:              make(map[uint64]bool),
		CollectOver:                 false,
		ReadySeq:                    make(map[uint64]uint64),
		OwnershipTransferred:        make(map[string]bool),
		HydratedAccounts:            make(map[string]bool),
		PendingHydrationRequests:    make(map[string]*message.HydrationRequest),
		PendingHydrationData:        make(map[string]*message.HydrationData),
		PendingHydrationChunks:      make(map[string]map[uint64][]byte),
		PendingHydrationChunkTotal:  make(map[string]uint64),
		PendingHydrationCommitments: make(map[string]string),
		PendingHydrationChunkSize:   128,
		ShadowInstallHeight:         make(map[string]uint64),
		RVCPool:                     make(map[string]*message.ReshardingValidityCertificate),
		DualAnchorReceiptPool:       make(map[string]*message.DualAnchorReceipt),
		ShadowCapsulePool:           make(map[string]*message.ShadowCapsule),
		RetirementProofPool:         make(map[string]*message.RetirementProof),
		AddressReceiptIndex:         make(map[string]map[string]bool),
		SettledDualAnchorReceipts:   make(map[string]bool),
		PostCutoverWriteSet:         make(map[string]map[string]bool),
		SourceCustodyState:          make(map[string]*core.AccountState),
		RetiredAccounts:             make(map[string]bool),
	}
}
