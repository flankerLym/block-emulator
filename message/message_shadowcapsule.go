package message

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

	ChunkIndex         uint64
	ChunkSize          uint64
	ExpectedCommitment string
}

type HydrationData struct {
	Addr      string
	EpochTag  uint64
	FromShard uint64
	ToShard   uint64

	ChunkIndex uint64
	ChunkTotal uint64

	ChunkPayload []byte
	ChunkHash    string

	StateCommitment string
	ProofSystem     string
	ChunkProof      string

	IsFinal bool
}

type RetirementProof struct {
	Addr            string
	EpochTag        uint64
	FromShard       uint64
	ToShard         uint64
	Hydrated        bool
	DebtRootCleared bool

	SettledReceiptCount     uint64
	OutstandingReceiptCount uint64
	PostCutoverWriteCount   uint64

	AddressBinding          string
	RVCBinding              string
	DebtWitnessDigest       string
	NoWriteWitnessDigest    string
	RetirementWitnessDigest string

	RVCID string

	ProtocolVersion string
	CircuitVersion  string
	VerifierKeyID   string
	PublicInputs    []string
	ProofSystem     string
	ProofBytes      []byte
	ProofDigest     string
	ProofMode       string
}
