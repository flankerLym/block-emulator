package message

// ZK-SCAR specific control-plane messages.
// These message types intentionally live in a dedicated file so they can be
// added to the existing message package without touching legacy CLPA / relay
// / broker message definitions.
var (
	CHydrationRequest MessageType = "ZKSCARHydrationRequest"
	CHydrationData    MessageType = "ZKSCARHydrationData"
	CRetirementProof  MessageType = "ZKSCARRetirementProof"
)

// HydrationRequest asks the source shard to stream the deferred full state of a
// previously installed shadow account. The request is chunked so that large
// states do not block cutover.
type HydrationRequest struct {
	Addr               string
	EpochTag           uint64
	FromShard          uint64
	ToShard            uint64
	Requester          uint64
	NeedFull           bool
	ChunkIndex         uint64
	ChunkSize          uint64
	ExpectedCommitment string
}

// HydrationData carries one state chunk plus a proof that the chunk belongs to
// the committed full-state payload.
type HydrationData struct {
	Addr            string
	EpochTag        uint64
	FromShard       uint64
	ToShard         uint64
	ChunkIndex      uint64
	ChunkTotal      uint64
	ChunkPayload    []byte
	ChunkHash       string
	StateCommitment string
	ProofSystem     string
	ChunkProof      string
	IsFinal         bool
}

// RetirementProof certifies that the source-shard custody copy can be safely
// decommissioned after deferred completion.
type RetirementProof struct {
	Addr      string
	EpochTag  uint64
	FromShard uint64
	ToShard   uint64

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
	RVCID                   string

	ProtocolVersion string
	CircuitVersion  string
	VerifierKeyID   string

	PublicInputs []string
	ProofSystem  string
	ProofBytes   []byte
	ProofDigest  string
	ProofMode    string
}
