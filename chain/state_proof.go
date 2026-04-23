package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"errors"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type TrieProofNode struct {
	NodeKeyHex   string `json:"node_key_hex"`
	NodeValueHex string `json:"node_value_hex"`
}

type AccountTrieProof struct {
	Root       string          `json:"root"`
	RootType   string          `json:"root_type"`
	TrieKeyHex string          `json:"trie_key_hex"`
	ValueHex   string          `json:"value_hex"`
	ProofNodes []TrieProofNode `json:"proof_nodes"`
}

func normalizeHexString(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimPrefix(s, "0x")
	return s
}

func hexStringToBytes(s string) ([]byte, error) {
	s = normalizeHexString(s)
	if s == "" {
		return []byte{}, nil
	}
	return hex.DecodeString(s)
}

// proveTrieKeyCompat 兼容不同 go-ethereum 版本中的 Prove 签名：
// 1) Prove(key []byte, proofDb ethdb.KeyValueWriter) error
// 2) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error
func proveTrieKeyCompat(st any, key []byte, proofDB ethdb.KeyValueWriter) error {
	type prove2Args interface {
		Prove([]byte, ethdb.KeyValueWriter) error
	}
	type prove3Args interface {
		Prove([]byte, uint, ethdb.KeyValueWriter) error
	}

	switch t := st.(type) {
	case prove2Args:
		return t.Prove(key, proofDB)
	case prove3Args:
		return t.Prove(key, 0, proofDB)
	default:
		return errors.New("unsupported trie Prove signature")
	}
}

func (bc *BlockChain) BuildAccountProofAtStateRoot(root []byte, addr string) (*AccountTrieProof, error) {
	if len(root) == 0 {
		return nil, errors.New("empty state root")
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
	if err != nil {
		return nil, err
	}
	key := []byte(addr)
	value, err := st.Get(key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, errors.New("account value missing under supplied root")
	}
	proofDB := rawdb.NewMemoryDatabase()
	if err := proveTrieKeyCompat(st, key, proofDB); err != nil {
		return nil, err
	}
	iter := proofDB.NewIterator(nil, nil)
	defer iter.Release()
	nodes := make([]TrieProofNode, 0)
	for iter.Next() {
		nodes = append(nodes, TrieProofNode{
			NodeKeyHex:   hex.EncodeToString(iter.Key()),
			NodeValueHex: hex.EncodeToString(iter.Value()),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeKeyHex < nodes[j].NodeKeyHex
	})
	return &AccountTrieProof{
		Root:       hex.EncodeToString(root),
		RootType:   "mpt-state-root",
		TrieKeyHex: hex.EncodeToString(key),
		ValueHex:   hex.EncodeToString(value),
		ProofNodes: nodes,
	}, nil
}

// BuildSyntheticAccountProofForState constructs a virtual proof bundle for stage-1/2
// experiments when we need a stable witness for an account state without mutating the
// live trie in advance. VerifyAccountProof understands root types with prefix "virtual-"
// and directly returns the embedded ValueHex bytes.
func BuildSyntheticAccountProofForState(root []byte, addr string, state *core.AccountState, rootType string) *AccountTrieProof {
	if rootType == "" {
		rootType = "virtual-account-state"
	}
	valueHex := ""
	if state != nil {
		valueHex = hex.EncodeToString(state.Encode())
	}
	return &AccountTrieProof{
		Root:       hex.EncodeToString(root),
		RootType:   rootType,
		TrieKeyHex: hex.EncodeToString([]byte(addr)),
		ValueHex:   valueHex,
		ProofNodes: []TrieProofNode{},
	}
}

func VerifyAccountProof(proof *AccountTrieProof) ([]byte, error) {
	if proof == nil {
		return nil, errors.New("nil account proof")
	}
	// Stage-1/2 experimental mode:
	// allow virtual proofs that simply bind an address/state payload into the witness
	// bundle without requiring the state to exist as a trie leaf beforehand.
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(proof.RootType)), "virtual-") {
		return hexStringToBytes(proof.ValueHex)
	}

	rootBytes, err := hexStringToBytes(proof.Root)
	if err != nil {
		return nil, err
	}
	trieKey, err := hexStringToBytes(proof.TrieKeyHex)
	if err != nil {
		return nil, err
	}
	proofDB := rawdb.NewMemoryDatabase()
	for _, node := range proof.ProofNodes {
		k, err := hexStringToBytes(node.NodeKeyHex)
		if err != nil {
			return nil, err
		}
		v, err := hexStringToBytes(node.NodeValueHex)
		if err != nil {
			return nil, err
		}
		if err := proofDB.Put(k, v); err != nil {
			return nil, err
		}
	}
	value, err := trie.VerifyProof(common.BytesToHash(rootBytes), trieKey, proofDB)
	if err != nil {
		return nil, err
	}
	expected := normalizeHexString(proof.ValueHex)
	if expected != "" && hex.EncodeToString(value) != expected {
		return nil, errors.New("verified value mismatch")
	}
	return value, nil
}
