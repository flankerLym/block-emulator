package chain

import (
	"encoding/hex"
	"errors"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
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
	if err := st.Prove(key, proofDB); err != nil {
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

func VerifyAccountProof(proof *AccountTrieProof) ([]byte, error) {
	if proof == nil {
		return nil, errors.New("nil account proof")
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
