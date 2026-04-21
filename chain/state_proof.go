package chain

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

func cloneProofBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

// BuildAccountProofAtRoot exports a real trie proof for addr under the provided state root.
// It returns the encoded account state stored at the leaf plus the exact proof DB key/value pairs.
func (bc *BlockChain) BuildAccountProofAtRoot(root []byte, addr string) ([]byte, [][]byte, [][]byte, error) {
	if len(root) == 0 {
		return nil, nil, nil, fmt.Errorf("empty state root")
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(root)), bc.triedb)
	if err != nil {
		return nil, nil, nil, err
	}
	value, err := st.Get([]byte(addr))
	if err != nil {
		return nil, nil, nil, err
	}
	proofDB := rawdb.NewMemoryDatabase()
	if err := st.Prove([]byte(addr), 0, proofDB); err != nil {
		return nil, nil, nil, err
	}

	keys := make([][]byte, 0)
	vals := make([][]byte, 0)
	it := proofDB.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		keys = append(keys, cloneProofBytes(it.Key()))
		vals = append(vals, cloneProofBytes(it.Value()))
	}
	return cloneProofBytes(value), keys, vals, nil
}

// VerifyAccountProofAtRoot verifies the supplied proof DB key/value pairs under root for addr,
// and checks that the recovered value matches expectedValue exactly.
func VerifyAccountProofAtRoot(root []byte, addr string, expectedValue []byte, proofKeys [][]byte, proofVals [][]byte) (bool, error) {
	if len(root) == 0 {
		return false, fmt.Errorf("empty state root")
	}
	if len(proofKeys) != len(proofVals) {
		return false, fmt.Errorf("proof key/value length mismatch")
	}
	proofDB := rawdb.NewMemoryDatabase()
	for i := range proofKeys {
		if err := proofDB.Put(proofKeys[i], proofVals[i]); err != nil {
			return false, err
		}
	}
	got, err := trie.VerifyProof(common.BytesToHash(root), []byte(addr), proofDB)
	if err != nil {
		return false, err
	}
	return bytes.Equal(got, expectedValue), nil
}
