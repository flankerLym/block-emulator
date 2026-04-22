package chain

import (
	"encoding/hex"
	"errors"
)

func NewSyntheticAccountTrieProof(stateRoot, addr, accountHash string, exists bool) *AccountTrieProof {
	valueHex := ""
	if exists {
		valueHex = accountHash
	}
	return &AccountTrieProof{
		Root:       stateRoot,
		RootType:   "synthetic-account-proof",
		TrieKeyHex: hex.EncodeToString([]byte(addr)),
		ValueHex:   valueHex,
		ProofNodes: nil,
	}
}

func VerifySyntheticAccountTrieProof(proof *AccountTrieProof, expectedRoot, expectedAddr, expectedHash string) bool {
	if proof == nil {
		return false
	}
	if proof.Root != expectedRoot {
		return false
	}
	if proof.TrieKeyHex != hex.EncodeToString([]byte(expectedAddr)) {
		return false
	}
	if proof.ValueHex != expectedHash {
		return false
	}
	return true
}

func (bc *BlockChain) BuildAccountTrieProof(addr string) (*AccountTrieProof, error) {
	bc.stateLock.RLock()
	defer bc.stateLock.RUnlock()

	if bc == nil || bc.CurrentBlock == nil || bc.CurrentBlock.Header == nil {
		return nil, errors.New("blockchain state is not initialized")
	}
	if len(bc.CurrentBlock.Header.StateRoot) == 0 {
		return nil, errors.New("empty current state root")
	}
	return bc.BuildAccountProofAtStateRoot(bc.CurrentBlock.Header.StateRoot, addr)
}
