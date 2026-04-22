package chain

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type AccountTrieProof struct {
	Addr        string
	StateRoot   string
	AccountHash string
	Exists      bool
}

func hashAccountState(as *core.AccountState) string {
	if as == nil {
		sum := sha256.Sum256([]byte("nil-account-state"))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(as.Encode())
	return hex.EncodeToString(sum[:])
}

func NewSyntheticAccountTrieProof(stateRoot, addr, accountHash string, exists bool) *AccountTrieProof {
	return &AccountTrieProof{
		Addr:        addr,
		StateRoot:   stateRoot,
		AccountHash: accountHash,
		Exists:      exists,
	}
}

func VerifySyntheticAccountTrieProof(proof *AccountTrieProof, expectedRoot, expectedAddr, expectedHash string) bool {
	if proof == nil {
		return false
	}
	if !proof.Exists {
		return false
	}
	if proof.StateRoot != expectedRoot {
		return false
	}
	if proof.Addr != expectedAddr {
		return false
	}
	if proof.AccountHash != expectedHash {
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
	as := bc.getAccountStateNoLock(addr)
	if as == nil {
		return &AccountTrieProof{
			Addr:        addr,
			StateRoot:   "",
			AccountHash: "",
			Exists:      false,
		}, nil
	}
	return &AccountTrieProof{
		Addr:        addr,
		StateRoot:   hex.EncodeToString(bc.CurrentBlock.Header.StateRoot),
		AccountHash: hashAccountState(as),
		Exists:      true,
	}, nil
}
