// Account, AccountState
// Some basic operation about accountState

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"math/big"
)

type Account struct {
	AcAddress utils.Address
	PublicKey []byte
}

// AccoutState record the details of an account, it will be saved in status trie
type AccountState struct {
	AcAddress   utils.Address // this part is not useful, abort
	Nonce       uint64
	Balance     *big.Int
	StorageRoot []byte // only for smart contract account
	CodeHash    []byte // only for smart contract account

	// ---- ZK-SCAR metadata ----
	DebtRoot             []byte
	EpochTag             uint64
	OwnershipTransferred bool
	Hydrated             bool
	PendingHydration     bool
	SourceShard          uint64
	TargetShard          uint64
	LastRVC              string
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(v)
}

func cloneBytes(v []byte) []byte {
	if v == nil {
		return nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp
}

func (as *AccountState) Clone() *AccountState {
	if as == nil {
		return nil
	}
	return &AccountState{
		AcAddress:            as.AcAddress,
		Nonce:                as.Nonce,
		Balance:              cloneBigInt(as.Balance),
		StorageRoot:          cloneBytes(as.StorageRoot),
		CodeHash:             cloneBytes(as.CodeHash),
		DebtRoot:             cloneBytes(as.DebtRoot),
		EpochTag:             as.EpochTag,
		OwnershipTransferred: as.OwnershipTransferred,
		Hydrated:             as.Hydrated,
		PendingHydration:     as.PendingHydration,
		SourceShard:          as.SourceShard,
		TargetShard:          as.TargetShard,
		LastRVC:              as.LastRVC,
	}
}

// BuildShadowState constructs the "ownership-transferred but not fully hydrated" state.
func (as *AccountState) BuildShadowState(epochTag, sourceShard, targetShard uint64, debtRoot []byte, rvcID string) *AccountState {
	shadow := as.Clone()
	shadow.DebtRoot = cloneBytes(debtRoot)
	shadow.EpochTag = epochTag
	shadow.OwnershipTransferred = true
	shadow.Hydrated = false
	shadow.PendingHydration = true
	shadow.SourceShard = sourceShard
	shadow.TargetShard = targetShard
	shadow.LastRVC = rvcID
	return shadow
}

// FinalizeHydration marks the state as fully hydrated.
func (as *AccountState) FinalizeHydration(epochTag uint64) *AccountState {
	full := as.Clone()
	full.EpochTag = epochTag
	full.PendingHydration = false
	full.Hydrated = true
	return full
}

// Reduce the balance of an account
func (as *AccountState) Deduct(val *big.Int) bool {
	if as.Balance.Cmp(val) < 0 {
		return false
	}
	as.Balance.Sub(as.Balance, val)
	return true
}

// Increase the balance of an account
func (s *AccountState) Deposit(value *big.Int) {
	s.Balance.Add(s.Balance, value)
}

// Encode AccountState in order to store in the MPT
func (as *AccountState) Encode() []byte {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err := encoder.Encode(as)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode AccountState
func DecodeAS(b []byte) *AccountState {
	var as AccountState

	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&as)
	if err != nil {
		log.Panic(err)
	}
	return &as
}

// Hash AccountState for computing the MPT Root
func (as *AccountState) Hash() []byte {
	h := sha256.Sum256(as.Encode())
	return h[:]
}
