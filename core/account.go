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

type AccountState struct {
	AcAddress   utils.Address
	Nonce       uint64
	Balance     *big.Int
	StorageRoot []byte
	CodeHash    []byte

	DebtRoot             []byte
	EpochTag             uint64
	OwnershipTransferred bool
	Hydrated             bool
	PendingHydration     bool
	SourceShard          uint64
	TargetShard          uint64
	LastRVC              string
	Retired              bool
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
		Retired:              as.Retired,
	}
}

func (as *AccountState) IsShadow() bool {
	return as != nil && as.OwnershipTransferred && as.PendingHydration && !as.Hydrated && !as.Retired
}

func (as *AccountState) IsHydrated() bool {
	return as != nil && as.Hydrated && !as.PendingHydration && !as.Retired
}

func (as *AccountState) CanExecuteAsShadow() bool {
	return as != nil && as.OwnershipTransferred && !as.Retired
}

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
	shadow.Retired = false
	return shadow
}

func (as *AccountState) FinalizeHydration(epochTag uint64) *AccountState {
	full := as.Clone()
	full.EpochTag = epochTag
	full.PendingHydration = false
	full.Hydrated = true
	full.Retired = false
	return full
}

func (as *AccountState) ApplyHydration(full *AccountState, epochTag uint64) *AccountState {
	if as == nil && full == nil {
		return nil
	}
	if as == nil {
		return full.FinalizeHydration(epochTag)
	}
	if full == nil {
		cp := as.Clone()
		cp.EpochTag = epochTag
		cp.PendingHydration = false
		cp.Hydrated = true
		cp.Retired = false
		return cp
	}
	res := full.Clone()
	res.DebtRoot = cloneBytes(as.DebtRoot)
	res.EpochTag = epochTag
	res.OwnershipTransferred = true
	res.PendingHydration = false
	res.Hydrated = true
	res.SourceShard = as.SourceShard
	res.TargetShard = as.TargetShard
	res.LastRVC = as.LastRVC
	res.Retired = false
	return res
}

func (as *AccountState) BuildRetiredCopy(epochTag uint64) *AccountState {
	ret := as.Clone()
	ret.EpochTag = epochTag
	ret.PendingHydration = false
	ret.Hydrated = false
	ret.OwnershipTransferred = false
	ret.Retired = true
	return ret
}

func (as *AccountState) Deduct(val *big.Int) bool {
	if as.Balance.Cmp(val) < 0 {
		return false
	}
	as.Balance.Sub(as.Balance, val)
	return true
}

func (s *AccountState) Deposit(value *big.Int) {
	s.Balance.Add(s.Balance, value)
}

func (as *AccountState) Encode() []byte {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err := encoder.Encode(as)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAS(b []byte) *AccountState {
	var as AccountState
	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&as)
	if err != nil {
		log.Panic(err)
	}
	return &as
}

func (as *AccountState) Hash() []byte {
	h := sha256.Sum256(as.Encode())
	return h[:]
}
