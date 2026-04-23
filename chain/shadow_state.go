package chain

import (
	"blockEmulator/core"
	"log"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

type shadowAccountEntry struct {
	state       *core.AccountState
	ownerShard  uint64
	sourceShard uint64
	epochTag    uint64
}

type shadowRuntime struct {
	mu       sync.RWMutex
	accounts map[string]*shadowAccountEntry
}

var shadowRegistry sync.Map // map[*BlockChain]*shadowRuntime

func getShadowRuntime(bc *BlockChain) *shadowRuntime {
	if v, ok := shadowRegistry.Load(bc); ok {
		return v.(*shadowRuntime)
	}
	rt := &shadowRuntime{accounts: make(map[string]*shadowAccountEntry)}
	actual, _ := shadowRegistry.LoadOrStore(bc, rt)
	return actual.(*shadowRuntime)
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(v)
}

func cloneAccountState(as *core.AccountState) *core.AccountState {
	if as == nil {
		return &core.AccountState{Balance: new(big.Int)}
	}
	return &core.AccountState{
		AcAddress:   as.AcAddress,
		Nonce:       as.Nonce,
		Balance:     cloneBigInt(as.Balance),
		StorageRoot: cloneBytes(as.StorageRoot),
		CodeHash:    cloneBytes(as.CodeHash),
	}
}

// InstallShadowCapsule installs the phase-1 executable shadow account on the
// target shard. This is the ownership-first, state-light handoff entry.
func (bc *BlockChain) InstallShadowCapsule(addr string, sourceShard, targetShard uint64, balance *big.Int, nonce uint64, codeHash, storageRoot []byte, epochTag uint64) {
	rt := getShadowRuntime(bc)
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.accounts[addr] = &shadowAccountEntry{
		state: &core.AccountState{
			Nonce:       nonce,
			Balance:     cloneBigInt(balance),
			StorageRoot: cloneBytes(storageRoot),
			CodeHash:    cloneBytes(codeHash),
		},
		ownerShard:  targetShard,
		sourceShard: sourceShard,
		epochTag:    epochTag,
	}
}

// InstallOwnershipHandoff installs a source-side ownership overlay without
// copying full state. It is used to block further local writes and to reroute
// late arrivals to the new owner shard immediately after cutover.
func (bc *BlockChain) InstallOwnershipHandoff(addr string, sourceShard, targetShard uint64, epochTag uint64) {
	rt := getShadowRuntime(bc)
	rt.mu.Lock()
	defer rt.mu.Unlock()

	entry, ok := rt.accounts[addr]
	if !ok {
		entry = &shadowAccountEntry{}
		rt.accounts[addr] = entry
	}
	entry.ownerShard = targetShard
	entry.sourceShard = sourceShard
	entry.epochTag = epochTag
}

func (bc *BlockChain) GetShadowOwner(addr string) (uint64, bool) {
	rt := getShadowRuntime(bc)
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	entry, ok := rt.accounts[addr]
	if !ok || entry.ownerShard == 0 {
		return 0, false
	}
	return entry.ownerShard, true
}

func (bc *BlockChain) GetShadowAccount(addr string) (*core.AccountState, bool) {
	rt := getShadowRuntime(bc)
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	entry, ok := rt.accounts[addr]
	if !ok || entry.state == nil {
		return nil, false
	}
	return cloneAccountState(entry.state), true
}

func (bc *BlockChain) RemoveShadowAccounts(addrs []string) {
	rt := getShadowRuntime(bc)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, addr := range addrs {
		delete(rt.accounts, addr)
	}
}

func (bc *BlockChain) HasAccountState(addr string) bool {
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	enc, _ := st.Get([]byte(addr))
	return enc != nil
}
