package chain

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"log"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

type shadowAccountEntry struct {
	state      *core.AccountState
	ownerShard uint64
	epochTag   uint64
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

func (bc *BlockChain) InstallShadowCapsule(capsule *message.ExecutionShadowCapsule) {
	if capsule == nil {
		return
	}
	rt := getShadowRuntime(bc)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.accounts[capsule.Addr] = &shadowAccountEntry{
		state: &core.AccountState{
			Nonce:       capsule.Nonce,
			Balance:     cloneBigInt(capsule.Balance),
			StorageRoot: cloneBytes(capsule.StorageRoot),
			CodeHash:    cloneBytes(capsule.CodeHash),
		},
		ownerShard: capsule.TargetShard,
		epochTag:   capsule.EpochTag,
	}
}

func (bc *BlockChain) GetShadowOwner(addr string) (uint64, bool) {
	rt := getShadowRuntime(bc)
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	entry, ok := rt.accounts[addr]
	if !ok {
		return 0, false
	}
	return entry.ownerShard, true
}

func (bc *BlockChain) GetShadowAccount(addr string) (*core.AccountState, bool) {
	rt := getShadowRuntime(bc)
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	entry, ok := rt.accounts[addr]
	if !ok {
		return nil, false
	}
	return cloneAccountState(entry.state), true
}

func (bc *BlockChain) ClearShadowAccounts(addrs []string) {
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
