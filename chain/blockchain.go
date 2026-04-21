package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	db           ethdb.Database
	triedb       *trie.Database
	ChainConfig  *params.ChainConfig
	CurrentBlock *core.Block
	Storage      *storage.Storage
	Txpool       *core.TxPool
	PartitionMap map[string]uint64
	pmlock       sync.RWMutex
	stateLock    sync.RWMutex
}

func GetTxTreeRoot(txs []*core.Transaction) []byte {
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	return transactionTree.Hash().Bytes()
}

func GetBloomFilter(txs []*core.Transaction) *bitset.BitSet {
	bs := bitset.New(2048)
	for _, tx := range txs {
		bs.Set(utils.ModBytes(tx.TxHash, 2048))
	}
	return bs
}

func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	bc.PartitionMap[key] = val
}

func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if _, ok := bc.PartitionMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	}
	return bc.PartitionMap[key]
}

func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	bc.Txpool.AddTxs2Pool(txs)
}

func (bc *BlockChain) getUpdateStatusTrieNoLock(txs []*core.Transaction) common.Hash {
	fmt.Printf("The len of txs is %d\n", len(txs))
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	cnt := 0
	for i, tx := range txs {
		if !tx.Relayed && (bc.Get_PartitionMap(tx.Sender) == bc.ChainConfig.ShardID || tx.HasBroker) {
			sStateEnc, _ := st.Get([]byte(tx.Sender))
			var sState *core.AccountState
			if sStateEnc == nil {
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				sState = &core.AccountState{Nonce: uint64(i), Balance: ib}
			} else {
				sState = core.DecodeAS(sStateEnc)
			}
			if sState.Retired {
				continue
			}
			if sState.Balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			sState.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), sState.Encode())
			cnt++
		}
		if bc.Get_PartitionMap(tx.Recipient) == bc.ChainConfig.ShardID || tx.HasBroker {
			rStateEnc, _ := st.Get([]byte(tx.Recipient))
			var rState *core.AccountState
			if rStateEnc == nil {
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				rState = &core.AccountState{Nonce: uint64(i), Balance: ib}
			} else {
				rState = core.DecodeAS(rStateEnc)
			}
			if rState.Retired {
				continue
			}
			rState.Deposit(tx.Value)
			if params.JustitiaEnabled {
				if tx.HasBroker && tx.Recipient == tx.FinalRecipient && tx.BrokerReward != nil {
					rState.Deposit(tx.BrokerReward)
				}
			}
			st.Update([]byte(tx.Recipient), rState.Encode())
			cnt++
		}
	}
	if cnt == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	rt, ns := st.Commit(false)
	if ns != nil {
		err = bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic(err)
		}
		err = bc.triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("modified account number is ", cnt)
	return rt
}

func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction) common.Hash {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	return bc.getUpdateStatusTrieNoLock(txs)
}

func (bc *BlockChain) GenerateBlock(miner int32) *core.Block {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()

	var txs []*core.Transaction
	if params.UseBlocksizeInBytes == 1 {
		txs = bc.Txpool.PackTxsWithBytes(params.BlocksizeInBytes)
	} else {
		txs = bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
	}

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
		Miner:           miner,
	}

	if params.JustitiaEnabled {
		bh.BlockReward = new(big.Int).Set(params.BlockReward)
		bh.CrossShardReward = new(big.Int)
		bh.BrokerReward = new(big.Int)
		for _, tx := range txs {
			if tx.HasBroker {
				tx.IsCrossShard = true
				tx.CrossShardReward = new(big.Int).Set(params.CrossShardRewardRate)
				bh.CrossShardReward.Add(bh.CrossShardReward, tx.CrossShardReward)
				if tx.SenderIsBroker || tx.Recipient == tx.FinalRecipient {
					tx.BrokerReward = new(big.Int).Set(params.BrokerRewardRate)
					bh.BrokerReward.Add(bh.BrokerReward, tx.BrokerReward)
				}
			}
			if tx.Fee == nil || tx.Fee.Cmp(params.MinTxFee) < 0 {
				tx.Fee = new(big.Int).Set(params.MinTxFee)
			}
		}
	}

	rt := bc.getUpdateStatusTrieNoLock(txs)
	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	bh.Bloom = *GetBloomFilter(txs)
	b := core.NewBlock(bh, txs)
	b.Hash = b.Header.Hash()
	return b
}

func (bc *BlockChain) NewGenisisBlock() *core.Block {
	body := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{Number: 0, Miner: 0}
	if params.JustitiaEnabled {
		bh.BlockReward = new(big.Int)
		bh.CrossShardReward = new(big.Int)
		bh.BrokerReward = new(big.Int)
		bh.IncentiveProof = nil
	}
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{Cache: 0, Preimages: true})
	bc.triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	bh.StateRoot = statusTrie.Hash().Bytes()
	bh.TxRoot = GetTxTreeRoot(body)
	bh.Bloom = *GetBloomFilter(body)
	b := core.NewBlock(bh, body)
	b.Hash = b.Header.Hash()
	return b
}

func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()

	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}

func (bc *BlockChain) AddBlock(b *core.Block) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()

	if b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		fmt.Println("the block height is not correct")
		return
	}
	if !bytes.Equal(b.Header.ParentBlockHash, bc.CurrentBlock.Hash) {
		fmt.Println("err parent block hash")
		return
	}

	_, err := trie.New(trie.TrieID(common.BytesToHash(b.Header.StateRoot)), bc.triedb)
	if err != nil {
		rt := bc.getUpdateStatusTrieNoLock(b.Body)
		fmt.Println(bc.CurrentBlock.Header.Number+1, "the root = ", rt.Bytes())
		if params.JustitiaEnabled {
			st, err := trie.New(trie.TrieID(common.BytesToHash(rt.Bytes())), bc.triedb)
			if err != nil {
				log.Panic(err)
			}
			minerAddr := utils.Int2Addr(uint64(b.Header.Miner))
			minerStateEnc, _ := st.Get([]byte(minerAddr))
			var minerState *core.AccountState
			if minerStateEnc == nil {
				minerState = &core.AccountState{Nonce: 0, Balance: new(big.Int)}
			} else {
				minerState = core.DecodeAS(minerStateEnc)
			}
			if b.Header.BlockReward != nil {
				minerState.Deposit(b.Header.BlockReward)
			}
			if b.Header.CrossShardReward != nil {
				minerState.Deposit(b.Header.CrossShardReward)
			}
			if b.Header.BrokerReward != nil {
				minerState.Deposit(b.Header.BrokerReward)
			}
			st.Update([]byte(minerAddr), minerState.Encode())
			newRt, ns := st.Commit(false)
			if ns != nil {
				err = bc.triedb.Update(trie.NewWithNodeSet(ns))
				if err != nil {
					log.Panic(err)
				}
				err = bc.triedb.Commit(newRt, false)
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}
	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	fmt.Println("Generating a new blockchain", db)
	chainDBfp := params.DatabaseWrite_path + fmt.Sprintf("chainDB/S%d_N%d", cc.ShardID, cc.NodeID)
	bc := &BlockChain{
		db:           db,
		ChainConfig:  cc,
		Txpool:       core.NewTxPool(),
		Storage:      storage.NewStorage(chainDBfp, cc),
		PartitionMap: make(map[string]uint64),
	}
	curHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		fmt.Println("There is no existed blockchain in the database. ")
		if err.Error() == "cannot find the newest block hash" {
			genisisBlock := bc.NewGenisisBlock()
			bc.AddGenisisBlock(genisisBlock)
			fmt.Println("New genisis block")
			return bc, nil
		}
		log.Panic()
	}

	fmt.Println("Existing blockchain found")
	curb, err := bc.Storage.GetBlock(curHash)
	if err != nil {
		log.Panic()
	}

	bc.CurrentBlock = curb
	triedb := trie.NewDatabaseWithConfig(db, &trie.Config{Cache: 0, Preimages: true})
	bc.triedb = triedb
	_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")
	fmt.Println("Generated a new blockchain successfully")
	return bc, nil
}

func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	bc.stateLock.RLock()
	defer bc.stateLock.RUnlock()

	if string(b.Header.ParentBlockHash) != string(bc.CurrentBlock.Hash) {
		fmt.Println("the parentblock hash is not equal to the current block hash")
		return errors.New("the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
		fmt.Println("the transaction root is wrong")
		return errors.New("the transaction root is wrong")
	}
	return nil
}

func (bc *BlockChain) commitStateTrieNoLock(st *trie.Trie) []byte {
	rt := bc.CurrentBlock.Header.StateRoot
	rrt, ns := st.Commit(false)
	if ns != nil {
		err := bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic(err)
		}
		err = bc.triedb.Commit(rrt, false)
		if err != nil {
			log.Panic(err)
		}
		rt = rrt.Bytes()
	}
	return rt
}

func (bc *BlockChain) buildEmptyStateBlockNoLock(rt []byte) {
	emptyTxs := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
		StateRoot:       rt,
		TxRoot:          GetTxTreeRoot(emptyTxs),
		Bloom:           *GetBloomFilter(emptyTxs),
		Miner:           0,
	}
	b := core.NewBlock(bh, emptyTxs)
	b.Hash = b.Header.Hash()
	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

func (bc *BlockChain) getAccountStateNoLock(addr string) *core.AccountState {
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	asenc, _ := st.Get([]byte(addr))
	if asenc == nil {
		ib := new(big.Int)
		ib.Add(ib, params.Init_Balance)
		return &core.AccountState{Nonce: 0, Balance: ib}
	}
	return core.DecodeAS(asenc)
}

func (bc *BlockChain) GetAccountState(addr string) *core.AccountState {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	return bc.getAccountStateNoLock(addr)
}

func (bc *BlockChain) putAccountStatesNoLock(ac []string, as []*core.AccountState) {
	if len(ac) == 0 {
		return
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for i, addr := range ac {
		if i >= len(as) || as[i] == nil {
			continue
		}
		if bc.Get_PartitionMap(addr) != bc.ChainConfig.ShardID {
			continue
		}
		st.Update([]byte(addr), as[i].Encode())
	}
	rt := bc.commitStateTrieNoLock(st)
	bc.buildEmptyStateBlockNoLock(rt)
}

func (bc *BlockChain) PutAccountState(addr string, as *core.AccountState) {
	bc.PutAccountStates([]string{addr}, []*core.AccountState{as})
}

func (bc *BlockChain) PutAccountStates(ac []string, as []*core.AccountState) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	bc.putAccountStatesNoLock(ac, as)
}

func (bc *BlockChain) deleteAccountsNoLock(addrs []string) {
	if len(addrs) == 0 {
		return
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		st.Delete([]byte(addr))
	}
	rt := bc.commitStateTrieNoLock(st)
	bc.buildEmptyStateBlockNoLock(rt)
}

func (bc *BlockChain) DeleteAccounts(addrs []string) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	bc.deleteAccountsNoLock(addrs)
}

func (bc *BlockChain) FreezeAccount(addr string, epochTag uint64) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	state := bc.getAccountStateNoLock(addr)
	if state == nil {
		return
	}
	frozen := state.BuildRetiredCopy(epochTag)
	bc.putAccountStatesNoLock([]string{addr}, []*core.AccountState{frozen})
}

func (bc *BlockChain) AddAccounts(ac []string, as []*core.AccountState, miner int32) {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()

	fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))
	if len(ac) == 0 {
		return
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for i, addr := range ac {
		if i >= len(as) || as[i] == nil {
			continue
		}
		if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
			st.Update([]byte(addr), as[i].Encode())
		}
	}
	rt := bc.commitStateTrieNoLock(st)
	bc.buildEmptyStateBlockNoLock(rt)
}

func (bc *BlockChain) fetchAccountsNoLock(addrs []string) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		asenc, _ := st.Get([]byte(addr))
		var stateA *core.AccountState
		if asenc == nil {
			ib := new(big.Int)
			ib.Add(ib, params.Init_Balance)
			stateA = &core.AccountState{Nonce: 0, Balance: ib}
		} else {
			stateA = core.DecodeAS(asenc)
		}
		res = append(res, stateA)
	}
	return res
}

func (bc *BlockChain) FetchAccounts(addrs []string) []*core.AccountState {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	return bc.fetchAccountsNoLock(addrs)
}

func (bc *BlockChain) CloseBlockChain() {
	bc.stateLock.Lock()
	defer bc.stateLock.Unlock()
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
	bc.db.Close()
}

func (bc *BlockChain) PrintBlockChain() string {
	bc.stateLock.RLock()
	defer bc.stateLock.RUnlock()
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}
