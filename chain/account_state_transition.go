package chain

import (
	"blockEmulator/core"
	"log"
)

func (bc *BlockChain) UpsertAccountsFull(addrs []string, states []*core.AccountState) {
	if len(addrs) != len(states) {
		log.Panic("UpsertAccountsFull: addrs and states length mismatch")
	}
	for i, addr := range addrs {
		if i >= len(states) {
			continue
		}
		bc.AccountMap[addr] = states[i]
	}
}

func (bc *BlockChain) DeleteAccounts(addrs []string) {
	for _, addr := range addrs {
		delete(bc.AccountMap, addr)
	}
}
