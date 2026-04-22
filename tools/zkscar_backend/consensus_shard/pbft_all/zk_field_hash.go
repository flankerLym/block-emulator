package pbft_all

import (
	"blockEmulator/message"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"strings"
)

var bn254Modulus, _ = new(big.Int).SetString("21888242871839275222246405745257275088548364400416034343698204186575808495617", 10)

func modField(v *big.Int) *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	out := new(big.Int).Mod(v, bn254Modulus)
	if out.Sign() < 0 {
		out.Add(out, bn254Modulus)
	}
	return out
}

func sha256ToField(raw []byte) *big.Int {
	h := sha256.Sum256(raw)
	return modField(new(big.Int).SetBytes(h[:]))
}

func fieldFromString(s string) *big.Int {
	return sha256ToField([]byte(s))
}

func fieldFromHexString(s string) *big.Int {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return big.NewInt(0)
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return sha256ToField([]byte(s))
	}
	return sha256ToField(raw)
}

func fieldFromDecimalString(s string) *big.Int {
	if s == "" {
		return big.NewInt(0)
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return big.NewInt(0)
	}
	return modField(v)
}

func mimc7Round(x *big.Int) *big.Int {
	state := modField(new(big.Int).Set(x))
	for i := 0; i < 91; i++ {
		state.Add(state, big.NewInt(int64(i)))
		state = modField(state)
		state.Exp(state, big.NewInt(7), bn254Modulus)
	}
	return modField(state)
}

func mimcChain(vals []*big.Int) *big.Int {
	state := big.NewInt(0)
	for _, v := range vals {
		mix := new(big.Int).Add(state, modField(v))
		state = mimc7Round(modField(mix))
	}
	return modField(state)
}

func hashHexToFieldString(s string) string {
	return fieldFromHexString(s).String()
}

func hashTextToFieldString(s string) string {
	return fieldFromString(s).String()
}

func buildWitnessBundleBinding(rootHex string) string {
	return fieldFromHexString(rootHex).String()
}

func buildCertificateBinding(certID string) string {
	return fieldFromString(certID).String()
}

func semanticWitnessToFieldVals(w *message.RVCSemanticWitness) []*big.Int {
	if w == nil {
		return []*big.Int{big.NewInt(0)}
	}
	return []*big.Int{
		fieldFromString(w.Addr),
		fieldFromDecimalString(w.SourceBalance),
		new(big.Int).SetUint64(w.SourceNonce),
		fieldFromHexString(w.SourceCodeHashHex),
		fieldFromHexString(w.SourceStorageRootHex),
		fieldFromDecimalString(w.FreezeBalance),
		new(big.Int).SetUint64(w.FreezeNonce),
		fieldFromHexString(w.FreezeCodeHashHex),
		fieldFromHexString(w.FreezeStorageRootHex),
		fieldFromDecimalString(w.CapsuleBalance),
		new(big.Int).SetUint64(w.CapsuleNonce),
		fieldFromHexString(w.CapsuleCodeHashHex),
		fieldFromHexString(w.CapsuleStorageRootHex),
		fieldFromHexString(w.DebtRootHex),
	}
}

func buildSemanticWitnessDigest(epochTag, fromShard, toShard, batchSize uint64, witnessBundleBinding, certificateBinding string, semanticWitnesses []*message.RVCSemanticWitness) string {
	vals := []*big.Int{
		new(big.Int).SetUint64(epochTag),
		new(big.Int).SetUint64(fromShard),
		new(big.Int).SetUint64(toShard),
		new(big.Int).SetUint64(batchSize),
		fieldFromDecimalString(witnessBundleBinding),
		fieldFromDecimalString(certificateBinding),
	}
	for _, wit := range semanticWitnesses {
		vals = append(vals, semanticWitnessToFieldVals(wit)...)
	}
	return mimcChain(vals).String()
}

func fieldFromChunkHashHex(hashHex string) *big.Int {
	return fieldFromHexString(hashHex)
}

func buildChunkMerkleRoot(chunkHashes []string) (string, map[uint64][]string) {
	if len(chunkHashes) == 0 {
		return big.NewInt(0).String(), map[uint64][]string{0: []string{}}
	}
	origN := len(chunkHashes)
	leaves := make([]*big.Int, 0, origN)
	for _, h := range chunkHashes {
		leaves = append(leaves, fieldFromChunkHashHex(h))
	}
	nextPow := 1
	for nextPow < len(leaves) {
		nextPow <<= 1
	}
	for len(leaves) < nextPow {
		leaves = append(leaves, big.NewInt(0))
	}
	levels := make([][]*big.Int, 0)
	first := make([]*big.Int, len(leaves))
	copy(first, leaves)
	levels = append(levels, first)
	cur := first
	for len(cur) > 1 {
		nxt := make([]*big.Int, 0, len(cur)/2)
		for i := 0; i < len(cur); i += 2 {
			nxt = append(nxt, mimcChain([]*big.Int{cur[i], cur[i+1]}))
		}
		levels = append(levels, nxt)
		cur = nxt
	}
	paths := make(map[uint64][]string)
	for i := 0; i < origN; i++ {
		idx := i
		path := make([]string, 0)
		for level := 0; level < len(levels)-1; level++ {
			sibling := idx ^ 1
			path = append(path, levels[level][sibling].String())
			idx /= 2
		}
		paths[uint64(i)] = path
	}
	return levels[len(levels)-1][0].String(), paths
}
