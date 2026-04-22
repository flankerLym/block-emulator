# ZK-SCAR strict proving backend

This directory removes the old JSON-digest mock path and replaces it with a fail-closed Groth16 proving flow.

## What is proved now

### RVC proof
The RVC proof is generated from a private **semantic witness bundle** derived from the source state proof, freeze state proof, and shadow capsule bundle. The Groth16 circuit enforces:

- source state == shadow capsule base fields (balance / nonce / code hash / storage root)
- freeze state == source state for the protected fields
- active witness count == batch size
- active witness addresses are pairwise unique inside the batch
- the private semantic witness bundle hashes to the public `semantic_witness_digest`
- the public statement is bound to `epoch / from shard / to shard / batch size / witness bundle binding / certificate binding`

The native Go verifier still verifies the actual MPT proofs and shadow-account installation. The Groth16 layer now seals the semantic transition statement **and batch uniqueness** in a real proof instead of leaving uniqueness entirely to native logic.

### Chunk proof
The chunk proof is a Groth16 proof of Merkle membership for the chunk hash under the public `state_commitment` root. The circuit now also enforces:

- `total > 0`
- `index < total`
- `total <= 2^DEPTH`

The payload hash itself is still checked natively in Go, and the zk proof seals the membership statement and basic chunk-range semantics.

## Still native today
These parts are still enforced outside the zk circuit:

- actual MPT membership verification for source / freeze / shadow proofs
- exact debt journal correctness behind `debtRoot`
- source-shard post-cutover write exclusion
- retirement proof semantics

## Toolchain

You need:

- `circom`
- `snarkjs`
- Python 3

Install `snarkjs` with npm if needed, then build artifacts:

### Linux / macOS
```bash
cd tools/zkscar_backend
./build_artifacts.sh
```

### Windows PowerShell
```powershell
cd tools/zkscar_backend
./build_artifacts.ps1
```

After artifacts are built, Go will call the Python wrappers automatically.

## Important
After replacing either circuit file, you **must rebuild artifacts** before running the Go project again.
