# ZK-SCAR strict proving backend

This directory removes the old JSON-digest mock path and replaces it with a fail-closed Groth16 proving flow.

## What is proved now

### RVC proof
The RVC proof is generated from a private **semantic witness bundle** derived from the source state proof, freeze state proof, and shadow capsule bundle. The Groth16 circuit enforces:

- source state == shadow capsule base fields (balance / nonce / code hash / storage root)
- freeze state == source state for the protected fields
- active witness count == batch size
- the private semantic witness bundle hashes to the public `semantic_witness_digest`
- the public statement is bound to `epoch / from shard / to shard / batch size / witness bundle binding / certificate binding`

The native Go verifier still verifies the actual MPT proofs and shadow-account installation. The Groth16 layer seals the semantic transition statement in a real proof instead of a mock digest.

### Chunk proof
The chunk proof is a Groth16 proof of Merkle membership for the chunk hash under the public `state_commitment` root. The payload hash itself is still checked natively in Go, and the zk proof seals the membership statement.

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
