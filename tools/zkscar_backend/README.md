# ZK-SCAR external prover/verifier reference backend

This folder provides a strict out-of-process reference backend for ZK-SCAR.

Default commands used by `consensus_shard/pbft_all/zk_backend_mockable.go`:
- `python tools/zkscar_backend/rvc_prover.py`
- `python tools/zkscar_backend/rvc_verifier.py`
- `python tools/zkscar_backend/chunk_prover.py`
- `python tools/zkscar_backend/chunk_verifier.py`

Override with environment variables:
- `ZKSCAR_PROOF_BACKEND=external|legacy-mock`
- `ZKSCAR_EXTERNAL_RVC_PROVER`
- `ZKSCAR_EXTERNAL_RVC_VERIFIER`
- `ZKSCAR_EXTERNAL_CHUNK_PROVER`
- `ZKSCAR_EXTERNAL_CHUNK_VERIFIER`

Verifier-key metadata lives under `tools/zkscar_backend/verifier_keys/`.
