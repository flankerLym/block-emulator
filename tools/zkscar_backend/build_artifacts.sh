#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
ART="$ROOT/artifacts"
CIR="$ROOT/circuits"
PTAU="$ART/pot12_final.ptau"
mkdir -p "$ART/rvc_semantic_batch" "$ART/chunk_membership"

if ! command -v circom >/dev/null 2>&1; then
  echo "circom not found in PATH" >&2
  exit 1
fi
if ! command -v snarkjs >/dev/null 2>&1; then
  echo "snarkjs not found in PATH" >&2
  exit 1
fi

if [ ! -f "$PTAU" ]; then
  snarkjs powersoftau new bn128 12 "$ART/pot12_0000.ptau" -v
  snarkjs powersoftau contribute "$ART/pot12_0000.ptau" "$ART/pot12_0001.ptau" --name="ZKSCAR" -v -e="zkscar"
  snarkjs powersoftau prepare phase2 "$ART/pot12_0001.ptau" "$PTAU" -v
fi

circom "$CIR/rvc_semantic_batch.circom" --r1cs --wasm --sym -o "$ART/rvc_semantic_batch"
snarkjs groth16 setup "$ART/rvc_semantic_batch/rvc_semantic_batch.r1cs" "$PTAU" "$ART/rvc_semantic_batch/rvc_semantic_batch_0000.zkey"
snarkjs zkey contribute "$ART/rvc_semantic_batch/rvc_semantic_batch_0000.zkey" "$ART/rvc_semantic_batch/rvc_semantic_batch_final.zkey" --name="ZKSCAR-RVC" -v -e="rvc"
snarkjs zkey export verificationkey "$ART/rvc_semantic_batch/rvc_semantic_batch_final.zkey" "$ART/rvc_semantic_batch/verification_key.json"

circom "$CIR/chunk_membership.circom" --r1cs --wasm --sym -o "$ART/chunk_membership"
snarkjs groth16 setup "$ART/chunk_membership/chunk_membership.r1cs" "$PTAU" "$ART/chunk_membership/chunk_membership_0000.zkey"
snarkjs zkey contribute "$ART/chunk_membership/chunk_membership_0000.zkey" "$ART/chunk_membership/chunk_membership_final.zkey" --name="ZKSCAR-CHUNK" -v -e="chunk"
snarkjs zkey export verificationkey "$ART/chunk_membership/chunk_membership_final.zkey" "$ART/chunk_membership/verification_key.json"

echo "Artifacts built under $ART"
