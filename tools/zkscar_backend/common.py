#!/usr/bin/env python3
import base64
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

BN254_MOD = 21888242871839275222246405745257275088548364400416034343698204186575808495617
MIMC_ROUNDS = 91
MAX_RVC_BATCH = 32
CHUNK_TREE_DEPTH = 16

ROOT = Path(__file__).resolve().parent
ARTIFACTS_ROOT = ROOT / "artifacts"
CIRCUITS_ROOT = ROOT / "circuits"
VK_DIR = ROOT / "verifier_keys"


def modp(v: int) -> int:
    return v % BN254_MOD


def sha256_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def sha256_to_field(data: bytes) -> int:
    return int.from_bytes(sha256_bytes(data), "big") % BN254_MOD


def hash_text_to_field(text: str) -> int:
    return sha256_to_field(text.encode("utf-8"))


def hash_hex_to_field(text: str) -> int:
    text = (text or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if text == "":
        return 0
    try:
        raw = bytes.fromhex(text)
    except ValueError:
        raw = text.encode("utf-8")
    return sha256_to_field(raw)


def decimal_text_to_field(text: str) -> int:
    return int(text or "0") % BN254_MOD


def mimc7(x: int) -> int:
    state = modp(x)
    for c in range(MIMC_ROUNDS):
        state = pow(modp(state + c), 7, BN254_MOD)
    return state


def mimc_chain(vals: List[int]) -> int:
    state = 0
    for v in vals:
        state = mimc7(modp(state + v))
    return state


def field_str(v: int) -> str:
    return str(modp(v))


def rvc_meta_path(vk_id: str) -> Path:
    return VK_DIR / f"{vk_id}.json"


def load_vk(vk_id: str) -> Dict:
    path = rvc_meta_path(vk_id)
    if not path.exists():
        raise FileNotFoundError(f"unknown verifier key: {vk_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def require_binary(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise RuntimeError(f"required binary not found in PATH: {name}")
    return path


def run(cmd: List[str], cwd: Path | None = None) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")


def ensure_rvc_artifacts(vk: Dict) -> Tuple[Path, Path, Path]:
    wasm = ROOT / vk["artifacts"]["wasm"]
    zkey = ROOT / vk["artifacts"]["zkey"]
    vkey = ROOT / vk["artifacts"]["vkey"]
    if not (wasm.exists() and zkey.exists() and vkey.exists()):
        raise RuntimeError(
            "Groth16 artifacts are missing. Run tools/zkscar_backend/build_artifacts.sh "
            "or tools/zkscar_backend/build_artifacts.ps1 first."
        )
    return wasm, zkey, vkey


def build_public_inputs_from_request(req: Dict) -> List[str]:
    return [
        str(req["epoch_tag"]),
        str(req["from_shard"]),
        str(req["to_shard"]),
        str(req["batch_size"]),
        str(req["semantic_witness_digest"]),
        str(req["witness_bundle_binding"]),
        str(req["certificate_binding"]),
    ]


def fieldize_semantic_witness(entry: Dict) -> Dict:
    return {
        "addr": hash_text_to_field(entry["addr"]),
        "src_bal": decimal_text_to_field(entry["source_balance"]),
        "src_nonce": int(entry["source_nonce"]),
        "src_code": hash_hex_to_field(entry["source_code_hash_hex"]),
        "src_store": hash_hex_to_field(entry["source_storage_root_hex"]),
        "freeze_bal": decimal_text_to_field(entry["freeze_balance"]),
        "freeze_nonce": int(entry["freeze_nonce"]),
        "freeze_code": hash_hex_to_field(entry["freeze_code_hash_hex"]),
        "freeze_store": hash_hex_to_field(entry["freeze_storage_root_hex"]),
        "cap_bal": decimal_text_to_field(entry["capsule_balance"]),
        "cap_nonce": int(entry["capsule_nonce"]),
        "cap_code": hash_hex_to_field(entry["capsule_code_hash_hex"]),
        "cap_store": hash_hex_to_field(entry["capsule_storage_root_hex"]),
        "debt_root": hash_hex_to_field(entry["debt_root_hex"]),
    }


def semantic_digest(epoch_tag: int, from_shard: int, to_shard: int, batch_size: int, witness_bundle_binding: str, certificate_binding: str, semantic_entries: List[Dict]) -> str:
    vals = [
        int(epoch_tag),
        int(from_shard),
        int(to_shard),
        int(batch_size),
        int(witness_bundle_binding),
        int(certificate_binding),
    ]
    for ent in semantic_entries:
        vals.extend([
            ent["addr"],
            ent["src_bal"],
            ent["src_nonce"],
            ent["src_code"],
            ent["src_store"],
            ent["freeze_bal"],
            ent["freeze_nonce"],
            ent["freeze_code"],
            ent["freeze_store"],
            ent["cap_bal"],
            ent["cap_nonce"],
            ent["cap_code"],
            ent["cap_store"],
            ent["debt_root"],
        ])
    return field_str(mimc_chain(vals))


def build_rvc_circuit_input(req: Dict) -> Dict:
    bundle = json.loads(base64.b64decode(req["witness_bundle_b64"]).decode("utf-8"))
    sem = bundle.get("semantic_witnesses") or []
    if len(sem) > MAX_RVC_BATCH:
        raise RuntimeError(f"semantic witness batch exceeds MAX_RVC_BATCH={MAX_RVC_BATCH}")
    fieldized = [fieldize_semantic_witness(x) for x in sem]
    expected_digest = semantic_digest(
        int(req["epoch_tag"]),
        int(req["from_shard"]),
        int(req["to_shard"]),
        int(req["batch_size"]),
        str(req["witness_bundle_binding"]),
        str(req["certificate_binding"]),
        fieldized,
    )
    if expected_digest != str(req["semantic_witness_digest"]):
        raise RuntimeError("semantic witness digest mismatch before proving")

    active = [1] * len(fieldized) + [0] * (MAX_RVC_BATCH - len(fieldized))

    def pad(key: str) -> List[str]:
        vals = [field_str(ent[key]) for ent in fieldized]
        vals.extend(["0"] * (MAX_RVC_BATCH - len(vals)))
        return vals

    def pad_nonce(key: str) -> List[str]:
        vals = [str(ent[key]) for ent in fieldized]
        vals.extend(["0"] * (MAX_RVC_BATCH - len(vals)))
        return vals

    return {
        "epochTag": str(req["epoch_tag"]),
        "fromShard": str(req["from_shard"]),
        "toShard": str(req["to_shard"]),
        "batchSize": str(req["batch_size"]),
        "semanticDigest": str(req["semantic_witness_digest"]),
        "witnessBundleBinding": str(req["witness_bundle_binding"]),
        "certificateBinding": str(req["certificate_binding"]),
        "active": [str(v) for v in active],
        "addr": pad("addr"),
        "sourceBalance": pad("src_bal"),
        "sourceNonce": pad_nonce("src_nonce"),
        "sourceCode": pad("src_code"),
        "sourceStore": pad("src_store"),
        "freezeBalance": pad("freeze_bal"),
        "freezeNonce": pad_nonce("freeze_nonce"),
        "freezeCode": pad("freeze_code"),
        "freezeStore": pad("freeze_store"),
        "capsuleBalance": pad("cap_bal"),
        "capsuleNonce": pad_nonce("cap_nonce"),
        "capsuleCode": pad("cap_code"),
        "capsuleStore": pad("cap_store"),
        "debtRoot": pad("debt_root"),
    }


def leaf_hash_field(hash_hex: str) -> int:
    return hash_hex_to_field(hash_hex)


def chunk_proof_json(req: Dict) -> Dict:
    return {
        "root": str(req["commitment"]),
        "leaf": field_str(leaf_hash_field(req["hash"])),
        "index": str(req["index"]),
        "total": str(req["total"]),
        "siblings": [str(x) for x in req.get("siblings", [])],
    }


def ensure_chunk_artifacts(vk: Dict) -> Tuple[Path, Path, Path]:
    return ensure_rvc_artifacts(vk)
