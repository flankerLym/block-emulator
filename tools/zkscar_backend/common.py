#!/usr/bin/env python3
import base64
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

BN254_MOD = 21888242871839275222246405745257275088548364400416034343698204186575808495617
MIMC_ROUNDS = 91
MAX_RVC_BATCH = 32
CHUNK_TREE_DEPTH = 16
MAX_RETIREMENT_RECEIPTS = 64
MAX_POST_CUTOVER_WRITES = 32

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
    return sha256_to_field((text or "").encode("utf-8"))


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


def ensure_chunk_artifacts(vk: Dict) -> Tuple[Path, Path, Path]:
    return ensure_rvc_artifacts(vk)


def ensure_retirement_artifacts(vk: Dict) -> Tuple[Path, Path, Path]:
    return ensure_rvc_artifacts(vk)


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


def retirement_debt_digest(settled_keys: List[str], outstanding_keys: List[str]) -> str:
    vals: List[int] = []
    for key in sorted(set(settled_keys or [])):
        vals.extend([hash_text_to_field(key), 1])
    for key in sorted(set(outstanding_keys or [])):
        vals.extend([hash_text_to_field(key), 0])
    return field_str(mimc_chain(vals))


def no_write_digest(write_keys: List[str]) -> str:
    entries = [f"write|{k}" for k in sorted(set(write_keys or []))]
    return field_str(mimc_chain([hash_text_to_field(x) for x in entries]))


def retirement_digest(address_binding: str, epoch_tag: int, from_shard: int, to_shard: int, settled_count: int, outstanding_count: int, write_count: int, debt_digest: str, no_write_digest_text: str, rvc_binding: str) -> str:
    return field_str(mimc_chain([
        decimal_text_to_field(address_binding),
        int(epoch_tag),
        int(from_shard),
        int(to_shard),
        int(settled_count),
        int(outstanding_count),
        int(write_count),
        decimal_text_to_field(debt_digest),
        decimal_text_to_field(no_write_digest_text),
        decimal_text_to_field(rvc_binding),
    ]))


def build_retirement_public_inputs_from_request(req: Dict) -> List[str]:
    return [
        str(req["address_binding"]),
        str(req["epoch_tag"]),
        str(req["from_shard"]),
        str(req["to_shard"]),
        str(req["hydrated_flag"]),
        str(req["debt_root_cleared_flag"]),
        str(req["settled_receipt_count"]),
        str(req["outstanding_receipt_count"]),
        str(req["post_cutover_write_count"]),
        str(req["debt_witness_digest"]),
        str(req["no_write_witness_digest"]),
        str(req["retirement_witness_digest"]),
        str(req["rvc_binding"]),
    ]


def build_retirement_circuit_input(req: Dict) -> Dict:
    bundle = json.loads(base64.b64decode(req["witness_bundle_b64"]).decode("utf-8"))
    settled_keys = sorted(set(bundle.get("settled_receipt_keys") or []))
    outstanding_keys = sorted(set(bundle.get("outstanding_receipt_keys") or []))
    write_keys = sorted(set(bundle.get("post_cutover_write_keys") or []))

    if len(settled_keys) + len(outstanding_keys) > MAX_RETIREMENT_RECEIPTS:
        raise RuntimeError(f"retirement receipt witness exceeds MAX_RETIREMENT_RECEIPTS={MAX_RETIREMENT_RECEIPTS}")
    if len(write_keys) > MAX_POST_CUTOVER_WRITES:
        raise RuntimeError(f"post-cutover write witness exceeds MAX_POST_CUTOVER_WRITES={MAX_POST_CUTOVER_WRITES}")

    expected_debt = retirement_debt_digest(settled_keys, outstanding_keys)
    if expected_debt != str(req["debt_witness_digest"]):
        raise RuntimeError("retirement debt witness digest mismatch before proving")

    expected_no_write = no_write_digest(write_keys)
    if expected_no_write != str(req["no_write_witness_digest"]):
        raise RuntimeError("retirement no-write digest mismatch before proving")

    expected_retirement = retirement_digest(
        str(req["address_binding"]),
        int(req["epoch_tag"]),
        int(req["from_shard"]),
        int(req["to_shard"]),
        int(req["settled_receipt_count"]),
        int(req["outstanding_receipt_count"]),
        int(req["post_cutover_write_count"]),
        str(req["debt_witness_digest"]),
        str(req["no_write_witness_digest"]),
        str(req["rvc_binding"]),
    )
    if expected_retirement != str(req["retirement_witness_digest"]):
        raise RuntimeError("retirement witness digest mismatch before proving")

    receipt_active = [1] * (len(settled_keys) + len(outstanding_keys))
    receipt_settled = [1] * len(settled_keys) + [0] * len(outstanding_keys)
    receipt_keys = [field_str(hash_text_to_field(k)) for k in settled_keys + outstanding_keys]
    while len(receipt_active) < MAX_RETIREMENT_RECEIPTS:
        receipt_active.append(0)
        receipt_settled.append(0)
        receipt_keys.append("0")

    write_active = [1] * len(write_keys)
    write_keys_field = [field_str(hash_text_to_field(f"write|{k}")) for k in write_keys]
    while len(write_active) < MAX_POST_CUTOVER_WRITES:
        write_active.append(0)
        write_keys_field.append("0")

    return {
        "addressBinding": str(req["address_binding"]),
        "epochTag": str(req["epoch_tag"]),
        "fromShard": str(req["from_shard"]),
        "toShard": str(req["to_shard"]),
        "hydratedFlag": str(req["hydrated_flag"]),
        "debtRootClearedFlag": str(req["debt_root_cleared_flag"]),
        "settledReceiptCount": str(req["settled_receipt_count"]),
        "outstandingReceiptCount": str(req["outstanding_receipt_count"]),
        "postCutoverWriteCount": str(req["post_cutover_write_count"]),
        "debtWitnessDigest": str(req["debt_witness_digest"]),
        "noWriteWitnessDigest": str(req["no_write_witness_digest"]),
        "retirementWitnessDigest": str(req["retirement_witness_digest"]),
        "rvcBinding": str(req["rvc_binding"]),
        "receiptActive": [str(x) for x in receipt_active],
        "receiptKey": receipt_keys,
        "receiptSettled": [str(x) for x in receipt_settled],
        "writeActive": [str(x) for x in write_active],
        "writeKey": write_keys_field,
    }
