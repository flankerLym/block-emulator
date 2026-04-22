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
RETIREMENT_MAX_RECEIPTS = 64
RETIREMENT_MAX_WRITES = 32

ROOT = Path(__file__).resolve().parent
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


def load_vk(vk_id: str) -> Dict:
    path = VK_DIR / f"{vk_id}.json"
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


def retirement_receipt_entries(req: Dict) -> List[Dict]:
    entries = []
    for item in req.get("receipt_witnesses", []) or []:
        entries.append({
            "key": str(item["key"]),
            "settled": bool(item["settled"]),
        })
    entries.sort(key=lambda x: x["key"])
    if len(entries) > RETIREMENT_MAX_RECEIPTS:
        raise RuntimeError(f"retirement receipt witnesses exceed RETIREMENT_MAX_RECEIPTS={RETIREMENT_MAX_RECEIPTS}")
    return entries


def retirement_write_keys(req: Dict) -> List[str]:
    keys = sorted([str(x) for x in (req.get("write_keys", []) or [])])
    if len(keys) > RETIREMENT_MAX_WRITES:
        raise RuntimeError(f"retirement write witnesses exceed RETIREMENT_MAX_WRITES={RETIREMENT_MAX_WRITES}")
    return keys


def retirement_debt_digest(epoch_tag: int, from_shard: int, to_shard: int, account_binding: str, rvc_binding: str, receipts: List[Dict]) -> str:
    vals = [
        int(epoch_tag),
        int(from_shard),
        int(to_shard),
        int(account_binding),
        int(rvc_binding),
    ]
    for i in range(RETIREMENT_MAX_RECEIPTS):
        if i < len(receipts):
            vals.extend([
                1,
                hash_text_to_field(receipts[i]["key"]),
                1 if receipts[i]["settled"] else 0,
            ])
        else:
            vals.extend([0, 0, 0])
    return field_str(mimc_chain(vals))


def retirement_no_write_digest(epoch_tag: int, from_shard: int, to_shard: int, account_binding: str, rvc_binding: str, write_keys: List[str]) -> str:
    vals = [
        int(epoch_tag),
        int(from_shard),
        int(to_shard),
        int(account_binding),
        int(rvc_binding),
    ]
    for i in range(RETIREMENT_MAX_WRITES):
        if i < len(write_keys):
            vals.extend([1, hash_text_to_field(write_keys[i])])
        else:
            vals.extend([0, 0])
    return field_str(mimc_chain(vals))


def retirement_witness_digest(req: Dict) -> str:
    hydrated = 1 if req.get("hydrated_flag") else 0
    debt_cleared = 1 if req.get("debt_root_cleared_flag") else 0
    vals = [
        int(req["epoch_tag"]),
        int(req["from_shard"]),
        int(req["to_shard"]),
        hydrated,
        debt_cleared,
        int(req["settled_receipt_count"]),
        int(req["outstanding_receipt_count"]),
        int(req["post_cutover_write_count"]),
        int(req["debt_witness_digest"]),
        int(req["no_write_witness_digest"]),
        int(req["account_binding"]),
        int(req["rvc_binding"]),
    ]
    return field_str(mimc_chain(vals))


def build_retirement_public_inputs_from_request(req: Dict) -> List[str]:
    hydrated = "1" if req.get("hydrated_flag") else "0"
    debt_cleared = "1" if req.get("debt_root_cleared_flag") else "0"
    return [
        str(req["epoch_tag"]),
        str(req["from_shard"]),
        str(req["to_shard"]),
        hydrated,
        debt_cleared,
        str(req["settled_receipt_count"]),
        str(req["outstanding_receipt_count"]),
        str(req["post_cutover_write_count"]),
        str(req["debt_witness_digest"]),
        str(req["no_write_witness_digest"]),
        str(req["retirement_witness_digest"]),
        str(req["account_binding"]),
        str(req["rvc_binding"]),
    ]


def build_retirement_circuit_input(req: Dict) -> Dict:
    receipts = retirement_receipt_entries(req)
    write_keys = retirement_write_keys(req)

    settled_count = sum(1 for x in receipts if x["settled"])
    outstanding_count = sum(1 for x in receipts if not x["settled"])
    if settled_count != int(req["settled_receipt_count"]):
        raise RuntimeError("settled receipt count mismatch before proving")
    if outstanding_count != int(req["outstanding_receipt_count"]):
        raise RuntimeError("outstanding receipt count mismatch before proving")
    if len(write_keys) != int(req["post_cutover_write_count"]):
        raise RuntimeError("post-cutover write count mismatch before proving")

    expected_debt = retirement_debt_digest(
        int(req["epoch_tag"]),
        int(req["from_shard"]),
        int(req["to_shard"]),
        str(req["account_binding"]),
        str(req["rvc_binding"]),
        receipts,
    )
    if expected_debt != str(req["debt_witness_digest"]):
        raise RuntimeError("retirement debt witness digest mismatch before proving")

    expected_no_write = retirement_no_write_digest(
        int(req["epoch_tag"]),
        int(req["from_shard"]),
        int(req["to_shard"]),
        str(req["account_binding"]),
        str(req["rvc_binding"]),
        write_keys,
    )
    if expected_no_write != str(req["no_write_witness_digest"]):
        raise RuntimeError("retirement no-write digest mismatch before proving")

    expected_retirement_digest = retirement_witness_digest(req)
    if expected_retirement_digest != str(req["retirement_witness_digest"]):
        raise RuntimeError("retirement witness digest mismatch before proving")

    expected_public = build_retirement_public_inputs_from_request(req)
    if [str(x) for x in req.get("public_inputs", [])] != expected_public:
        raise RuntimeError("retirement public inputs mismatch before proving")

    receipt_active = []
    receipt_key = []
    receipt_settled = []
    for item in receipts:
        receipt_active.append("1")
        receipt_key.append(field_str(hash_text_to_field(item["key"])))
        receipt_settled.append("1" if item["settled"] else "0")
    while len(receipt_active) < RETIREMENT_MAX_RECEIPTS:
        receipt_active.append("0")
        receipt_key.append("0")
        receipt_settled.append("0")

    write_active = []
    write_key = []
    for item in write_keys:
        write_active.append("1")
        write_key.append(field_str(hash_text_to_field(item)))
    while len(write_active) < RETIREMENT_MAX_WRITES:
        write_active.append("0")
        write_key.append("0")

    return {
        "epochTag": str(req["epoch_tag"]),
        "fromShard": str(req["from_shard"]),
        "toShard": str(req["to_shard"]),
        "hydratedFlag": "1" if req.get("hydrated_flag") else "0",
        "debtRootClearedFlag": "1" if req.get("debt_root_cleared_flag") else "0",
        "settledReceiptCount": str(req["settled_receipt_count"]),
        "outstandingReceiptCount": str(req["outstanding_receipt_count"]),
        "postCutoverWriteCount": str(req["post_cutover_write_count"]),
        "debtWitnessDigest": str(req["debt_witness_digest"]),
        "noWriteWitnessDigest": str(req["no_write_witness_digest"]),
        "retirementWitnessDigest": str(req["retirement_witness_digest"]),
        "accountBinding": str(req["account_binding"]),
        "rvcBinding": str(req["rvc_binding"]),
        "receiptActive": receipt_active,
        "receiptKey": receipt_key,
        "receiptSettled": receipt_settled,
        "writeActive": write_active,
        "writeKey": write_key,
    }
