#!/usr/bin/env python3
import base64
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VK_DIR = ROOT / "verifier_keys"
DEFAULT_VK = "zkscar-vk-v3"


def load_vk(vk_id: str):
    path = VK_DIR / f"{vk_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    req = json.load(sys.stdin)
    vk_id = req.get("verifier_key_id") or DEFAULT_VK
    vk = load_vk(vk_id)
    if vk is None:
        json.dump({"ok": False, "error": f"unknown verifier key: {vk_id}"}, sys.stdout)
        return
    if req.get("protocol_version") != vk.get("protocol_version") or req.get("circuit_version") != vk.get("circuit_version"):
        json.dump({"ok": False, "error": "protocol/circuit version mismatch", "valid": False}, sys.stdout)
        return
    try:
        proof_bytes = base64.b64decode(req.get("proof_bytes_b64", ""))
        payload = json.loads(proof_bytes.decode("utf-8"))
    except Exception:
        json.dump({"ok": False, "error": "invalid proof encoding", "valid": False}, sys.stdout)
        return
    expected_payload = {
        "protocol_version": req.get("protocol_version"),
        "circuit_version": req.get("circuit_version"),
        "verifier_key_id": vk_id,
        "witness_bundle_hash": req.get("witness_bundle_hash", ""),
        "public_inputs": req.get("public_inputs", []),
        "witness_bundle_b64": payload.get("witness_bundle_b64", ""),
    }
    expected_bytes = json.dumps(expected_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    expected_digest = hashlib.sha256(expected_bytes).hexdigest()
    valid = (
        payload == expected_payload
        and req.get("proof_digest") == expected_digest
        and req.get("proof_system") == vk.get("proof_system", "external-witness-proof")
    )
    json.dump({"ok": True, "valid": valid}, sys.stdout)


if __name__ == "__main__":
    main()
