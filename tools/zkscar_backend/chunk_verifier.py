#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from common import ensure_chunk_artifacts, field_str, leaf_hash_field, load_vk


def main():
    req = json.load(sys.stdin)
    vk = load_vk(req.get("verifier_key_id", ""))
    _, _, vkey = ensure_chunk_artifacts(vk)

    if req.get("protocol_version") != vk.get("protocol_version"):
        json.dump({"ok": False, "valid": False, "error": "protocol mismatch"}, sys.stdout)
        return
    if req.get("circuit_version") != vk.get("circuit_version"):
        json.dump({"ok": False, "valid": False, "error": "circuit mismatch"}, sys.stdout)
        return
    if req.get("proof_system") != vk.get("proof_system", "groth16-bn128-strict"):
        json.dump({"ok": True, "valid": False}, sys.stdout)
        return

    try:
        payload = json.loads(req.get("proof", ""))
    except Exception:
        json.dump({"ok": False, "valid": False, "error": "invalid proof payload"}, sys.stdout)
        return

    expected_public = [
        str(req["commitment"]),
        field_str(leaf_hash_field(req["hash"])),
        str(req["index"]),
        str(req["total"]),
    ]
    if [str(x) for x in payload.get("public", [])] != expected_public:
        json.dump({"ok": True, "valid": False}, sys.stdout)
        return

    with tempfile.TemporaryDirectory(prefix="zkscar_chunk_verify_") as td:
        td = Path(td)
        proof_json = td / "proof.json"
        public_json = td / "public.json"
        proof_json.write_text(json.dumps(payload["proof"]), encoding="utf-8")
        public_json.write_text(json.dumps(payload["public"]), encoding="utf-8")

        cmd = ["snarkjs", "groth16", "verify", str(vkey), str(public_json), str(proof_json)]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        valid = proc.returncode == 0 and ("OK" in (proc.stdout or "") or "OK" in (proc.stderr or ""))
        json.dump({"ok": True, "valid": valid}, sys.stdout)


if __name__ == "__main__":
    main()
