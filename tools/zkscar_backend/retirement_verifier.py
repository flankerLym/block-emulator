#!/usr/bin/env python3
import base64
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from common import build_retirement_public_inputs_from_request, ensure_retirement_artifacts, load_vk


def main():
    req = json.load(sys.stdin)
    vk = load_vk(req.get("verifier_key_id", ""))
    _, _, vkey = ensure_retirement_artifacts(vk)
    if req.get("protocol_version") != vk.get("protocol_version") or req.get("circuit_version") != vk.get("circuit_version"):
        json.dump({"ok": False, "valid": False, "error": "protocol/circuit version mismatch"}, sys.stdout)
        return
    try:
        proof_bytes = base64.b64decode(req.get("proof_bytes_b64", ""))
        payload = json.loads(proof_bytes.decode("utf-8"))
    except Exception:
        json.dump({"ok": False, "valid": False, "error": "invalid proof encoding"}, sys.stdout)
        return
    expected_digest = hashlib.sha256(proof_bytes).hexdigest()
    if expected_digest != req.get("proof_digest"):
        json.dump({"ok": True, "valid": False}, sys.stdout)
        return
    expected_public = [str(x) for x in build_retirement_public_inputs_from_request(req)]
    if [str(x) for x in payload.get("public", [])] != expected_public:
        json.dump({"ok": True, "valid": False}, sys.stdout)
        return
    with tempfile.TemporaryDirectory(prefix="zkscar_retirement_verify_") as td:
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
