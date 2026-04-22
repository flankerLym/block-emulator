#!/usr/bin/env python3
import base64
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from common import build_public_inputs_from_request, build_rvc_circuit_input, ensure_rvc_artifacts, load_vk


def main():
    req = json.load(sys.stdin)
    vk = load_vk(req.get("verifier_key_id", ""))
    wasm, zkey, _ = ensure_rvc_artifacts(vk)
    if req.get("protocol_version") != vk.get("protocol_version") or req.get("circuit_version") != vk.get("circuit_version"):
        json.dump({"ok": False, "error": "protocol/circuit version mismatch"}, sys.stdout)
        return
    circuit_input = build_rvc_circuit_input(req)
    with tempfile.TemporaryDirectory(prefix="zkscar_rvc_") as td:
        td = Path(td)
        input_json = td / "input.json"
        proof_json = td / "proof.json"
        public_json = td / "public.json"
        input_json.write_text(json.dumps(circuit_input), encoding="utf-8")
        cmd = [
            "snarkjs", "groth16", "fullprove",
            str(input_json),
            str(wasm),
            str(zkey),
            str(proof_json),
            str(public_json),
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.returncode != 0:
            json.dump({"ok": False, "error": proc.stderr or proc.stdout or "fullprove failed"}, sys.stdout)
            return
        proof = json.loads(proof_json.read_text(encoding="utf-8"))
        public_vals = json.loads(public_json.read_text(encoding="utf-8"))
        expected_public = build_public_inputs_from_request(req)
        if [str(x) for x in public_vals] != [str(x) for x in expected_public]:
            json.dump({"ok": False, "error": "public input mismatch after proving"}, sys.stdout)
            return
        payload = {
            "proof": proof,
            "public": [str(x) for x in public_vals],
        }
        proof_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        import hashlib
        proof_digest = hashlib.sha256(proof_bytes).hexdigest()
        json.dump({
            "ok": True,
            "proof_system": vk.get("proof_system", "groth16-bn128-strict"),
            "verifier_key_id": req.get("verifier_key_id"),
            "proof_bytes_b64": base64.b64encode(proof_bytes).decode("ascii"),
            "proof_digest": proof_digest,
            "proof_mode": "external-groth16-strict-v1",
        }, sys.stdout)


if __name__ == "__main__":
    main()
