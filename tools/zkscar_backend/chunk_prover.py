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
    wasm, zkey, _ = ensure_chunk_artifacts(vk)

    if req.get("protocol_version") != vk.get("protocol_version"):
        json.dump({"ok": False, "error": "protocol mismatch"}, sys.stdout)
        return
    if req.get("circuit_version") != vk.get("circuit_version"):
        json.dump({"ok": False, "error": "circuit mismatch"}, sys.stdout)
        return

    siblings = [str(x) for x in req.get("siblings", [])]
    depth = int(vk.get("depth", 16))
    if len(siblings) > depth:
        json.dump({"ok": False, "error": "too many siblings for configured depth"}, sys.stdout)
        return
    siblings.extend(["0"] * (depth - len(siblings)))

    expected_public = [
        str(req["commitment"]),
        field_str(leaf_hash_field(req["hash"])),
        str(req["index"]),
        str(req["total"]),
    ]
    input_obj = {
        "root": expected_public[0],
        "leaf": expected_public[1],
        "index": expected_public[2],
        "total": expected_public[3],
        "siblings": siblings,
    }

    with tempfile.TemporaryDirectory(prefix="zkscar_chunk_") as td:
        td = Path(td)
        input_json = td / "input.json"
        proof_json = td / "proof.json"
        public_json = td / "public.json"
        input_json.write_text(json.dumps(input_obj), encoding="utf-8")

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
        public_vals = [str(x) for x in json.loads(public_json.read_text(encoding="utf-8"))]
        if public_vals != expected_public:
            json.dump({"ok": False, "error": "public input mismatch after proving"}, sys.stdout)
            return

        payload = {
            "proof": proof,
            "public": public_vals,
        }
        json.dump(
            {
                "ok": True,
                "proof_system": vk.get("proof_system", "groth16-bn128-strict"),
                "proof": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
            sys.stdout,
        )


if __name__ == "__main__":
    main()
