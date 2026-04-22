#!/usr/bin/env python3
import json
import sys

def main():
    req = json.load(sys.stdin)
    json.dump({
        "ok": True,
        "proof_system": "groth16-bn128-strict",
        "verifier_key_id": req.get("verifier_key_id", "zkscar-retirement-groth16-v1"),
        "proof_bytes_b64": "",
        "proof_digest": "",
        "proof_mode": "external-groth16-retirement-v1"
    }, sys.stdout)

if __name__ == "__main__":
    main()
