#!/usr/bin/env python3
import hashlib
import json
import sys


def main():
    req = json.load(sys.stdin)
    payload = {
        "protocol_version": req.get("protocol_version"),
        "verifier_key_id": req.get("verifier_key_id"),
        "commitment": req.get("commitment"),
        "hash": req.get("hash"),
        "index": req.get("index"),
        "total": req.get("total"),
    }
    proof = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    json.dump({"ok": True, "proof_system": "external-chunk-proof", "proof": proof}, sys.stdout)


if __name__ == "__main__":
    main()
