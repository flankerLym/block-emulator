#!/usr/bin/env python3
import json
import sys


def main():
    req = json.load(sys.stdin)
    try:
        payload = json.loads(req.get("proof", ""))
    except Exception:
        json.dump({"ok": False, "valid": False, "error": "invalid proof payload"}, sys.stdout)
        return
    expected = {
        "protocol_version": req.get("protocol_version"),
        "verifier_key_id": req.get("verifier_key_id"),
        "commitment": req.get("commitment"),
        "hash": req.get("hash"),
        "index": req.get("index"),
        "total": req.get("total"),
    }
    valid = payload == expected and req.get("proof_system") == "external-chunk-proof"
    json.dump({"ok": True, "valid": valid}, sys.stdout)


if __name__ == "__main__":
    main()
