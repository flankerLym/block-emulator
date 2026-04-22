#!/usr/bin/env python3
import json
import sys

def main():
    _ = json.load(sys.stdin)
    json.dump({"ok": True, "valid": False}, sys.stdout)

if __name__ == "__main__":
    main()
