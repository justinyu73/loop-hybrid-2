#!/usr/bin/env sh
set -eu

output="$(python3 gate-pack/ceremony_grader.py --root . --config gate-pack/.ceremony_grader.json gate)"
printf '%s\n' "$output"
python3 -c 'import json, sys; sys.exit(1 if json.loads(sys.argv[1])["violations"] else 0)' "$output"
