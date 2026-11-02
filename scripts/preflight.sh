#!/usr/bin/env sh
set -eu
missing=0
for cmd in docker kwokctl kubectl python3 make jq; do
  if command -v "$cmd" >/dev/null 2>&1; then printf 'ok       %s\n' "$cmd"
  else printf 'MISSING  %s\n' "$cmd"; missing=1; fi
done
docker info >/dev/null 2>&1 || { echo "MISSING  docker daemon not running"; missing=1; }
[ "$missing" -eq 0 ] && echo "All prerequisites found." || exit 1
