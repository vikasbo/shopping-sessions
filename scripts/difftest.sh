#!/bin/sh
# Differential test: Rust pipeline vs the Python reference on fresh generator seeds.
# Usage: scripts/difftest.sh EVENTS CLICKS SEED...   (needs `generate` on PATH or $GENERATE)
set -eu
GEN="${GENERATE:-generate}"
BIN="${BIN:-target/release/shopping-sessions}"
EVENTS="$1"; CLICKS="$2"; shift 2
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
fail=0
for seed in "$@"; do
  "$GEN" --seed "$seed" --events "$EVENTS" --clicks "$CLICKS" --output "$TMP/data" >/dev/null
  "$BIN" "$TMP/data" "$TMP/rs"
  python3 reference/reference.py "$TMP/data" "$TMP/py"
  if python3 scripts/compare.py "$TMP/rs" "$TMP/py" >"$TMP/log"; then
    echo "seed $seed: MATCH ($(grep -o '"window_start": "[^"]*' "$TMP/data/manifest.json" | cut -d'"' -f4))"
  else
    echo "seed $seed: DIFFER"; cat "$TMP/log"; fail=1
  fi
  rm -rf "$TMP/data" "$TMP/rs" "$TMP/py"
done
exit $fail
