#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
base_url=${QMAX_TEST_URL:?QMAX_TEST_URL must point to the real qmax API}

QMAX_TEST_URL="$base_url" go -C "$repo_dir" test ./test/contract -count=1 -v

if command -v k6 >/dev/null 2>&1; then
  QMAX_TEST_URL="$base_url" k6 run "$repo_dir/test/load/soak.js"
else
  printf '%s\n' "k6 is required for load verification" >&2
  exit 1
fi
