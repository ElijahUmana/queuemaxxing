#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${ARTIFACT_DIR:-"$repo_dir/artifacts"}
mkdir -p "$artifact_dir"

new_results="$artifact_dir/bench-new.txt"
go -C "$repo_dir" test ./... -run '^$' -bench . -benchmem -count 10 > "$new_results"

if [ -n "${BENCH_BASELINE:-}" ]; then
  command -v benchstat >/dev/null 2>&1 || {
    printf '%s\n' "benchstat is required when BENCH_BASELINE is set" >&2
    exit 1
  }
  benchstat "$BENCH_BASELINE" "$new_results" | tee "$artifact_dir/benchstat.txt"
fi
