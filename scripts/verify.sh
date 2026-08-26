#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${ARTIFACT_DIR:-"$repo_dir/artifacts"}
mkdir -p "$artifact_dir"

unformatted=$(cd "$repo_dir" && gofmt -l .)
if [ -n "$unformatted" ]; then
  printf '%s\n' "$unformatted" >&2
  exit 1
fi

go -C "$repo_dir" vet ./...
go -C "$repo_dir" test ./... -count=1 -shuffle=on -coverprofile="$artifact_dir/coverage.out"
go -C "$repo_dir" test ./... -race -count=1

go -C "$repo_dir" tool cover -func="$artifact_dir/coverage.out" > "$artifact_dir/coverage.txt"
awk -v minimum="${MIN_COVERAGE:-90}" '
  $1 == "total:" {
    value=$3
    sub(/%$/, "", value)
    if (value + 0 < minimum + 0) {
      printf "total coverage %.1f%% is below %.1f%%\n", value, minimum > "/dev/stderr"
      exit 1
    }
    found=1
  }
  END { if (!found) exit 1 }
' "$artifact_dir/coverage.txt"
