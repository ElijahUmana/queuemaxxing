#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fuzz_time=${FUZZ_TIME:-30m}
found=0

for package in $(go -C "$repo_dir" list ./...); do
  names=$(go -C "$repo_dir" test "$package" -list '^Fuzz' | awk '/^Fuzz/ { print $1 }')
  for name in $names; do
    found=$((found + 1))
    go -C "$repo_dir" test "$package" -run '^$' -fuzz "^${name}$" -fuzztime "$fuzz_time"
  done
done

if [ "$found" -eq 0 ]; then
  printf '%s\n' "no fuzz targets were discovered" >&2
  exit 1
fi
