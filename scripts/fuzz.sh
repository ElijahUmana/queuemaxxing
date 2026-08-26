#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fuzz_time=${FUZZ_TIME:-30m}

run_fuzzers() {
  package=$1
  names=$(go -C "$repo_dir" test "$package" -list '^Fuzz' | awk '/^Fuzz/ { print $1 }')
  for name in $names; do
    go -C "$repo_dir" test "$package" -run '^$' -fuzz "^${name}$" -fuzztime "$fuzz_time"
  done
}

run_fuzzers ./internal/journal
run_fuzzers ./internal/api
run_fuzzers ./internal/engine
