#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${ARTIFACT_DIR:-"$repo_dir/artifacts"}
minimum=${MIN_COVERAGE:-90}
profile="$artifact_dir/coverage.out"
mkdir -p "$artifact_dir"

unformatted=$(cd "$repo_dir" && gofmt -l .)
if [ -n "$unformatted" ]; then
  printf '%s\n' "$unformatted" >&2
  exit 1
fi

go -C "$repo_dir" vet ./...
go -C "$repo_dir" test -p=1 ./... \
  -count=1 -shuffle=on \
  -coverpkg=./api,./client,./cmd/qmax,./cmd/qmax-workbench,./internal/clock,./internal/engine,./internal/journal \
  -coverprofile="$profile"
go -C "$repo_dir" test ./... -race -count=1

python3 - "$profile" "$minimum" "$artifact_dir/coverage.txt" <<'PY'
from pathlib import Path
import sys

profile = Path(sys.argv[1])
minimum = float(sys.argv[2])
report = Path(sys.argv[3])
seen = {}
for line in profile.read_text().splitlines()[1:]:
    key, statements, count = line.rsplit(" ", 2)
    statements, count = int(statements), int(count)
    previous = seen.get(key)
    seen[key] = (statements, max(count, previous[1] if previous else 0))
covered = sum(statements for statements, count in seen.values() if count)
total = sum(statements for statements, _ in seen.values())
percentage = covered / total * 100 if total else 100.0
text = f"combined production statement coverage: {percentage:.3f}% ({covered}/{total})\n"
report.write_text(text)
print(text, end="")
if covered * 100 < total * minimum:
    raise SystemExit(f"coverage {percentage:.3f}% is below {minimum:.3f}%")
PY
