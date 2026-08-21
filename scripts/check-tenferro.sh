#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

cargo fmt --all -- --check
cargo test -p hataori-tenferro --all-targets
cargo test -p hataori-tenferro --doc
cargo test -p hataori --no-default-features --features rayon --doc
cargo clippy -p hataori-tenferro --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc -p hataori-tenferro --no-deps

cargo +1.85.0 check -p hataori --no-default-features
cargo +1.85.0 check -p hataori --no-default-features --features rayon

for features in "" rayon; do
    tree=$(cargo tree -p hataori --no-default-features ${features:+--features "$features"})
    if grep -Eq '(^|[[:space:]])(hataori-tenferro|tenferro-(cpu|tensor)|tensor4all)([[:space:]]|$)' <<<"$tree"; then
        echo "check-tenferro: core tree unexpectedly contains adapter/tensor dependencies" >&2
        exit 1
    fi
done

metadata=$(mktemp)
trap 'rm -f "$metadata"' EXIT
cargo metadata --format-version 1 >"$metadata"
python3 - "$metadata" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    packages = json.load(stream)["packages"]

by_name = {package["name"]: package for package in packages}
for name in ("hataori-tenferro", "tenferro-cpu", "tenferro-tensor"):
    if name not in by_name:
        raise SystemExit(f"check-tenferro: missing package {name}")

pin = "a21a4c602fc6700b9bc0c3f1b14ebd19b9d7ec45"
for name in ("tenferro-cpu", "tenferro-tensor"):
    source = by_name[name].get("source") or ""
    if f"rev={pin}" not in source or not source.endswith(f"#{pin}"):
        raise SystemExit(f"check-tenferro: {name} is not pinned to {pin}: {source}")

for package in packages:
    if package["name"] == "tensor4all" or package["name"].startswith("tensor4all-"):
        raise SystemExit(
            f"check-tenferro: Phase 20a unexpectedly includes {package['name']}"
        )
PY

git diff --check
echo "Hataori Phase 20a tenferro adapter checks passed"
