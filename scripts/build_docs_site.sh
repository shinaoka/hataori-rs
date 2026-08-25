#!/usr/bin/env bash
# Builds the Hataori documentation site into target/docs-site.
#
#   1. verify that every published Rust snippet matches examples/
#   2. build rustdoc for the core crate with every backend documented
#   3. render the Quarto site and copy rustdoc under <site>/api/
#
# Usage: scripts/build_docs_site.sh [OUTPUT_DIR]
set -Eeuo pipefail

root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out_dir=${1:-$root_dir/target/docs-site}
doc_root=$root_dir/target/doc
cd "$root_dir"

echo "[1/4] Checking doc snippets"
python3 scripts/check-doc-snippets.py --root-dir "$root_dir" --check

echo "[2/4] Building rustdoc"
rm -rf "$doc_root"
# `mpi` and `rsmpi-rt` are mutually exclusive; the `mpi` backend documents the
# shared MPI/hybrid API surface. Feature-gated items are labelled with
# `--cfg docsrs` so readers see which feature enables them.
RUSTDOCFLAGS="--cfg docsrs" cargo doc --no-deps --no-default-features --features mpi,rayon

echo "[3/4] Rendering Quarto site"
rm -rf "$out_dir"
if ! command -v quarto >/dev/null 2>&1; then
    echo "quarto is required to render docs/ (https://quarto.org)" >&2
    exit 1
fi
quarto render "$root_dir/docs" --output-dir "$out_dir"

echo "[4/4] Copying rustdoc into the site"
mkdir -p "$out_dir/api"
cp -a "$doc_root/." "$out_dir/api/"
[[ -f $out_dir/api/hataori/index.html ]]
touch "$out_dir/.nojekyll"

echo "Done: $out_dir"
