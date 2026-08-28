#!/usr/bin/env bash
# Builds the Hataori documentation site into target/docs-site.
#
#   1. verify that every published Rust snippet matches examples/
#   2. validate docs/llms.txt (every entry resolves, README links it)
#   3. build rustdoc for the core and runtime crates
#   4. render the Quarto site and copy rustdoc under <site>/api/
#   5. verify that the rendered site publishes llms.txt at its root
#
# Usage: scripts/build_docs_site.sh [OUTPUT_DIR]
set -Eeuo pipefail

root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out_dir=${1:-$root_dir/target/docs-site}
doc_root=$root_dir/target/doc
cd "$root_dir"
export BINDGEN_EXTRA_CLANG_ARGS=${BINDGEN_EXTRA_CLANG_ARGS:-"-I$(gcc -print-file-name=include)"}

echo "[1/6] Checking doc snippets"
python3 scripts/check-doc-snippets.py --root-dir "$root_dir" --check

echo "[2/6] Checking llms.txt index"
python3 scripts/check-llms-index.py --root-dir "$root_dir"

echo "[3/6] Building rustdoc"
rm -rf "$doc_root"
# `mpi` and `rsmpi-rt` are mutually exclusive; the `mpi` backend documents the
# shared MPI/hybrid API surface. Feature-gated items are labelled with
# `--cfg docsrs` so readers see which feature enables them.
RUSTDOCFLAGS="--cfg docsrs" cargo doc --no-deps --no-default-features --features mpi,rayon
RUSTDOCFLAGS="--cfg docsrs" cargo doc -p hataori-runtime --no-deps --no-default-features --features mpi

echo "[4/6] Rendering Quarto site"
rm -rf "$out_dir"
if ! command -v quarto >/dev/null 2>&1; then
    echo "quarto is required to render docs/ (https://quarto.org)" >&2
    exit 1
fi
quarto render "$root_dir/docs" --output-dir "$out_dir"

echo "[5/6] Copying rustdoc into the site"
mkdir -p "$out_dir/api"
cp -a "$doc_root/." "$out_dir/api/"
[[ -f $out_dir/api/hataori/index.html ]]
[[ -f $out_dir/api/hataori_runtime/index.html ]]
touch "$out_dir/.nojekyll"

echo "[6/6] Verifying llms.txt in the rendered site"
# Quarto copies docs/llms.txt as a project resource; fail loudly if it did not.
python3 scripts/check-llms-index.py --root-dir "$root_dir" --docs-site-root "$out_dir" --quiet
[[ -f $out_dir/llms.txt ]]

echo "Done: $out_dir"
