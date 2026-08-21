#!/usr/bin/env bash
set -Eeuo pipefail

if (($# != 1)); then
    printf 'usage: %s ABSOLUTE_MPIWRAPPER_LIBRARY\n' "$0" >&2
    exit 2
fi
mpi_rt_lib=$1
[[ $mpi_rt_lib = /* && -f $mpi_rt_lib ]] || {
    printf 'check-hybrid: MPIwrapper path must be an existing absolute file\n' >&2
    exit 2
}

root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT
cd "$root_dir"

export BINDGEN_EXTRA_CLANG_ARGS=${BINDGEN_EXTRA_CLANG_ARGS:-"-I$(gcc -print-file-name=include)"}
cargo build --example mpi_pmap_smoke --no-default-features --features mpi,rayon
cp target/debug/examples/mpi_pmap_smoke "$tmp_dir/upstream"
cargo build --example rsmpi_rt_pmap_smoke --no-default-features --features rsmpi-rt,rayon
cp target/debug/examples/rsmpi_rt_pmap_smoke "$tmp_dir/runtime"

launcher=${MPIEXEC:-mpiexec}
launcher_flags=()
if "$launcher" --version 2>&1 | grep -Eq 'Open MPI|OpenRTE' && ((EUID == 0)); then
    launcher_flags+=(--allow-run-as-root)
fi

run_case() {
    local backend=$1
    local count=$2
    local trace=$tmp_dir/trace-$backend-$count
    local binary=$tmp_dir/$backend
    local output=$tmp_dir/$backend-$count.log
    local -a environment=(env HATAORI_HYBRID_TRACE=$trace)
    if [[ $backend == runtime ]]; then
        environment+=(MPI_RT_LIB=$mpi_rt_lib)
    fi
    rm -f "$trace"
    if ! setsid timeout --signal=TERM --kill-after=5s 120s \
        "${environment[@]}" "$launcher" "${launcher_flags[@]}" -n "$count" "$binary" \
        >"$output" 2>&1; then
        cat "$output" >&2
        printf 'check-hybrid: %s backend failed at n=%s\n' "$backend" "$count" >&2
        return 1
    fi
    [[ ! -e $trace ]] || {
        printf 'check-hybrid: trace cleanup failed for %s n=%s\n' "$backend" "$count" >&2
        return 1
    }
}

for backend in upstream runtime; do
    for count in 1 2 4; do
        run_case "$backend" "$count"
    done

    binary=$tmp_dir/$backend
    output=$tmp_dir/$backend-panic.log
    environment=(env HATAORI_HYBRID_PANIC=1)
    if [[ $backend == runtime ]]; then
        environment+=(MPI_RT_LIB=$mpi_rt_lib)
    fi
    set +e
    setsid timeout --signal=TERM --kill-after=5s 60s \
        "${environment[@]}" "$launcher" "${launcher_flags[@]}" -n 2 "$binary" \
        >"$output" 2>&1
    status=$?
    set -e
    if ((status == 0 || status == 124 || status == 137)); then
        cat "$output" >&2
        printf 'check-hybrid: %s panic path did not abort promptly\n' "$backend" >&2
        exit 1
    fi
    grep -Eq 'MPI_ABORT|MPI_Abort|expected hybrid callback panic' "$output" || {
        cat "$output" >&2
        printf 'check-hybrid: %s panic path lacked abort evidence\n' "$backend" >&2
        exit 1
    }
done

printf 'hybrid checks passed for mpi and rsmpi-rt (n=1,2,4)\n'
