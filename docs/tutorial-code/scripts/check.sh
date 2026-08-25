#!/usr/bin/env bash
# Builds and runs the tutorial binaries for every supported feature set.
#
# Usage: docs/tutorial-code/scripts/check.sh [ABSOLUTE_MPIWRAPPER_LIBRARY]
#
# Without an argument the `rsmpi-rt` lanes are skipped. `mpiexec` must be on
# PATH for the MPI lanes (override with HATAORI_MPIEXEC).
set -Eeuo pipefail

root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$root_dir"
mpi_rt_lib=${1:-}

run_lane() {
    local features=$1
    echo "== hataori-tutorial-code: features='${features:-none}'"
    if [[ -z $features ]]; then
        cargo test -p hataori-tutorial-code --no-default-features
    else
        cargo test -p hataori-tutorial-code --no-default-features --features "$features"
    fi
}

run_lane ''
run_lane rayon
run_lane mpi
run_lane mpi,rayon

if [[ -n $mpi_rt_lib ]]; then
    [[ $mpi_rt_lib = /* && -f $mpi_rt_lib ]] || {
        printf 'check.sh: MPIwrapper path must be an existing absolute file\n' >&2
        exit 2
    }
    export MPI_RT_LIB=$mpi_rt_lib
    run_lane rsmpi-rt
    run_lane rsmpi-rt,rayon
fi

echo "hataori tutorial code passed"
