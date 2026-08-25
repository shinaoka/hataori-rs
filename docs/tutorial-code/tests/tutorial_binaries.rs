//! Runs every tutorial binary that the enabled features support.
//!
//! Single-process binaries run directly. MPI binaries are launched through
//! `mpiexec` (override with `HATAORI_MPIEXEC`) with `HATAORI_MPI_RANKS`
//! ranks (default 2). Binaries that are compiled without their backend print
//! `HATAORI_TUTORIAL_SKIP:` and exit successfully.

use std::process::Command;

const SKIP_MARKER: &str = "HATAORI_TUTORIAL_SKIP:";

fn check(name: &str, mut command: Command) {
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("failed to run tutorial binary {name}: {err}"));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "tutorial binary {name} failed\nstatus: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    if let Some(skip) = stdout.lines().find(|line| line.starts_with(SKIP_MARKER)) {
        eprintln!("{name}: {skip}");
    }
}

fn direct(name: &str, path: &str) {
    check(name, Command::new(path));
}

fn mpi_enabled() -> bool {
    cfg!(any(feature = "mpi", feature = "rsmpi-rt"))
}

fn via_mpiexec(name: &str, path: &str) {
    if !mpi_enabled() {
        direct(name, path);
        return;
    }
    let launcher = std::env::var("HATAORI_MPIEXEC").unwrap_or_else(|_| "mpiexec".to_owned());
    let ranks = std::env::var("HATAORI_MPI_RANKS").unwrap_or_else(|_| "2".to_owned());
    let mut command = Command::new(launcher);
    command.arg("-n").arg(ranks).arg(path);
    check(name, command);
}

#[test]
fn serial_map_runs() {
    direct("serial_map", env!("CARGO_BIN_EXE_serial_map"));
}

#[cfg(feature = "rayon")]
#[test]
fn rayon_map_in_runs() {
    direct("rayon_map_in", env!("CARGO_BIN_EXE_rayon_map_in"));
}

#[test]
fn mpi_pmap_runs() {
    via_mpiexec("mpi_pmap", env!("CARGO_BIN_EXE_mpi_pmap"));
}

#[test]
fn mpi_placement_runs() {
    via_mpiexec("mpi_placement", env!("CARGO_BIN_EXE_mpi_placement"));
}

#[test]
fn hybrid_pmap_runs() {
    via_mpiexec("hybrid_pmap", env!("CARGO_BIN_EXE_hybrid_pmap"));
}
