//! Runs every tutorial binary that the enabled features support.
//!
//! Single-process binaries run directly. MPI binaries are launched one at a
//! time through `mpiexec` (override with `HATAORI_MPIEXEC`) with
//! `HATAORI_MPI_RANKS` ranks (default 2) and are killed after
//! `HATAORI_TUTORIAL_TIMEOUT_SECS` (default 120). Binaries that are compiled
//! without their backend print `HATAORI_TUTORIAL_SKIP:` and exit successfully.

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const SKIP_MARKER: &str = "HATAORI_TUTORIAL_SKIP:";

/// MPI launches share the machine's cores and the launcher's session
/// directories; run them one at a time.
static MPI_LAUNCH: Mutex<()> = Mutex::new(());

fn timeout() -> Duration {
    let secs = std::env::var("HATAORI_TUTORIAL_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(120);
    Duration::from_secs(secs)
}

fn wait_with_timeout(name: &str, mut child: Child) -> (bool, String, String) {
    let deadline = Instant::now() + timeout();
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let reader = std::thread::spawn(move || {
        let mut out = String::new();
        let mut err = String::new();
        let _ = stdout.read_to_string(&mut out);
        let _ = stderr.read_to_string(&mut err);
        (out, err)
    });
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    let (out, err) = reader.join().expect("output reader");
    match status {
        Some(status) => (status.success(), out, err),
        None => (
            false,
            out,
            format!("{err}\n{name}: killed after {:?} timeout", timeout()),
        ),
    }
}

fn check(name: &str, mut command: Command) {
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let child = command
        .spawn()
        .unwrap_or_else(|err| panic!("failed to run tutorial binary {name}: {err}"));
    let (ok, stdout, stderr) = wait_with_timeout(name, child);
    assert!(
        ok,
        "tutorial binary {name} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
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

fn launcher_is_open_mpi(launcher: &str) -> bool {
    Command::new(launcher)
        .arg("--version")
        .output()
        .map(|output| {
            let text = String::from_utf8_lossy(&output.stdout).into_owned()
                + &String::from_utf8_lossy(&output.stderr);
            text.contains("Open MPI") || text.contains("OpenRTE")
        })
        .unwrap_or(false)
}

fn via_mpiexec(name: &str, path: &str) {
    if !mpi_enabled() {
        direct(name, path);
        return;
    }
    let _serial = MPI_LAUNCH
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let launcher = std::env::var("HATAORI_MPIEXEC").unwrap_or_else(|_| "mpiexec".to_owned());
    let ranks = std::env::var("HATAORI_MPI_RANKS").unwrap_or_else(|_| "2".to_owned());
    let mut command = Command::new(&launcher);
    if launcher_is_open_mpi(&launcher) {
        // Small CI machines: allow ranks x workers to exceed the core count,
        // and do not bind each rank to one core, which would make the
        // tutorials' managed Rayon domains fail their CPU-set validation.
        command.args(["--oversubscribe", "--bind-to", "none"]);
    }
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
