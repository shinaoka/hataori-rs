#!/usr/bin/env python3
"""Fail-closed host preflight for the frozen Phase E performance experiment."""

import json
import os
import platform
import subprocess
from pathlib import Path


def text(command):
    return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


logical = os.cpu_count() or 0
load = os.getloadavg()[0]
try:
    physical = len({tuple(line.split(",")) for line in text(["lscpu", "-p=Core,Socket"]).splitlines()
                    if line and not line.startswith("#")})
except (OSError, subprocess.SubprocessError):
    physical = 0
governors = sorted(
    {value for path in Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor")
     if (value := read(path))}
)
reasons = []
if not logical:
    reasons.append("logical CPU count unavailable")
if not physical:
    reasons.append("physical core count unavailable")
for name in ("HATAORI_NETWORK_PATH", "HATAORI_THREAD_SETTINGS", "HATAORI_PROVIDER_SETTINGS"):
    if not os.environ.get(name):
        reasons.append(f"required observation {name} is unset")
if logical and load / logical > 0.25:
    reasons.append("load exceeds 0.25 per logical CPU")
if not governors:
    reasons.append("CPU governor observations unavailable")
elif governors != ["performance"]:
    reasons.append(f"CPU governor is not fixed performance: {','.join(governors)}")

record = {
    "classification": "INCONCLUSIVE" if reasons else "HOST_PREFLIGHT_PASS",
    "reasons": reasons,
    "hostname": platform.node(),
    "os": platform.system(),
    "kernel": platform.release(),
    "architecture": platform.machine(),
    "cpu_model": next((line.split(":", 1)[1].strip() for line in
                       (read("/proc/cpuinfo") or "").splitlines()
                       if line.startswith("model name")), None),
    "logical_cpus": logical,
    "physical_cores": physical,
    "memory_bytes": int((read("/proc/meminfo") or "MemTotal: 0 kB").splitlines()[0].split()[1]) * 1024,
    "rustc_version": text(["rustc", "--version"]),
    "cargo_version": text(["cargo", "--version"]),
    "mpi_implementation": text([os.environ.get("MPIEXEC", "mpiexec"), "--version"]).splitlines()[0],
    "mpi_version": text([os.environ.get("MPIEXEC", "mpiexec"), "--version"]).splitlines()[0],
    "network_path": os.environ.get("HATAORI_NETWORK_PATH"),
    "cpu_affinity": text(["taskset", "-pc", str(os.getpid())]),
    "thread_settings": os.environ.get("HATAORI_THREAD_SETTINGS"),
    "provider_settings": os.environ.get("HATAORI_PROVIDER_SETTINGS"),
    "governor": governors,
    "candidate_commit": text(["git", "rev-parse", "HEAD"]),
    "benchmark_source_commit": text(["git", "rev-parse", "HEAD"]),
    "manifest_sha256": text(["sha256sum", "benchmarks/performance/manifest.toml"]).split()[0],
    "load_1m": load,
}
print(json.dumps(record, sort_keys=True, indent=2))
raise SystemExit(1 if reasons else 0)
