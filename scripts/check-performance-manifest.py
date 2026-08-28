#!/usr/bin/env python3
"""Fail-closed structural checks for the frozen Phase A performance manifest."""

from __future__ import annotations

import hashlib
import pathlib
import sys
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
PATH = ROOT / "benchmarks" / "performance" / "manifest.toml"
BASELINE = "34cb1b1371c8b2f8ef750e2d49d10f9ef8f0782e"
REQUIRED_CASES = {
    "local-map",
    "rayon-map-in",
    "mpi-pmap",
    "hybrid-pmap",
    "mpi-collectives",
    "runtime-reuse",
    "segmented-transfer",
    "same-locality-fast-path",
    "object-placement",
    "object-concurrency",
    "runtime-cleanup",
}
REQUIRED_METADATA = {
    "cpu_model",
    "logical_cpus",
    "physical_cores",
    "memory_bytes",
    "rustc_version",
    "mpi_implementation",
    "network_path",
    "cpu_affinity",
    "thread_settings",
    "provider_settings",
    "governor",
    "candidate_commit",
    "benchmark_source_commit",
    "manifest_sha256",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def case(cases: dict[str, dict], name: str) -> dict:
    require(name in cases, f"missing case {name}")
    return cases[name]


def positive_values(entry: dict, key: str) -> None:
    values = entry.get(key, [])
    require(isinstance(values, list), f"{entry['id']}.{key} must be an array")
    require(all(isinstance(value, int) and value > 0 for value in values),
            f"{entry['id']}.{key} must contain positive integers")


def main() -> int:
    raw = PATH.read_bytes()
    data = tomllib.loads(raw.decode())
    require(data.get("schema_version") == 1, "schema_version must be 1")
    require(data.get("baseline_commit") == BASELINE, "baseline commit changed")
    require(data.get("manifest_status") == "frozen-before-candidate", "manifest is not frozen")

    experiment = data["experiment"]
    require(experiment["build_profile"] == "release", "measurements must use release")
    require(experiment["warmups"] >= 1, "warmups must be positive")
    require(experiment["repetitions"] >= 30, "at least 30 repetitions are required")
    require(experiment["calibration_pairs"] >= 30, "at least 30 calibration pairs are required")
    require(experiment["selective_retries"] is False, "selective retries are forbidden")
    require(experiment["outlier_removal"] is False, "outlier removal is forbidden")
    require(experiment["candidate_may_modify_manifest"] is False,
            "candidate must not modify the manifest")

    statistics = data["statistics"]
    require(statistics["confidence"] == 0.95, "confidence must remain 95%")
    require(statistics["ratio_transform"] == "natural-log", "paired ratios must use logs")
    require(0 < statistics["tolerance_cap"] <= 0.02, "tolerance cap exceeds 2%")
    require(statistics["invalid_or_noisy_classification"] == "INCONCLUSIVE",
            "invalid hosts must be INCONCLUSIVE")
    require(statistics["failed_gate_classification"] == "FAIL", "failed gates must be FAIL")

    acceptance = data["acceptance"]
    require(acceptance["primary_geometric_mean_point_estimate_max"] == 1.0,
            "primary geometric mean must not exceed 1.00")
    for key in (
        "correctness_required", "boundedness_required", "leak_free_shutdown_required"
    ):
        require(acceptance[key] is True, f"{key} must remain required")
    for key in (
        "same_locality_serializations_max", "same_locality_transport_messages_max",
        "sugar_extra_payload_copies_max", "sugar_extra_serializations_max",
        "sugar_extra_parcel_sequences_max", "sugar_extra_queue_hops_max",
        "sugar_extra_per_item_allocations_max", "sugar_extra_per_item_resolver_lookups_max",
    ):
        require(acceptance[key] == 0, f"{key} must remain zero")

    host = data["host"]
    require(REQUIRED_METADATA <= set(host["required_metadata"]), "host metadata is incomplete")
    require(host["max_load_per_logical_cpu"] <= 0.25, "host load gate was relaxed")
    require(host["swap_activity_allowed"] is False, "swap activity must invalidate a run")
    require(host["cpu_affinity_required"] is True, "CPU affinity must be recorded")
    require(host["thermal_throttling_allowed"] is False, "thermal throttling must invalidate")
    require(host["failed_or_missing_required_observation"] == "INCONCLUSIVE",
            "missing host evidence must be INCONCLUSIVE")

    for profile_name, profile in data["tcp_targets"].items():
        require(profile["max_p95_round_trip_latency_us"] > 0,
                f"{profile_name} latency target must be positive")
        require(profile["min_large_payload_throughput_mib_s"] > 0,
                f"{profile_name} throughput target must be positive")
        require(0 < profile["min_four_locality_scaling_efficiency"] <= 1,
                f"{profile_name} scaling target must be in (0, 1]")

    entries = data.get("case", [])
    require(entries, "case list is empty")
    cases = {entry["id"]: entry for entry in entries}
    require(len(cases) == len(entries), "case IDs must be unique")
    require(set(cases) == REQUIRED_CASES, "required case set changed")
    for entry in entries:
        require(entry.get("comparisons"), f"{entry['id']} lacks comparisons")
        require(entry.get("backends"), f"{entry['id']} lacks backends")
        require(entry.get("operations"), f"{entry['id']} lacks operations")
        require(entry.get("metrics"), f"{entry['id']} lacks metrics")
        for key in (
            "items", "payload_bytes", "work", "threads", "world_sizes",
            "batch_sizes", "invocations",
        ):
            if key in entry:
                positive_values(entry, key)

    local = case(cases, "local-map")
    require({16, 4096, 1048576} <= set(local["items"]), "local size coverage regressed")
    rayon = case(cases, "rayon-map-in")
    require({"sequential", "outer", "inner"} == set(rayon["local_modes"]),
            "Rayon mode coverage regressed")
    mpi = case(cases, "mpi-pmap")
    require({1, 2, 4, 8} <= set(mpi["world_sizes"]), "MPI world sizes regressed")
    require({1, 10} <= set(mpi["batch_sizes"]), "MPI batch sizes 1 and 10 are mandatory")
    require(1048576 in mpi["payload_bytes"], "MPI lacks a 1 MiB payload")
    hybrid = case(cases, "hybrid-pmap")
    require({False, True} == set(hybrid["prefetch"]), "hybrid prefetch matrix regressed")
    require({"sequential", "outer", "inner"} == set(hybrid["local_modes"]),
            "hybrid mode coverage regressed")
    collectives = case(cases, "mpi-collectives")
    require({"broadcast", "scatter", "gather"} == set(collectives["operations"]),
            "collective coverage regressed")
    require(1048576 in collectives["payload_bytes"], "collectives lack a 1 MiB payload")
    segmented = case(cases, "segmented-transfer")
    require(min(segmented["payload_bytes"]) >= 1048576, "segmented cases must start at 1 MiB")
    require("tcp" in segmented["backends"], "TCP segmented transfer missing")
    require("tcp-absolute" in segmented["comparisons"], "TCP absolute gate missing")
    fast = case(cases, "same-locality-fast-path")
    require({"payload_copies", "serializations", "transport_messages", "queue_hops"}
            <= set(fast["metrics"]), "same-locality instrumentation incomplete")
    placement = case(cases, "object-placement")
    require({"exact-placement", "preferred-placement", "resolver-cache-hit", "redirect"}
            <= set(placement["operations"]), "object placement coverage incomplete")
    concurrency = case(cases, "object-concurrency")
    require({"parallel-readers", "exclusive-writer", "saturated-affinity-queue"}
            <= set(concurrency["operations"]), "object concurrency coverage incomplete")
    cleanup = case(cases, "runtime-cleanup")
    require({"success", "error", "timeout", "cancellation", "peer-failure", "shutdown"}
            <= set(cleanup["operations"]), "cleanup coverage incomplete")

    digest = hashlib.sha256(raw).hexdigest()
    print(f"performance manifest valid: {len(entries)} cases, sha256={digest}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, TypeError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"performance manifest invalid: {error}", file=sys.stderr)
        raise SystemExit(1)
