from __future__ import annotations

import copy
import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from socket import gethostname
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from ray.data.benchmarks import cbo_benchmark_harness as benchmark_harness
    from ray.data.benchmarks.cbo_benchmark_schema import (
        build_end_to_end_payload,
        build_memory_profile_payload,
        build_per_rule_payload,
        build_r_value_matrix_payload,
        validate_payload,
    )
except ImportError:
    import cbo_benchmark_harness as benchmark_harness  # type: ignore
    from cbo_benchmark_schema import (  # type: ignore
        build_end_to_end_payload,
        build_memory_profile_payload,
        build_per_rule_payload,
        build_r_value_matrix_payload,
        validate_payload,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}, got {type(payload)!r}")
    return payload


def load_payload_files(paths: Sequence[str]) -> List[Dict[str, Any]]:
    return [load_json_file(path) for path in paths]


def load_manifest_file(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    return load_json_file(path)


def load_environment_metadata_file(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    return load_json_file(path)


def build_environment_metadata_template() -> Dict[str, Any]:
    return {
        "benchmark_date": "",
        "ray_version": "",
        "python_version": "",
        "git_sha": "",
        "git_branch": "",
        "platform": "",
        "cluster_name": "",
        "hardware": "",
        "command": "",
        "owner": "",
        "notes": "",
    }


def _run_git_command(args: Sequence[str]) -> Optional[str]:
    try:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def build_environment_metadata(
    user_metadata: Optional[Dict[str, Any]] = None,
    input_result_paths: Optional[Sequence[str]] = None,
    manifest_path: Optional[str] = None,
    output_path: Optional[str] = None,
    report_markdown_path: Optional[str] = None,
    bundle_dir: Optional[str] = None,
) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "generated_at": _utc_now(),
        "benchmark_date": datetime.now(timezone.utc).date().isoformat(),
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "hostname": gethostname(),
        "cwd": os.getcwd(),
        "git_sha": _run_git_command(["rev-parse", "HEAD"]),
        "git_branch": _run_git_command(["rev-parse", "--abbrev-ref", "HEAD"]),
        "input_result_paths": list(input_result_paths or []),
        "manifest_path": manifest_path,
        "output_path": output_path,
        "report_markdown_path": report_markdown_path,
        "bundle_dir": bundle_dir,
    }
    if user_metadata:
        metadata.update(user_metadata)
    return metadata


def _input_section_key(experiment_type: str) -> str:
    mapping = {
        "per_rule": "results",
        "end_to_end": "runs",
        "r_value_matrix": "results",
        "memory_profile": "runs",
    }
    if experiment_type not in mapping:
        raise ValueError(f"Unsupported experiment_type: {experiment_type}")
    return mapping[experiment_type]


def _ignored_config_keys(experiment_type: str) -> set[str]:
    ignored = {"subtask_index"}
    if experiment_type == "r_value_matrix":
        ignored.add("selected_workload_ids")
    if experiment_type == "memory_profile":
        ignored.add("selected_run_ids")
    return ignored


def _subtask_info(payload: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    config = payload.get("config") or {}
    subtask_index = config.get("subtask_index")
    subtask_count = config.get("subtask_count")
    if subtask_index is not None or subtask_count is not None:
        return subtask_index, subtask_count

    for record in payload.get("records", []):
        metadata = record.get("metadata") or {}
        if (
            metadata.get("subtask_index") is not None
            or metadata.get("subtask_count") is not None
        ):
            return metadata.get("subtask_index"), metadata.get("subtask_count")
    return None, None


def _selected_item_ids(payload: Dict[str, Any]) -> List[str]:
    experiment_type = payload["experiment_type"]
    config = payload.get("config") or {}
    if experiment_type == "r_value_matrix":
        item_ids = config.get("selected_workload_ids")
        if item_ids:
            return [str(item_id) for item_id in item_ids]
        return [
            str(result["workload_id"])
            for result in payload.get("results", [])
            if result.get("workload_id")
        ]
    if experiment_type == "memory_profile":
        item_ids = config.get("selected_run_ids")
        if item_ids:
            return [str(item_id) for item_id in item_ids]
        inferred: List[str] = []
        for run in payload.get("runs", []):
            metadata = run.get("metadata") or {}
            run_id = metadata.get("run_id")
            if run_id:
                inferred.append(str(run_id))
        return inferred
    if experiment_type == "per_rule":
        return [
            str(result["rule_name"])
            for result in payload.get("results", [])
            if result.get("rule_name")
        ]
    return [
        str(run["workload_id"])
        for run in payload.get("runs", [])
        if run.get("workload_id")
    ]


def _compare_configs(
    payloads: Sequence[Dict[str, Any]],
    experiment_type: str,
) -> None:
    ignored = _ignored_config_keys(experiment_type)
    baseline = {
        key: value
        for key, value in (payloads[0].get("config") or {}).items()
        if key not in ignored
    }
    for idx, payload in enumerate(payloads[1:], start=1):
        current = {
            key: value
            for key, value in (payload.get("config") or {}).items()
            if key not in ignored
        }
        if current != baseline:
            raise ValueError(
                "Incompatible benchmark configs across result files: "
                f"file[0]={baseline!r}, file[{idx}]={current!r}"
            )


def _validate_inputs(payloads: Sequence[Dict[str, Any]]) -> str:
    if not payloads:
        raise ValueError("At least one benchmark payload is required for merge")

    experiment_type = payloads[0].get("experiment_type")
    schema_version = payloads[0].get("schema_version")
    benchmark_name = payloads[0].get("benchmark_name")

    for idx, payload in enumerate(payloads):
        errors = validate_payload(payload)
        if errors:
            raise ValueError(
                f"Input payload #{idx} failed schema validation: {'; '.join(errors)}"
            )
        if payload.get("experiment_type") != experiment_type:
            raise ValueError("All payloads must share the same experiment_type")
        if payload.get("schema_version") != schema_version:
            raise ValueError("All payloads must share the same schema_version")
        if payload.get("benchmark_name") != benchmark_name:
            raise ValueError("All payloads must share the same benchmark_name")

    _compare_configs(payloads, str(experiment_type))
    return str(experiment_type)


def _merge_config(
    payloads: Sequence[Dict[str, Any]],
    experiment_type: str,
    coverage: Dict[str, Any],
) -> Dict[str, Any]:
    merged_config = dict(payloads[0].get("config") or {})
    merged_config.pop("subtask_index", None)
    if experiment_type == "r_value_matrix":
        merged_ids: List[str] = []
        for payload in payloads:
            merged_ids.extend(_selected_item_ids(payload))
        merged_config["selected_workload_ids"] = sorted(set(merged_ids))
    elif experiment_type == "memory_profile":
        merged_ids = []
        for payload in payloads:
            merged_ids.extend(_selected_item_ids(payload))
        merged_config["selected_run_ids"] = sorted(set(merged_ids))
    if coverage.get("observed_subtask_count") is not None:
        merged_config["subtask_count"] = coverage["observed_subtask_count"]
    return merged_config


def _entry_identity(experiment_type: str, entry: Dict[str, Any]) -> str:
    if experiment_type == "per_rule":
        return str(entry.get("rule_name"))
    if experiment_type == "end_to_end":
        return "|".join(
            [
                str(entry.get("mode")),
                str(entry.get("enable_cbo")),
                str(entry.get("workload_id")),
                str(entry.get("reservation_ratio")),
            ]
        )
    if experiment_type == "r_value_matrix":
        return str(entry.get("workload_id"))
    metadata = entry.get("metadata") or {}
    run_id = metadata.get("run_id")
    if run_id:
        return str(run_id)
    return "|".join(
        [
            str(entry.get("workload_id")),
            str(metadata.get("repetition")),
            str(entry.get("enable_cbo")),
        ]
    )


def _sort_entries(
    experiment_type: str, entries: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if experiment_type == "per_rule":
        return sorted(entries, key=lambda entry: str(entry.get("rule_name", "")))
    if experiment_type == "end_to_end":
        return sorted(
            entries,
            key=lambda entry: (
                str(entry.get("mode", "")),
                str(entry.get("workload_id", "")),
                bool(entry.get("enable_cbo")),
            ),
        )
    if experiment_type == "r_value_matrix":
        return sorted(
            entries,
            key=lambda entry: (
                int(entry.get("num_rows") or 0),
                int(entry.get("transform_ops") or 0),
                str(entry.get("workload_id", "")),
            ),
        )
    return sorted(
        entries,
        key=lambda entry: (
            int(entry.get("total_input_rows") or 0),
            int((entry.get("metadata") or {}).get("transform_ops") or 0),
            int((entry.get("metadata") or {}).get("repetition") or 0),
            bool(entry.get("enable_cbo")),
            str((entry.get("metadata") or {}).get("run_id") or ""),
        ),
    )


def _collect_entries(
    payloads: Sequence[Dict[str, Any]],
    experiment_type: str,
) -> List[Dict[str, Any]]:
    section_key = _input_section_key(experiment_type)
    merged_entries: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for payload_idx, payload in enumerate(payloads):
        for entry in payload.get(section_key, []):
            copied = copy.deepcopy(entry)
            identity = _entry_identity(experiment_type, copied)
            if identity in seen:
                raise ValueError(
                    f"Duplicate benchmark entry detected for identity {identity!r} "
                    f"in payload #{payload_idx}"
                )
            seen[identity] = payload_idx
            merged_entries.append(copied)
    return _sort_entries(experiment_type, merged_entries)


def summarize_shard_coverage(
    payloads: Sequence[Dict[str, Any]],
    manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not payloads:
        return {
            "observed_payloads": 0,
            "observed_subtask_indices": [],
            "observed_subtask_count": None,
            "missing_subtask_indices": [],
            "duplicate_subtask_indices": [],
            "unexpected_subtask_indices": [],
            "mismatched_manifest_assignments": [],
            "is_complete": False,
        }

    experiment_type = str(payloads[0]["experiment_type"])
    manifest_entries = []
    if manifest is not None:
        raw_entries = manifest.get(experiment_type) or []
        if not isinstance(raw_entries, list):
            raise ValueError(
                f"Manifest section {experiment_type!r} must be a list, "
                f"got {type(raw_entries)!r}"
            )
        manifest_entries = raw_entries

    observed_map: Dict[int, Dict[str, Any]] = {}
    duplicate_indices: List[int] = []
    observed_counts: set[int] = set()

    for payload in payloads:
        subtask_index, subtask_count = _subtask_info(payload)
        if subtask_count is not None:
            observed_counts.add(int(subtask_count))
        if subtask_index is None:
            continue
        subtask_index = int(subtask_index)
        if subtask_index in observed_map:
            duplicate_indices.append(subtask_index)
        observed_map[subtask_index] = {
            "item_ids": sorted(_selected_item_ids(payload)),
            "generated_at": payload.get("generated_at"),
        }

    if len(observed_counts) > 1:
        raise ValueError(
            f"Inconsistent subtask_count across payloads: {sorted(observed_counts)}"
        )

    observed_subtask_count = next(iter(observed_counts), None)
    observed_indices = sorted(observed_map)

    if manifest_entries:
        expected_indices = sorted(
            int(entry["subtask_index"]) for entry in manifest_entries
        )
        expected_items = {
            int(entry["subtask_index"]): sorted(str(item) for item in entry["item_ids"])
            for entry in manifest_entries
        }
    elif observed_subtask_count is not None:
        expected_indices = list(range(int(observed_subtask_count)))
        expected_items = {}
    else:
        expected_indices = []
        expected_items = {}

    missing_indices = sorted(set(expected_indices) - set(observed_indices))
    unexpected_indices = sorted(set(observed_indices) - set(expected_indices))
    mismatches: List[Dict[str, Any]] = []
    for subtask_index, expected_item_ids in expected_items.items():
        observed_item_ids = observed_map.get(subtask_index, {}).get("item_ids")
        if observed_item_ids is None:
            continue
        if observed_item_ids != expected_item_ids:
            mismatches.append(
                {
                    "subtask_index": subtask_index,
                    "expected_item_ids": expected_item_ids,
                    "observed_item_ids": observed_item_ids,
                }
            )

    return {
        "observed_payloads": len(payloads),
        "observed_subtask_indices": observed_indices,
        "observed_subtask_count": observed_subtask_count,
        "missing_subtask_indices": missing_indices,
        "duplicate_subtask_indices": sorted(set(duplicate_indices)),
        "unexpected_subtask_indices": unexpected_indices,
        "mismatched_manifest_assignments": mismatches,
        "is_complete": (
            not missing_indices
            and not duplicate_indices
            and not unexpected_indices
            and not mismatches
        ),
    }


def build_runtime_feedback_summary(
    payload: Dict[str, Any],
    alpha: float = 0.7,
    tolerance: float = 0.02,
    max_iterations: int = 5,
) -> Optional[Dict[str, Any]]:
    if payload.get("experiment_type") != "r_value_matrix":
        return None
    if not 0.0 < alpha <= 1.0:
        raise ValueError("alpha must be within (0.0, 1.0]")
    if tolerance < 0.0:
        raise ValueError("tolerance must be non-negative")
    if max_iterations < 0:
        raise ValueError("max_iterations must be non-negative")

    workloads: List[Dict[str, Any]] = []
    initial_errors: List[float] = []
    final_errors: List[float] = []

    for result in payload.get("results", []):
        best_ratio = float(result["best_ratio"])
        default_ratio = float(result.get("default_ratio", 0.5))
        derived_ratio = float(result["derived_ratio"])
        current_ratio = derived_ratio
        history = [
            {
                "iteration": 0,
                "ratio": round(current_ratio, 4),
                "abs_error": round(abs(current_ratio - best_ratio), 4),
            }
        ]
        iterations_to_converge = 0
        converged = abs(current_ratio - best_ratio) <= tolerance

        while not converged and iterations_to_converge < max_iterations:
            iterations_to_converge += 1
            current_ratio = alpha * best_ratio + (1.0 - alpha) * current_ratio
            abs_error = abs(current_ratio - best_ratio)
            history.append(
                {
                    "iteration": iterations_to_converge,
                    "ratio": round(current_ratio, 4),
                    "abs_error": round(abs_error, 4),
                }
            )
            converged = abs_error <= tolerance

        initial_error = abs(derived_ratio - best_ratio)
        final_error = abs(current_ratio - best_ratio)
        initial_errors.append(initial_error)
        final_errors.append(final_error)
        workloads.append(
            {
                "workload_id": result["workload_id"],
                "num_rows": result.get("num_rows"),
                "transform_ops": result.get("transform_ops"),
                "default_ratio": round(default_ratio, 4),
                "derived_ratio": round(derived_ratio, 4),
                "best_ratio": round(best_ratio, 4),
                "default_error": round(abs(default_ratio - best_ratio), 4),
                "initial_error": round(initial_error, 4),
                "final_ratio": round(current_ratio, 4),
                "final_error": round(final_error, 4),
                "iterations_to_converge": iterations_to_converge,
                "converged": converged,
                "history": history,
            }
        )

    if not workloads:
        return None

    return {
        "generated_at": _utc_now(),
        "alpha": alpha,
        "tolerance": tolerance,
        "max_iterations": max_iterations,
        "converged_workloads": sum(1 for item in workloads if item["converged"]),
        "total_workloads": len(workloads),
        "mean_initial_error": round(statistics.mean(initial_errors), 4),
        "mean_final_error": round(statistics.mean(final_errors), 4),
        "workloads": workloads,
    }


def merge_benchmark_payloads(
    payloads: Sequence[Dict[str, Any]],
    manifest: Optional[Dict[str, Any]] = None,
    runtime_feedback_alpha: float = 0.7,
    runtime_feedback_tolerance: float = 0.02,
    runtime_feedback_max_iterations: int = 5,
) -> Dict[str, Any]:
    experiment_type = _validate_inputs(payloads)
    coverage = summarize_shard_coverage(payloads, manifest=manifest)
    merged_config = _merge_config(payloads, experiment_type, coverage)
    merged_entries = _collect_entries(payloads, experiment_type)

    if experiment_type == "per_rule":
        merged_payload = build_per_rule_payload(merged_entries, merged_config)
    elif experiment_type == "end_to_end":
        runs = [benchmark_harness.RunMetrics(**entry) for entry in merged_entries]
        aggregates = benchmark_harness.aggregate_runs(runs)
        merged_payload = build_end_to_end_payload(
            merged_entries, aggregates, merged_config
        )
    elif experiment_type == "r_value_matrix":
        merged_payload = build_r_value_matrix_payload(merged_entries, merged_config)
    elif experiment_type == "memory_profile":
        merged_payload = build_memory_profile_payload(merged_entries, merged_config)
    else:
        raise ValueError(f"Unsupported experiment_type: {experiment_type}")

    merged_payload["merge_summary"] = {
        "merged_at": _utc_now(),
        "input_file_count": len(payloads),
        "coverage": coverage,
    }
    runtime_feedback = build_runtime_feedback_summary(
        merged_payload,
        alpha=runtime_feedback_alpha,
        tolerance=runtime_feedback_tolerance,
        max_iterations=runtime_feedback_max_iterations,
    )
    if runtime_feedback is not None:
        merged_payload["runtime_feedback"] = runtime_feedback
    return merged_payload


def merge_benchmark_payload_files(
    paths: Sequence[str],
    manifest_path: Optional[str] = None,
    runtime_feedback_alpha: float = 0.7,
    runtime_feedback_tolerance: float = 0.02,
    runtime_feedback_max_iterations: int = 5,
) -> Dict[str, Any]:
    return merge_benchmark_payloads(
        load_payload_files(paths),
        manifest=load_manifest_file(manifest_path),
        runtime_feedback_alpha=runtime_feedback_alpha,
        runtime_feedback_tolerance=runtime_feedback_tolerance,
        runtime_feedback_max_iterations=runtime_feedback_max_iterations,
    )


def _bytes_to_mib(value: Optional[int]) -> float:
    if value is None:
        return 0.0
    return round(float(value) / (1024**2), 1)


def _format_optional_float(value: Optional[float], decimals: int = 3) -> str:
    if value is None:
        return ""
    return f"{float(value):.{decimals}f}"


def _format_optional_mib(value: Optional[int]) -> str:
    if value is None:
        return ""
    return f"{_bytes_to_mib(value):.1f}"


def _render_coverage_lines(coverage: Optional[Dict[str, Any]]) -> List[str]:
    if not coverage:
        return []
    lines = [
        "## Shard Coverage",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Observed payloads | {coverage['observed_payloads']} |",
        f"| Observed shards | {coverage['observed_subtask_indices']} |",
        f"| Missing shards | {coverage['missing_subtask_indices']} |",
        f"| Duplicate shards | {coverage['duplicate_subtask_indices']} |",
        f"| Unexpected shards | {coverage['unexpected_subtask_indices']} |",
        f"| Manifest-complete | {'yes' if coverage['is_complete'] else 'no'} |",
        "",
    ]
    mismatches = coverage.get("mismatched_manifest_assignments") or []
    if mismatches:
        lines.append("Manifest assignment mismatches:")
        lines.append("")
        lines.append("| Shard | Expected | Observed |")
        lines.append("| --- | --- | --- |")
        for mismatch in mismatches:
            lines.append(
                f"| {mismatch['subtask_index']} | "
                f"`{', '.join(mismatch['expected_item_ids'])}` | "
                f"`{', '.join(mismatch['observed_item_ids'])}` |"
            )
        lines.append("")
    return lines


def _render_matrix_lines(
    payload: Dict[str, Any],
    runtime_feedback: Optional[Dict[str, Any]],
) -> List[str]:
    results = payload.get("results", [])
    mean_r_error = round(
        statistics.mean(float(result.get("r_error") or 0.0) for result in results), 4
    )
    within_005 = sum(
        1 for result in results if float(result.get("r_error") or 0.0) <= 0.05
    )
    lines = [
        "## R-Value Matrix Summary",
        "",
        f"- Workload cells: {len(results)}",
        f"- Mean R error: {mean_r_error}",
        f"- Cells within error <= 0.05: {within_005}/{len(results)}",
        "",
        "| Workload | Rows | Ops | Derived R | Best R | R Error | Derived (s) | Best (s) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(
            f"| {result['workload_id']} | {result['num_rows']} | {result['transform_ops']} | "
            f"{float(result['derived_ratio']):.3f} | {float(result['best_ratio']):.3f} | "
            f"{float(result['r_error']):.3f} | {float(result['derived_duration_s']):.3f} | "
            f"{float(result['best_duration_s']):.3f} |"
        )
    lines.append("")

    if runtime_feedback:
        lines.extend(
            [
                "## Runtime Feedback Convergence",
                "",
                f"- Alpha: {runtime_feedback['alpha']}",
                f"- Tolerance: {runtime_feedback['tolerance']}",
                f"- Mean initial error: {runtime_feedback['mean_initial_error']}",
                f"- Mean final error: {runtime_feedback['mean_final_error']}",
                f"- Converged workloads: "
                f"{runtime_feedback['converged_workloads']}/{runtime_feedback['total_workloads']}",
                "",
                "| Workload | Default R | Derived R | Best R | Default Error | Final Error | Iterations |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in runtime_feedback["workloads"]:
            lines.append(
                f"| {item['workload_id']} | {item['default_ratio']:.3f} | "
                f"{item['derived_ratio']:.3f} | {item['best_ratio']:.3f} | "
                f"{item['default_error']:.3f} | {item['final_error']:.3f} | "
                f"{item['iterations_to_converge']} |"
            )
        lines.append("")
    return lines


def _memory_profile_groups(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[bool, Dict[str, Any]]] = {}
    for run in payload.get("runs", []):
        metadata = run.get("metadata") or {}
        group_id = str(metadata.get("run_group_id") or run.get("workload_id"))
        groups.setdefault(group_id, {})[bool(run.get("enable_cbo"))] = run

    rows: List[Dict[str, Any]] = []
    for group_id, pair in groups.items():
        off_run = pair.get(False)
        on_run = pair.get(True)
        sample = on_run or off_run or {}
        metadata = sample.get("metadata") or {}
        if off_run and on_run and float(off_run.get("duration_s") or 0.0) > 0:
            delta_pct = (
                (float(on_run["duration_s"]) - float(off_run["duration_s"]))
                / float(off_run["duration_s"])
                * 100.0
            )
        else:
            delta_pct = None
        rows.append(
            {
                "group_id": group_id,
                "workload_id": sample.get("workload_id"),
                "repetition": metadata.get("repetition"),
                "transform_ops": metadata.get("transform_ops"),
                "payload_bytes_per_row": metadata.get("payload_bytes_per_row"),
                "off_run": off_run,
                "on_run": on_run,
                "delta_pct": delta_pct,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            int((row["off_run"] or row["on_run"] or {}).get("total_input_rows") or 0),
            int(row.get("transform_ops") or 0),
            int(row.get("repetition") or 0),
            str(row["group_id"]),
        ),
    )


def _render_memory_profile_lines(payload: Dict[str, Any]) -> List[str]:
    groups = _memory_profile_groups(payload)
    complete_pairs = [row for row in groups if row["off_run"] and row["on_run"]]
    lines = [
        "## Memory Profile Summary",
        "",
        f"- Run groups: {len(groups)}",
        f"- Complete OFF/ON pairs: {len(complete_pairs)}",
        "",
        "| Group | Rows | Ops | Payload/row | OFF Time (s) | ON Time (s) | Delta | OFF Peak Heap MiB | ON Peak Heap MiB | ON Spill MiB |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in groups:
        off_run = row["off_run"]
        on_run = row["on_run"]
        sample = on_run or off_run or {}
        delta = "" if row["delta_pct"] is None else f"{row['delta_pct']:+.1f}%"
        on_spill_bytes = None
        if on_run is not None:
            on_spill_bytes = max(
                int(on_run.get("global_bytes_spilled", 0) or 0),
                int(on_run.get("dataset_bytes_spilled", 0) or 0),
            )
        lines.append(
            f"| {row['group_id']} | {sample.get('total_input_rows', '')} | "
            f"{row.get('transform_ops', '')} | {row.get('payload_bytes_per_row', '')} | "
            f"{_format_optional_float(None if off_run is None else off_run['duration_s'])} | "
            f"{_format_optional_float(None if on_run is None else on_run['duration_s'])} | "
            f"{delta} | "
            f"{_format_optional_mib(None if off_run is None else off_run.get('peak_heap_bytes'))} | "
            f"{_format_optional_mib(None if on_run is None else on_run.get('peak_heap_bytes'))} | "
            f"{_format_optional_mib(on_spill_bytes)} |"
        )
    lines.append("")
    return lines


def _render_per_rule_lines(payload: Dict[str, Any]) -> List[str]:
    results = payload.get("results", [])
    lines = [
        "## Per-Rule Summary",
        "",
        f"- Rules benchmarked: {len(results)}",
        "",
        "| Rule | Category | Goal | Baseline (s) | Optimized (s) | Improvement |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(
            f"| {result['rule_name']} | {result['rule_category']} | "
            f"{result['optimization_goal']} | {float(result['baseline_time_s']):.3f} | "
            f"{float(result['optimized_time_s']):.3f} | {float(result['improvement_pct']):+.1f}% |"
        )
    lines.append("")
    return lines


def _render_end_to_end_lines(payload: Dict[str, Any]) -> List[str]:
    aggregates = payload.get("aggregates", {})
    lines = [
        "## End-to-End Summary",
        "",
        "| Mode | CBO OFF (s) | CBO ON (s) | Delta |",
        "| --- | ---: | ---: | ---: |",
    ]
    for mode in ("batch", "streaming"):
        off = aggregates.get(f"{mode}-cbo-False")
        on = aggregates.get(f"{mode}-cbo-True")
        if not off or not on:
            continue
        off_duration = float(off["duration_s_mean"])
        on_duration = float(on["duration_s_mean"])
        delta_pct = (
            ((on_duration - off_duration) / off_duration * 100.0)
            if off_duration
            else 0.0
        )
        lines.append(
            f"| {mode} | {off_duration:.3f} | {on_duration:.3f} | {delta_pct:+.1f}% |"
        )
    lines.append("")
    return lines


def render_markdown_report(
    payload: Dict[str, Any],
    title: Optional[str] = None,
    runtime_feedback_alpha: float = 0.7,
    runtime_feedback_tolerance: float = 0.02,
    runtime_feedback_max_iterations: int = 5,
) -> str:
    experiment_type = str(payload["experiment_type"])
    coverage = (payload.get("merge_summary") or {}).get("coverage")
    runtime_feedback = payload.get("runtime_feedback")
    if runtime_feedback is None:
        runtime_feedback = build_runtime_feedback_summary(
            payload,
            alpha=runtime_feedback_alpha,
            tolerance=runtime_feedback_tolerance,
            max_iterations=runtime_feedback_max_iterations,
        )

    lines: List[str] = [
        f"# {title or 'Ray Data CBO Benchmark Report'}",
        "",
        f"- Benchmark: `{payload.get('benchmark_name')}`",
        f"- Experiment: `{experiment_type}`",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Schema version: `{payload.get('schema_version')}`",
        "",
    ]

    lines.extend(_render_coverage_lines(coverage))

    if experiment_type == "per_rule":
        lines.extend(_render_per_rule_lines(payload))
    elif experiment_type == "end_to_end":
        lines.extend(_render_end_to_end_lines(payload))
    elif experiment_type == "r_value_matrix":
        lines.extend(_render_matrix_lines(payload, runtime_feedback))
    elif experiment_type == "memory_profile":
        lines.extend(_render_memory_profile_lines(payload))
    else:
        raise ValueError(
            f"Unsupported experiment_type for report rendering: {experiment_type}"
        )

    return "\n".join(lines).rstrip() + "\n"


def write_markdown_report(
    path: str,
    payload: Dict[str, Any],
    title: Optional[str] = None,
    runtime_feedback_alpha: float = 0.7,
    runtime_feedback_tolerance: float = 0.02,
    runtime_feedback_max_iterations: int = 5,
) -> str:
    rendered = render_markdown_report(
        payload,
        title=title,
        runtime_feedback_alpha=runtime_feedback_alpha,
        runtime_feedback_tolerance=runtime_feedback_tolerance,
        runtime_feedback_max_iterations=runtime_feedback_max_iterations,
    )
    report_path = Path(path)
    report_path.write_text(rendered, encoding="utf-8")
    return str(report_path)


def _render_key_value_table(items: Sequence[Tuple[str, Any]]) -> List[str]:
    lines = [
        "| Field | Value |",
        "| --- | --- |",
    ]
    for key, value in items:
        lines.append(f"| {key} | {value} |")
    lines.append("")
    return lines


def _render_environment_lines(environment_metadata: Dict[str, Any]) -> List[str]:
    ordered_keys = [
        "benchmark_date",
        "ray_version",
        "python_version",
        "git_sha",
        "git_branch",
        "platform",
        "cluster_name",
        "hardware",
        "owner",
        "command",
        "notes",
        "hostname",
        "cwd",
    ]
    items: List[Tuple[str, Any]] = []
    seen = set()
    for key in ordered_keys:
        if key in environment_metadata:
            items.append((key, environment_metadata.get(key, "")))
            seen.add(key)
    for key in sorted(environment_metadata):
        if key in seen:
            continue
        items.append((key, environment_metadata[key]))
    return _render_key_value_table(items)


def render_execution_log_markdown(
    payload: Dict[str, Any],
    environment_metadata: Dict[str, Any],
    bundle_manifest: Dict[str, Any],
) -> str:
    coverage = (payload.get("merge_summary") or {}).get("coverage", {})
    lines: List[str] = [
        "# Ray Data CBO Benchmark Execution Log",
        "",
        "## Execution Summary",
        "",
    ]
    lines.extend(
        _render_key_value_table(
            [
                ("benchmark_name", payload.get("benchmark_name")),
                ("experiment_type", payload.get("experiment_type")),
                ("payload_generated_at", payload.get("generated_at")),
                ("bundle_generated_at", bundle_manifest.get("generated_at")),
                ("result_json", bundle_manifest["artifacts"]["result_json"]),
                ("report_markdown", bundle_manifest["artifacts"]["report_markdown"]),
                (
                    "input_result_count",
                    len(bundle_manifest.get("input_result_paths") or []),
                ),
                ("manifest_path", bundle_manifest.get("manifest_path") or ""),
            ]
        )
    )
    lines.extend(
        [
            "## Environment Metadata",
            "",
        ]
    )
    lines.extend(_render_environment_lines(environment_metadata))
    lines.extend(
        [
            "## Shard Coverage",
            "",
        ]
    )
    lines.extend(
        _render_key_value_table(
            [
                ("observed_payloads", coverage.get("observed_payloads", "")),
                (
                    "observed_subtask_indices",
                    coverage.get("observed_subtask_indices", ""),
                ),
                (
                    "missing_subtask_indices",
                    coverage.get("missing_subtask_indices", ""),
                ),
                (
                    "duplicate_subtask_indices",
                    coverage.get("duplicate_subtask_indices", ""),
                ),
                (
                    "unexpected_subtask_indices",
                    coverage.get("unexpected_subtask_indices", ""),
                ),
                ("is_complete", coverage.get("is_complete", "")),
            ]
        )
    )
    lines.extend(
        [
            "## Bundle Artifacts",
            "",
        ]
    )
    lines.extend(
        _render_key_value_table(
            [
                (name, path)
                for name, path in sorted(bundle_manifest["artifacts"].items())
            ]
        )
    )
    lines.extend(
        [
            "## Input Result Files",
            "",
        ]
    )
    input_result_paths = bundle_manifest.get("input_result_paths") or []
    if input_result_paths:
        for path in input_result_paths:
            lines.append(f"- `{path}`")
    else:
        lines.append("- No explicit input-result file list recorded.")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_matrix_qa_lines(payload: Dict[str, Any]) -> List[str]:
    results = payload.get("results", [])
    runtime_feedback = payload.get("runtime_feedback") or {}
    mean_r_error = (
        round(
            statistics.mean(float(result.get("r_error") or 0.0) for result in results),
            4,
        )
        if results
        else 0.0
    )
    within_005 = sum(
        1 for result in results if float(result.get("r_error") or 0.0) <= 0.05
    )
    lines = [
        "### Q5: CBO 推导的 reservation_ratio 是否可信？",
        "",
        f"- 同链路 workload cells: {len(results)}",
        f"- `R` 平均误差: {mean_r_error}",
        f"- `R` 误差 <= 0.05 的 workload: {within_005}/{len(results)}",
        "",
        "| Workload | Rows | Ops | Derived R | Best R | R Error |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(
            f"| {result['workload_id']} | {result['num_rows']} | {result['transform_ops']} | "
            f"{float(result['derived_ratio']):.3f} | {float(result['best_ratio']):.3f} | "
            f"{float(result['r_error']):.3f} |"
        )
    lines.append("")
    if runtime_feedback:
        lines.extend(
            [
                "Runtime feedback convergence summary:",
                "",
                f"- Alpha: {runtime_feedback.get('alpha')}",
                f"- Mean initial error: {runtime_feedback.get('mean_initial_error')}",
                f"- Mean final error: {runtime_feedback.get('mean_final_error')}",
                f"- Converged workloads: "
                f"{runtime_feedback.get('converged_workloads')}/"
                f"{runtime_feedback.get('total_workloads')}",
                "",
            ]
        )
    return lines


def _render_memory_qa_lines(payload: Dict[str, Any]) -> List[str]:
    config = payload.get("config") or {}
    groups = _memory_profile_groups(payload)
    complete_pairs = [row for row in groups if row["off_run"] and row["on_run"]]
    preset_name = config.get("profile_preset")
    lines = [
        "### Q5: 10GB / 10M 内存消耗测试是否已覆盖？",
        "",
        f"- Profile preset: `{preset_name or 'custom'}`",
        f"- Target rows: `{config.get('target_rows')}`",
        f"- Target total bytes: `{config.get('target_total_bytes')}`",
        f"- Complete OFF/ON run groups: {len(complete_pairs)}",
        "",
        "| Group | Rows | Ops | Payload/row | OFF Peak Heap MiB | ON Peak Heap MiB | ON Spill MiB |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in groups:
        off_run = row["off_run"]
        on_run = row["on_run"]
        on_spill_bytes = None
        if on_run is not None:
            on_spill_bytes = max(
                int(on_run.get("global_bytes_spilled", 0) or 0),
                int(on_run.get("dataset_bytes_spilled", 0) or 0),
            )
        sample = on_run or off_run or {}
        lines.append(
            f"| {row['group_id']} | {sample.get('total_input_rows', '')} | "
            f"{row.get('transform_ops', '')} | {row.get('payload_bytes_per_row', '')} | "
            f"{_format_optional_mib(None if off_run is None else off_run.get('peak_heap_bytes'))} | "
            f"{_format_optional_mib(None if on_run is None else on_run.get('peak_heap_bytes'))} | "
            f"{_format_optional_mib(on_spill_bytes)} |"
        )
    lines.append("")
    return lines


def render_community_qa_markdown(
    payload: Dict[str, Any],
    bundle_manifest: Dict[str, Any],
) -> str:
    coverage = (payload.get("merge_summary") or {}).get("coverage", {})
    lines: List[str] = [
        "# Ray Data CBO Community QA Update",
        "",
        "## 本轮基准覆盖结论",
        "",
        f"- Experiment type: `{payload.get('experiment_type')}`",
        f"- Result JSON: `{bundle_manifest['artifacts']['result_json']}`",
        f"- Report Markdown: `{bundle_manifest['artifacts']['report_markdown']}`",
        f"- Coverage complete: `{coverage.get('is_complete')}`",
        "",
        "## 对社区反馈的直接响应",
        "",
        "### Q5: subtask 拆分是否可回收？",
        "",
        f"- Observed shard indices: {coverage.get('observed_subtask_indices', [])}",
        f"- Missing shard indices: {coverage.get('missing_subtask_indices', [])}",
        f"- Duplicate shard indices: {coverage.get('duplicate_subtask_indices', [])}",
        "",
        "### Q5: 产出的文档和设计细节是否可复用？",
        "",
        "- Phase5 publication bundle 已固化以下产物：",
        f"  - 主报告：`{bundle_manifest['artifacts']['report_markdown']}`",
        f"  - 执行日志：`{bundle_manifest['artifacts']['execution_log_markdown']}`",
        f"  - 社区 QA 附录：`{bundle_manifest['artifacts']['community_qa_markdown']}`",
        f"  - 环境元数据：`{bundle_manifest['artifacts']['environment_metadata_json']}`",
        "",
    ]
    experiment_type = payload.get("experiment_type")
    if experiment_type == "r_value_matrix":
        lines.extend(_render_matrix_qa_lines(payload))
    elif experiment_type == "memory_profile":
        lines.extend(_render_memory_qa_lines(payload))
    return "\n".join(lines).rstrip() + "\n"


def write_publication_bundle(
    output_dir: str,
    payload: Dict[str, Any],
    title: Optional[str] = None,
    environment_metadata: Optional[Dict[str, Any]] = None,
    input_result_paths: Optional[Sequence[str]] = None,
    manifest_path: Optional[str] = None,
    runtime_feedback_alpha: float = 0.7,
    runtime_feedback_tolerance: float = 0.02,
    runtime_feedback_max_iterations: int = 5,
) -> Dict[str, Any]:
    bundle_path = Path(output_dir)
    bundle_path.mkdir(parents=True, exist_ok=True)

    result_json_path = bundle_path / "merged_benchmark_results.json"
    report_path = bundle_path / "cbo_benchmark_report.md"
    execution_log_path = bundle_path / "cbo_benchmark_execution_log.md"
    community_qa_path = bundle_path / "cbo_benchmark_community_qa.md"
    environment_metadata_path = bundle_path / "environment_metadata.json"
    environment_template_path = bundle_path / "environment_metadata.template.json"
    bundle_manifest_path = bundle_path / "bundle_manifest.json"

    merged_payload = copy.deepcopy(payload)
    runtime_feedback = merged_payload.get("runtime_feedback")
    if runtime_feedback is None:
        runtime_feedback = build_runtime_feedback_summary(
            merged_payload,
            alpha=runtime_feedback_alpha,
            tolerance=runtime_feedback_tolerance,
            max_iterations=runtime_feedback_max_iterations,
        )
        if runtime_feedback is not None:
            merged_payload["runtime_feedback"] = runtime_feedback

    env_metadata = build_environment_metadata(
        user_metadata=environment_metadata,
        input_result_paths=input_result_paths,
        manifest_path=manifest_path,
        output_path=str(result_json_path),
        report_markdown_path=str(report_path),
        bundle_dir=str(bundle_path),
    )
    environment_metadata_path.write_text(
        json.dumps(env_metadata, indent=2),
        encoding="utf-8",
    )
    environment_template_path.write_text(
        json.dumps(build_environment_metadata_template(), indent=2),
        encoding="utf-8",
    )

    result_json_path.write_text(json.dumps(merged_payload, indent=2), encoding="utf-8")
    write_markdown_report(
        str(report_path),
        merged_payload,
        title=title,
        runtime_feedback_alpha=runtime_feedback_alpha,
        runtime_feedback_tolerance=runtime_feedback_tolerance,
        runtime_feedback_max_iterations=runtime_feedback_max_iterations,
    )

    bundle_manifest = {
        "generated_at": _utc_now(),
        "benchmark_name": merged_payload.get("benchmark_name"),
        "experiment_type": merged_payload.get("experiment_type"),
        "input_result_paths": list(input_result_paths or []),
        "manifest_path": manifest_path,
        "artifacts": {
            "result_json": str(result_json_path),
            "report_markdown": str(report_path),
            "execution_log_markdown": str(execution_log_path),
            "community_qa_markdown": str(community_qa_path),
            "environment_metadata_json": str(environment_metadata_path),
            "environment_metadata_template_json": str(environment_template_path),
        },
    }
    execution_log_path.write_text(
        render_execution_log_markdown(merged_payload, env_metadata, bundle_manifest),
        encoding="utf-8",
    )
    community_qa_path.write_text(
        render_community_qa_markdown(merged_payload, bundle_manifest),
        encoding="utf-8",
    )
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest, indent=2), encoding="utf-8"
    )
    bundle_manifest["artifacts"]["bundle_manifest_json"] = str(bundle_manifest_path)
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest, indent=2), encoding="utf-8"
    )
    return bundle_manifest
