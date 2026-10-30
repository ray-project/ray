from __future__ import annotations

import base64
import io
import json
import os
import re
import shlex
import subprocess
import sys
import tarfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from ray.data.benchmarks import cbo_benchmark_harness as benchmark_harness
except ImportError:
    import cbo_benchmark_harness as benchmark_harness  # type: ignore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def _resolve_path(path: Optional[str], base_dir: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(base_dir, path))


def _join_csv(values: Sequence[Any]) -> str:
    return ",".join(str(value) for value in values)


def _normalize_artifact_sync_config(
    backend: Dict[str, Any],
    *,
    default_enabled: bool,
) -> Dict[str, Any]:
    raw = dict(backend.get("artifact_sync") or {})
    return {
        "enabled": bool(raw.get("enabled", default_enabled)),
        "mode": str(raw.get("mode", "embedded")),
        "remote_base_dir": str(
            raw.get("remote_base_dir", "/tmp/cbo_benchmark_campaign_artifacts")
        ),
        "max_inline_bytes": int(raw.get("max_inline_bytes", 32 * 1024 * 1024)),
    }


@dataclass
class CampaignTask:
    task_id: str
    experiment_type: str
    stage: str
    description: str
    command: List[str]
    output_path: str
    log_path: str
    dependency_ids: List[str] = field(default_factory=list)
    cwd: Optional[str] = None
    resume_skip_if_output_exists: bool = True
    artifact_paths: List[str] = field(default_factory=list)


def build_campaign_spec_template() -> Dict[str, Any]:
    return {
        "campaign_name": "cbo_phase6_campaign",
        "output_dir": "./cbo_campaign_output",
        "report_title": "Ray Data CBO Benchmark Campaign",
        "environment_metadata_json": None,
        "working_dir": None,
        "python_executable": sys.executable,
        "benchmark_script": None,
        "feedback_alpha": 0.7,
        "feedback_tolerance": 0.02,
        "feedback_max_iterations": 5,
        "executor": {
            "max_parallelism": 4,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": True,
            "state_path": "./cbo_campaign_output/execution_state.json",
            "targets": [],
            "remote_targets": [],
        },
        "experiments": {
            "r_value_matrix": {
                "enabled": True,
                "row_scales": [200000, 500000],
                "operator_counts": [2, 4, 6],
                "sweep_ratios": [0.1, 0.3, 0.5, 0.7, 0.9],
                "repetitions": 2,
                "warmup": True,
                "subtask_count": 2,
            },
            "memory_profile": {
                "enabled": True,
                "profile_preset": "10gb_10m",
                "operator_counts": [2, 4, 6],
                "repetitions": 1,
                "warmup": False,
                "subtask_count": 2,
                "memory_poll_interval_s": 0.1,
            },
            "per_rule": {
                "enabled": False,
                "num_rows": 200000,
                "repetitions": 2,
                "warmup": False,
                "rules": [],
            },
            "end_to_end": {
                "enabled": False,
                "batch_rows": 200000,
                "streaming_rows": 150000,
                "repetitions": 2,
                "warmup": True,
            },
        },
    }


def load_campaign_spec_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        raise ValueError(f"Campaign spec must be a JSON object, got {type(payload)!r}")
    payload["__spec_path__"] = os.path.abspath(path)
    payload["__spec_dir__"] = os.path.dirname(os.path.abspath(path))
    return payload


def write_campaign_spec_template(path: str) -> str:
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(build_campaign_spec_template(), fp, indent=2)
    return path


def _normalize_plan_context(
    spec: Dict[str, Any],
    script_path: Optional[str] = None,
    python_executable: Optional[str] = None,
) -> Dict[str, Any]:
    spec_dir = spec.get("__spec_dir__") or os.getcwd()
    output_dir = _resolve_path(spec.get("output_dir"), spec_dir)
    if not output_dir:
        raise ValueError("campaign spec must provide output_dir")
    resolved_script_path = (
        script_path
        or spec.get("benchmark_script")
        or os.path.abspath(os.path.join(os.path.dirname(__file__), "cbo_benchmark.py"))
    )
    if not os.path.isabs(resolved_script_path):
        resolved_script_path = os.path.abspath(
            os.path.join(spec_dir, resolved_script_path)
        )

    resolved_python = (
        python_executable or spec.get("python_executable") or sys.executable
    )
    working_dir = _resolve_path(spec.get("working_dir"), spec_dir) or os.getcwd()
    environment_metadata_json = _resolve_path(
        spec.get("environment_metadata_json"),
        spec_dir,
    )
    return {
        "spec_dir": spec_dir,
        "output_dir": output_dir,
        "script_path": resolved_script_path,
        "python_executable": resolved_python,
        "working_dir": working_dir,
        "environment_metadata_json": environment_metadata_json,
        "feedback_alpha": spec.get("feedback_alpha", 0.7),
        "feedback_tolerance": spec.get("feedback_tolerance", 0.02),
        "feedback_max_iterations": spec.get("feedback_max_iterations", 5),
    }


def _normalize_executor_config(
    spec: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    raw_executor = dict(spec.get("executor") or {})
    max_parallelism = int(raw_executor.get("max_parallelism", 1))
    local_max_parallelism = int(
        raw_executor.get("local_max_parallelism", max_parallelism)
    )
    if max_parallelism <= 0 or local_max_parallelism <= 0:
        raise ValueError("executor parallelism values must be positive")
    raw_targets = raw_executor.get("targets")
    if raw_targets is None:
        raw_targets = raw_executor.get("remote_targets") or []
    targets: List[Dict[str, Any]] = []
    for index, target in enumerate(raw_targets):
        if not isinstance(target, dict):
            raise ValueError("executor.targets entries must be objects")
        name = str(target.get("name") or f"remote-{index}")
        capacity = int(target.get("capacity", 1))
        if capacity <= 0:
            raise ValueError(f"executor.targets[{index}].capacity must be positive")
        stages = [str(stage) for stage in (target.get("stages") or ["run_shard"])]
        backend = dict(target.get("backend") or {})
        if not backend and target.get("command_template"):
            backend = {
                "type": "template",
                "command_template": list(target["command_template"]),
            }
        backend_type = str(backend.get("type") or "local")
        normalized_backend: Dict[str, Any] = {"type": backend_type}
        if backend_type == "local":
            pass
        elif backend_type == "template":
            command_template = backend.get("command_template") or []
            if not isinstance(command_template, list) or not command_template:
                raise ValueError(
                    f"executor.targets[{index}].backend.command_template must be a non-empty list"
                )
            normalized_backend["command_template"] = [
                str(part) for part in command_template
            ]
            normalized_backend["artifact_sync"] = _normalize_artifact_sync_config(
                backend,
                default_enabled=True,
            )
        elif backend_type == "ssh":
            if not backend.get("host"):
                raise ValueError(f"executor.targets[{index}].backend.host is required")
            normalized_backend.update(
                {
                    "host": str(backend["host"]),
                    "ssh_binary": str(backend.get("ssh_binary", "ssh")),
                    "ssh_args": [str(arg) for arg in (backend.get("ssh_args") or [])],
                    "remote_working_dir": backend.get("remote_working_dir"),
                    "artifact_sync": _normalize_artifact_sync_config(
                        backend,
                        default_enabled=True,
                    ),
                }
            )
        elif backend_type == "ray_job":
            if not backend.get("address"):
                raise ValueError(
                    f"executor.targets[{index}].backend.address is required"
                )
            normalized_backend.update(
                {
                    "address": str(backend["address"]),
                    "ray_binary": str(backend.get("ray_binary", "ray")),
                    "ray_args": [str(arg) for arg in (backend.get("ray_args") or [])],
                    "working_dir": backend.get("working_dir"),
                    "job_working_dir": backend.get("job_working_dir"),
                    "runtime_env_json": backend.get("runtime_env_json"),
                    "submission_id_prefix": str(
                        backend.get("submission_id_prefix", "cbo-bench")
                    ),
                    "poll_interval_s": float(backend.get("poll_interval_s", 5.0)),
                    "timeout_s": _parse_duration_seconds(
                        backend.get("timeout_s", backend.get("timeout")),
                        3600.0,
                    ),
                    "collect_logs": bool(backend.get("collect_logs", True)),
                    "artifact_sync": _normalize_artifact_sync_config(
                        backend,
                        default_enabled=True,
                    ),
                }
            )
        elif backend_type == "k8s_job":
            if not backend.get("image"):
                raise ValueError(f"executor.targets[{index}].backend.image is required")
            normalized_backend.update(
                {
                    "image": str(backend["image"]),
                    "kubectl_binary": str(backend.get("kubectl_binary", "kubectl")),
                    "kubectl_args": [
                        str(arg) for arg in (backend.get("kubectl_args") or [])
                    ],
                    "namespace": backend.get("namespace"),
                    "job_name_prefix": str(backend.get("job_name_prefix", "cbo-bench")),
                    "container_working_dir": backend.get("container_working_dir"),
                    "wait_for_completion": bool(
                        backend.get("wait_for_completion", True)
                    ),
                    "timeout": str(backend.get("timeout", "3600s")),
                    "timeout_s": _parse_duration_seconds(
                        backend.get("timeout_s", backend.get("timeout", "3600s")),
                        3600.0,
                    ),
                    "poll_interval_s": float(backend.get("poll_interval_s", 5.0)),
                    "collect_logs": bool(backend.get("collect_logs", True)),
                    "cleanup": bool(backend.get("cleanup", False)),
                    "artifact_sync": _normalize_artifact_sync_config(
                        backend,
                        default_enabled=True,
                    ),
                }
            )
        else:
            raise ValueError(f"Unsupported executor backend type: {backend_type}")
        targets.append(
            {
                "name": name,
                "capacity": capacity,
                "stages": stages,
                "backend": normalized_backend,
            }
        )

    state_path = _resolve_path(
        raw_executor.get("state_path")
        or os.path.join(context["output_dir"], "execution_state.json"),
        context["spec_dir"],
    )
    return {
        "max_parallelism": max_parallelism,
        "local_max_parallelism": local_max_parallelism,
        "allow_local_fallback_for_remote_stages": bool(
            raw_executor.get("allow_local_fallback_for_remote_stages", True)
        ),
        "targets": targets,
        "state_path": state_path,
    }


def _plan_file(path: str, payload: Any) -> Dict[str, Any]:
    return {"path": path, "payload": payload}


def _subtask_file_name(subtask_index: int, subtask_count: int) -> str:
    width = max(3, len(str(max(subtask_count - 1, 0))))
    return f"shard_{subtask_index:0{width}d}_of_{subtask_count}.json"


def _build_base_command(context: Dict[str, Any]) -> List[str]:
    return [context["python_executable"], context["script_path"]]


def _stringify_optional_bool_flag(command: List[str], flag: str, enabled: bool) -> None:
    """Appends the flag for ``argparse.BooleanOptionalAction`` arguments.

    The benchmark CLI defaults warmup to on, so "off" has to be spelled out as
    ``--no-warmup``; omitting the flag would silently keep it enabled.
    """
    command.append(flag if enabled else "--no-" + flag.lstrip("-"))


def _extend_common_finalize_flags(
    command: List[str],
    experiment_dir: str,
    context: Dict[str, Any],
    report_title: Optional[str],
) -> None:
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--feedback-alpha",
            str(context["feedback_alpha"]),
            "--feedback-tolerance",
            str(context["feedback_tolerance"]),
            "--feedback-max-iterations",
            str(context["feedback_max_iterations"]),
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )


def _build_r_value_matrix_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "r_value_matrix"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    shard_dir = os.path.join(experiment_dir, "shards")
    log_dir = os.path.join(experiment_dir, "logs")
    manifest_path = os.path.join(experiment_dir, "manifest.json")
    input_results_path = os.path.join(experiment_dir, "input_results.json")
    merged_output_path = os.path.join(experiment_dir, "merged.json")
    bundle_manifest_path = os.path.join(
        experiment_dir, "bundle", "bundle_manifest.json"
    )

    row_scales = list(spec.get("row_scales") or [])
    operator_counts = list(spec.get("operator_counts") or [])
    sweep_ratios = list(spec.get("sweep_ratios") or [])
    repetitions = int(spec.get("repetitions", 1))
    subtask_count = int(spec.get("subtask_count", 1))
    input_results = spec.get("input_results")
    manifest_override = spec.get("manifest_path")

    support_files: List[Dict[str, Any]] = []
    run_tasks: List[CampaignTask] = []
    dependency_ids: List[str] = []

    if input_results:
        resolved_input_results = [
            _resolve_path(path, context["spec_dir"]) for path in input_results
        ]
        resolved_manifest_path = _resolve_path(manifest_override, context["spec_dir"])
    else:
        if not row_scales or not operator_counts or not sweep_ratios:
            raise ValueError(
                "r_value_matrix campaign spec requires row_scales, "
                "operator_counts, and sweep_ratios unless input_results is provided"
            )
        workloads = benchmark_harness.build_parameterized_workloads(
            row_counts=row_scales,
            operator_counts=operator_counts,
            payload_bytes_per_row=0,
        )
        manifest_payload = {
            experiment_type: [
                asdict(item)
                for item in benchmark_harness.build_parameterized_subtask_manifest(
                    workloads,
                    subtask_count=subtask_count,
                    experiment_type=experiment_type,
                )
            ]
        }
        support_files.append(_plan_file(manifest_path, manifest_payload))
        resolved_manifest_path = manifest_path
        resolved_input_results = []
        for subtask_index in range(subtask_count):
            output_path = os.path.join(
                shard_dir, _subtask_file_name(subtask_index, subtask_count)
            )
            log_path = os.path.join(
                log_dir,
                f"run_shard_{subtask_index:03d}.log",
            )
            command = _build_base_command(context)
            command.extend(
                [
                    "--r-value-matrix",
                    "--matrix-row-scales",
                    _join_csv(row_scales),
                    "--matrix-operator-counts",
                    _join_csv(operator_counts),
                    "--matrix-ratios",
                    _join_csv(sweep_ratios),
                    "--repetitions",
                    str(repetitions),
                    "--subtask-count",
                    str(subtask_count),
                    "--subtask-index",
                    str(subtask_index),
                    "--output",
                    output_path,
                ]
            )
            _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
            task = CampaignTask(
                task_id=f"{experiment_type}.run.{subtask_index}",
                experiment_type=experiment_type,
                stage="run_shard",
                description=(
                    f"Run {experiment_type} shard {subtask_index}/{subtask_count}"
                ),
                command=command,
                output_path=output_path,
                log_path=log_path,
                cwd=context["working_dir"],
                artifact_paths=[output_path],
            )
            run_tasks.append(task)
            dependency_ids.append(task.task_id)
            resolved_input_results.append(output_path)

    support_files.append(_plan_file(input_results_path, resolved_input_results))
    finalize_command = _build_base_command(context)
    finalize_command.extend(
        [
            "--merge-results",
            "--input-results-file",
            input_results_path,
            "--output",
            merged_output_path,
        ]
    )
    if resolved_manifest_path:
        finalize_command.extend(["--merge-manifest", resolved_manifest_path])
    _extend_common_finalize_flags(
        finalize_command,
        experiment_dir,
        context,
        report_title,
    )
    finalize_task = CampaignTask(
        task_id=f"{experiment_type}.finalize",
        experiment_type=experiment_type,
        stage="finalize",
        description=f"Merge and bundle {experiment_type}",
        command=finalize_command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "finalize.log"),
        dependency_ids=dependency_ids,
        cwd=context["working_dir"],
        artifact_paths=[merged_output_path, os.path.join(experiment_dir, "bundle")],
    )
    return {
        "tasks": [asdict(task) for task in [*run_tasks, finalize_task]],
        "support_files": support_files,
    }


def _build_memory_profile_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "memory_profile"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    shard_dir = os.path.join(experiment_dir, "shards")
    log_dir = os.path.join(experiment_dir, "logs")
    manifest_path = os.path.join(experiment_dir, "manifest.json")
    input_results_path = os.path.join(experiment_dir, "input_results.json")
    merged_output_path = os.path.join(experiment_dir, "merged.json")
    bundle_manifest_path = os.path.join(
        experiment_dir, "bundle", "bundle_manifest.json"
    )

    repetitions = int(spec.get("repetitions", 1))
    subtask_count = int(spec.get("subtask_count", 1))
    input_results = spec.get("input_results")
    manifest_override = spec.get("manifest_path")
    memory_poll_interval_s = float(spec.get("memory_poll_interval_s", 0.1))

    support_files: List[Dict[str, Any]] = []
    run_tasks: List[CampaignTask] = []
    dependency_ids: List[str] = []

    if input_results:
        resolved_input_results = [
            _resolve_path(path, context["spec_dir"]) for path in input_results
        ]
        resolved_manifest_path = _resolve_path(manifest_override, context["spec_dir"])
    else:
        operator_counts = list(spec.get("operator_counts") or [])
        if not operator_counts:
            raise ValueError(
                "memory_profile campaign spec requires operator_counts "
                "unless input_results is provided"
            )
        if spec.get("profile_preset"):
            profile_inputs = benchmark_harness.resolve_memory_profile_preset(
                str(spec["profile_preset"]),
                operator_counts=operator_counts,
            )
        else:
            row_scales = list(spec.get("row_scales") or [])
            if not row_scales:
                raise ValueError(
                    "memory_profile campaign spec requires row_scales "
                    "when profile_preset is not provided"
                )
            profile_inputs = {
                "preset_name": None,
                "row_counts": row_scales,
                "operator_counts": operator_counts,
                "payload_bytes_per_row": int(spec["payload_bytes_per_row"]),
            }
        memory_plan = benchmark_harness.build_memory_profile_plan(
            row_counts=profile_inputs["row_counts"],
            operator_counts=profile_inputs["operator_counts"],
            repetitions=repetitions,
            payload_bytes_per_row=profile_inputs["payload_bytes_per_row"],
        )
        manifest_payload = {
            experiment_type: [
                asdict(item)
                for item in benchmark_harness.build_memory_profile_subtask_manifest(
                    memory_plan,
                    subtask_count=subtask_count,
                )
            ]
        }
        support_files.append(_plan_file(manifest_path, manifest_payload))
        resolved_manifest_path = manifest_path
        resolved_input_results = []

        for subtask_index in range(subtask_count):
            output_path = os.path.join(
                shard_dir, _subtask_file_name(subtask_index, subtask_count)
            )
            log_path = os.path.join(
                log_dir,
                f"run_shard_{subtask_index:03d}.log",
            )
            command = _build_base_command(context)
            command.extend(
                [
                    "--memory-profile",
                    "--profile-operator-counts",
                    _join_csv(profile_inputs["operator_counts"]),
                    "--repetitions",
                    str(repetitions),
                    "--subtask-count",
                    str(subtask_count),
                    "--subtask-index",
                    str(subtask_index),
                    "--memory-poll-interval-s",
                    str(memory_poll_interval_s),
                    "--output",
                    output_path,
                ]
            )
            _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
            if profile_inputs.get("preset_name"):
                command.extend(["--profile-preset", profile_inputs["preset_name"]])
            else:
                command.extend(
                    [
                        "--profile-row-scales",
                        _join_csv(profile_inputs["row_counts"]),
                        "--payload-bytes-per-row",
                        str(profile_inputs["payload_bytes_per_row"]),
                    ]
                )
            task = CampaignTask(
                task_id=f"{experiment_type}.run.{subtask_index}",
                experiment_type=experiment_type,
                stage="run_shard",
                description=(
                    f"Run {experiment_type} shard {subtask_index}/{subtask_count}"
                ),
                command=command,
                output_path=output_path,
                log_path=log_path,
                cwd=context["working_dir"],
                artifact_paths=[output_path],
            )
            run_tasks.append(task)
            dependency_ids.append(task.task_id)
            resolved_input_results.append(output_path)

    support_files.append(_plan_file(input_results_path, resolved_input_results))
    finalize_command = _build_base_command(context)
    finalize_command.extend(
        [
            "--merge-results",
            "--input-results-file",
            input_results_path,
            "--output",
            merged_output_path,
        ]
    )
    if resolved_manifest_path:
        finalize_command.extend(["--merge-manifest", resolved_manifest_path])
    _extend_common_finalize_flags(
        finalize_command,
        experiment_dir,
        context,
        report_title,
    )
    finalize_task = CampaignTask(
        task_id=f"{experiment_type}.finalize",
        experiment_type=experiment_type,
        stage="finalize",
        description=f"Merge and bundle {experiment_type}",
        command=finalize_command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "finalize.log"),
        dependency_ids=dependency_ids,
        cwd=context["working_dir"],
        artifact_paths=[merged_output_path, os.path.join(experiment_dir, "bundle")],
    )
    return {
        "tasks": [asdict(task) for task in [*run_tasks, finalize_task]],
        "support_files": support_files,
    }


def _build_per_rule_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "per_rule"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    log_dir = os.path.join(experiment_dir, "logs")
    output_path = os.path.join(experiment_dir, "results.json")
    bundle_manifest_path = os.path.join(
        experiment_dir, "bundle", "bundle_manifest.json"
    )
    command = _build_base_command(context)
    command.extend(
        [
            "--per-rule",
            "--num-rows",
            str(spec["num_rows"]),
            "--repetitions",
            str(spec["repetitions"]),
            "--output",
            output_path,
        ]
    )
    rules = spec.get("rules") or []
    if rules:
        command.extend(["--rules", _join_csv(rules)])
    _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )
    task = CampaignTask(
        task_id=f"{experiment_type}.run",
        experiment_type=experiment_type,
        stage="run",
        description="Run and bundle per_rule benchmark",
        command=command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "run.log"),
        cwd=context["working_dir"],
        artifact_paths=[output_path, os.path.join(experiment_dir, "bundle")],
    )
    return {"tasks": [asdict(task)], "support_files": []}


def _build_end_to_end_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "end_to_end"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    log_dir = os.path.join(experiment_dir, "logs")
    output_path = os.path.join(experiment_dir, "results.json")
    bundle_manifest_path = os.path.join(
        experiment_dir, "bundle", "bundle_manifest.json"
    )
    command = _build_base_command(context)
    command.extend(
        [
            "--batch-rows",
            str(spec["batch_rows"]),
            "--streaming-rows",
            str(spec["streaming_rows"]),
            "--repetitions",
            str(spec["repetitions"]),
            "--output",
            output_path,
        ]
    )
    _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )
    task = CampaignTask(
        task_id=f"{experiment_type}.run",
        experiment_type=experiment_type,
        stage="run",
        description="Run and bundle end_to_end benchmark",
        command=command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "run.log"),
        cwd=context["working_dir"],
        artifact_paths=[output_path, os.path.join(experiment_dir, "bundle")],
    )
    return {"tasks": [asdict(task)], "support_files": []}


def build_campaign_plan(
    spec: Dict[str, Any],
    script_path: Optional[str] = None,
    python_executable: Optional[str] = None,
) -> Dict[str, Any]:
    context = _normalize_plan_context(
        spec,
        script_path=script_path,
        python_executable=python_executable,
    )
    executor = _normalize_executor_config(spec, context)
    campaign_name = spec.get("campaign_name") or "cbo_campaign"
    experiments = spec.get("experiments") or {}
    report_title = spec.get("report_title")
    tasks: List[Dict[str, Any]] = []
    support_files: List[Dict[str, Any]] = []

    builders = {
        "r_value_matrix": _build_r_value_matrix_plan,
        "memory_profile": _build_memory_profile_plan,
        "per_rule": _build_per_rule_plan,
        "end_to_end": _build_end_to_end_plan,
    }
    for experiment_type, builder in builders.items():
        experiment_spec = experiments.get(experiment_type) or {}
        if not experiment_spec.get("enabled"):
            continue
        partial = builder(experiment_spec, context, report_title)
        tasks.extend(partial["tasks"])
        support_files.extend(partial["support_files"])

    if not tasks:
        raise ValueError("campaign spec does not enable any experiment")

    support_files.append(
        _plan_file(
            os.path.join(context["output_dir"], "campaign_spec.snapshot.json"),
            {key: value for key, value in spec.items() if not key.startswith("__")},
        )
    )

    return {
        "campaign_name": campaign_name,
        "generated_at": _utc_now(),
        "output_dir": context["output_dir"],
        "working_dir": context["working_dir"],
        "script_path": context["script_path"],
        "python_executable": context["python_executable"],
        "environment_metadata_json": context["environment_metadata_json"],
        "executor": executor,
        "tasks": tasks,
        "support_files": support_files,
    }


def materialize_campaign_support_files(plan: Dict[str, Any]) -> List[str]:
    written: List[str] = []
    for entry in plan.get("support_files", []):
        path = entry["path"]
        _ensure_parent_dir(path)
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(entry["payload"], fp, indent=2)
        written.append(path)
    return written


def write_campaign_plan(path: str, plan: Dict[str, Any]) -> str:
    materialize_campaign_support_files(plan)
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(plan, fp, indent=2)
    return path


def _task_output_exists(task: Dict[str, Any]) -> bool:
    return os.path.exists(task["output_path"])


def render_campaign_plan_summary(plan: Dict[str, Any]) -> str:
    executor = plan.get("executor") or {}
    lines = [
        f"Campaign: {plan['campaign_name']}",
        f"Output dir: {plan['output_dir']}",
        f"Task count: {len(plan.get('tasks', []))}",
        (
            "Executor: "
            f"max_parallelism={executor.get('max_parallelism', 1)}, "
            f"local_max_parallelism={executor.get('local_max_parallelism', 1)}, "
            f"targets={len(executor.get('targets') or [])}"
        ),
        "Tasks:",
    ]
    for task in plan.get("tasks", []):
        status = "completed" if _task_output_exists(task) else "pending"
        lines.append(
            f"  - {task['task_id']} [{task['stage']}] "
            f"{task['experiment_type']} -> {task['output_path']} ({status})"
        )
    return "\n".join(lines)


def _slugify_identifier(value: str, max_length: int = 48) -> str:
    slug = re.sub(r"[^a-zA-Z0-9-]+", "-", value).strip("-").lower()
    slug = slug or "task"
    return slug[:max_length].rstrip("-") or "task"


def _render_shell_command(task_command: Sequence[str], cwd: Optional[str]) -> str:
    rendered = shlex.join(list(task_command))
    if cwd:
        return f"cd {shlex.quote(cwd)} && {rendered}"
    return rendered


def _parse_duration_seconds(value: Any, default_seconds: float) -> float:
    if value is None:
        return default_seconds
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip().lower()
    if not raw:
        return default_seconds
    unit_multipliers = {
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
    }
    suffix = raw[-1]
    if suffix in unit_multipliers:
        return float(raw[:-1]) * unit_multipliers[suffix]
    return float(raw)


def _ray_submission_id(task: Dict[str, Any], backend: Dict[str, Any]) -> str:
    return (
        f"{backend.get('submission_id_prefix', 'cbo-bench')}-"
        f"{_slugify_identifier(task['task_id'])}"
    )


def _k8s_job_name(task: Dict[str, Any], backend: Dict[str, Any]) -> str:
    return (
        f"{backend.get('job_name_prefix', 'cbo-bench')}-"
        f"{_slugify_identifier(task['task_id'], max_length=40)}"
    )[:63].rstrip("-")


_ARTIFACT_MANIFEST_BEGIN = "__CBO_ARTIFACT_MANIFEST_BEGIN__"
_ARTIFACT_MANIFEST_END = "__CBO_ARTIFACT_MANIFEST_END__"
_ARTIFACT_PAYLOAD_BEGIN = "__CBO_ARTIFACT_PAYLOAD_BEGIN__"
_ARTIFACT_PAYLOAD_END = "__CBO_ARTIFACT_PAYLOAD_END__"


def _task_artifact_paths(task: Dict[str, Any]) -> List[str]:
    artifact_paths = [str(path) for path in (task.get("artifact_paths") or []) if path]
    if artifact_paths:
        return artifact_paths
    return [str(task["output_path"])]


def _infer_artifact_kind(path: str) -> str:
    if os.path.isdir(path):
        return "directory"
    if os.path.exists(path):
        return "file"
    return "directory" if not Path(path).suffix else "file"


def _build_embedded_artifact_entries(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> List[Dict[str, Any]]:
    backend = dict(target.get("backend") or {})
    backend_type = str(backend.get("type") or "local")
    default_enabled = backend_type in {"template", "ssh", "ray_job", "k8s_job"}
    artifact_sync = _normalize_artifact_sync_config(
        backend,
        default_enabled=default_enabled,
    )
    if not artifact_sync.get("enabled", default_enabled):
        return []
    if str(artifact_sync.get("mode") or "embedded") != "embedded":
        raise ValueError(
            f"Unsupported artifact sync mode: {artifact_sync.get('mode')!r}"
        )

    remote_root = os.path.join(
        str(artifact_sync.get("remote_base_dir")),
        _slugify_identifier(task["task_id"], max_length=80),
    )
    entries: List[Dict[str, Any]] = []
    for index, local_path in enumerate(_task_artifact_paths(task)):
        absolute_local_path = os.path.abspath(local_path)
        path_kind = _infer_artifact_kind(absolute_local_path)
        basename = (
            os.path.basename(absolute_local_path.rstrip(os.sep)) or f"artifact-{index}"
        )
        remote_name = f"{index:03d}_{basename}"
        remote_path = os.path.join(remote_root, remote_name)
        entries.append(
            {
                "artifact_id": f"artifact_{index:03d}",
                "kind": path_kind,
                "local_path": absolute_local_path,
                "remote_path": remote_path,
            }
        )
    return entries


def _rewrite_command_for_artifacts(
    command: Sequence[str],
    artifact_entries: Sequence[Dict[str, Any]],
) -> List[str]:
    replacements = {
        entry["local_path"]: entry["remote_path"] for entry in artifact_entries
    }
    return [str(replacements.get(str(part), part)) for part in command]


def _render_embedded_artifact_wrapper(
    task: Dict[str, Any],
    artifact_entries: Sequence[Dict[str, Any]],
    *,
    max_inline_bytes: int,
) -> str:
    python_binary = str((task.get("command") or ["python3"])[0] or "python3")
    rewritten_command = _rewrite_command_for_artifacts(
        task["command"], artifact_entries
    )
    remote_dirs: List[str] = []
    for entry in artifact_entries:
        remote_path = str(entry["remote_path"])
        if entry["kind"] == "directory":
            remote_dirs.append(remote_path)
        else:
            remote_dirs.append(os.path.dirname(remote_path))
    seen_remote_dirs = sorted({path for path in remote_dirs if path})
    manifest_json = json.dumps(list(artifact_entries), sort_keys=True)
    script_lines = ["set +e"]
    for remote_dir in seen_remote_dirs:
        script_lines.append(f"mkdir -p {shlex.quote(remote_dir)}")
    script_lines.append(_render_shell_command(rewritten_command, task.get("cwd")))
    script_lines.append("cbo_exit_code=$?")
    script_lines.append(f"{shlex.quote(python_binary)} - <<'PY'")
    script_lines.extend(
        [
            "import base64",
            "import io",
            "import json",
            "import os",
            "import tarfile",
            f"manifest = json.loads({manifest_json!r})",
            f"max_inline_bytes = {int(max_inline_bytes)}",
            f"manifest_begin = { _ARTIFACT_MANIFEST_BEGIN!r }",
            f"manifest_end = { _ARTIFACT_MANIFEST_END!r }",
            f"payload_begin = { _ARTIFACT_PAYLOAD_BEGIN!r }",
            f"payload_end = { _ARTIFACT_PAYLOAD_END!r }",
            "prepared = []",
            "emitted_manifest = []",
            "for entry in manifest:",
            "    emitted = dict(entry)",
            "    remote_path = entry['remote_path']",
            "    if not os.path.exists(remote_path):",
            "        emitted['exists'] = False",
            "        emitted_manifest.append(emitted)",
            "        continue",
            "    if entry['kind'] == 'directory':",
            "        buffer = io.BytesIO()",
            "        with tarfile.open(fileobj=buffer, mode='w:gz') as archive:",
            "            archive.add(remote_path, arcname='.')",
            "        payload = buffer.getvalue()",
            "        emitted['encoding'] = 'base64_targz'",
            "    else:",
            "        with open(remote_path, 'rb') as fp:",
            "            payload = fp.read()",
            "        emitted['encoding'] = 'base64_file'",
            "    emitted['exists'] = True",
            "    emitted['size_bytes'] = len(payload)",
            "    if len(payload) > max_inline_bytes:",
            "        emitted['inline_omitted'] = True",
            "        emitted['error'] = f\"artifact exceeds max_inline_bytes={max_inline_bytes}\"",
            "        emitted_manifest.append(emitted)",
            "        continue",
            "    prepared.append((entry['artifact_id'], base64.b64encode(payload).decode('ascii')))",
            "    emitted_manifest.append(emitted)",
            "print(manifest_begin)",
            "print(json.dumps(emitted_manifest, sort_keys=True))",
            "print(manifest_end)",
            "for artifact_id, payload in prepared:",
            "    print(f'{payload_begin} {artifact_id}')",
            "    for offset in range(0, len(payload), 76):",
            "        print(payload[offset:offset + 76])",
            "    print(f'{payload_end} {artifact_id}')",
        ]
    )
    script_lines.append("PY")
    script_lines.append('exit "$cbo_exit_code"')
    return "\n".join(script_lines)


def _prepare_task_for_execution(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if target.get("name") == "local":
        return dict(task), []
    backend = dict(target.get("backend") or {})
    backend_type = str(backend.get("type") or "local")
    if backend_type == "local":
        return dict(task), []
    artifact_entries = _build_embedded_artifact_entries(task, target)
    if not artifact_entries:
        return dict(task), []
    effective_task = dict(task)
    artifact_sync = _normalize_artifact_sync_config(
        backend,
        default_enabled=True,
    )
    effective_task["command"] = [
        "sh",
        "-lc",
        _render_embedded_artifact_wrapper(
            task,
            artifact_entries,
            max_inline_bytes=int(
                artifact_sync.get("max_inline_bytes", 32 * 1024 * 1024)
            ),
        ),
    ]
    return effective_task, artifact_entries


def _parse_embedded_artifact_output(
    output_text: str,
) -> Tuple[str, List[Dict[str, Any]], Dict[str, bytes]]:
    sanitized_lines: List[str] = []
    manifest_lines: List[str] = []
    current_payload_id: Optional[str] = None
    current_payload_lines: List[str] = []
    payloads: Dict[str, bytes] = {}
    in_manifest = False

    for line in output_text.splitlines():
        if line == _ARTIFACT_MANIFEST_BEGIN:
            in_manifest = True
            manifest_lines = []
            continue
        if line == _ARTIFACT_MANIFEST_END:
            in_manifest = False
            continue
        if line.startswith(f"{_ARTIFACT_PAYLOAD_BEGIN} "):
            current_payload_id = line.split(" ", 1)[1].strip()
            current_payload_lines = []
            continue
        if line.startswith(f"{_ARTIFACT_PAYLOAD_END} "):
            artifact_id = line.split(" ", 1)[1].strip()
            if current_payload_id == artifact_id:
                payload_blob = "".join(current_payload_lines).strip()
                if payload_blob:
                    payloads[artifact_id] = base64.b64decode(
                        payload_blob.encode("ascii")
                    )
            current_payload_id = None
            current_payload_lines = []
            continue
        if in_manifest:
            manifest_lines.append(line)
            continue
        if current_payload_id is not None:
            current_payload_lines.append(line.strip())
            continue
        sanitized_lines.append(line)

    manifest_text = "\n".join(manifest_lines).strip()
    if not manifest_text:
        return output_text, [], {}
    manifest = json.loads(manifest_text)
    sanitized_output = "\n".join(sanitized_lines)
    if output_text.endswith("\n"):
        sanitized_output += "\n"
    return sanitized_output, manifest, payloads


def _safe_extract_targz(data: bytes, destination: str) -> None:
    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for member in archive.getmembers():
            member_path = (destination_path / member.name).resolve()
            if (
                destination_path.resolve() not in member_path.parents
                and member_path != destination_path.resolve()
            ):
                raise ValueError(
                    f"Refusing to extract artifact outside destination: {member.name}"
                )
        archive.extractall(destination_path)


def _restore_artifacts_from_output(
    output_text: str,
) -> Tuple[str, List[Dict[str, Any]], List[str]]:
    sanitized_output, manifest, payloads = _parse_embedded_artifact_output(output_text)
    if not manifest:
        return sanitized_output, [], []

    restored: List[Dict[str, Any]] = []
    errors: List[str] = []
    for entry in manifest:
        local_path = str(entry["local_path"])
        artifact_id = str(entry["artifact_id"])
        if not entry.get("exists"):
            errors.append(
                f"{artifact_id}: missing remote artifact {entry['remote_path']}"
            )
            continue
        if entry.get("inline_omitted"):
            errors.append(f"{artifact_id}: {entry.get('error')}")
            continue
        if artifact_id not in payloads:
            errors.append(f"{artifact_id}: payload missing from collected logs")
            continue
        payload = payloads[artifact_id]
        if entry["kind"] == "directory":
            _safe_extract_targz(payload, local_path)
        else:
            _ensure_parent_dir(local_path)
            with open(local_path, "wb") as fp:
                fp.write(payload)
        restored.append(
            {
                "artifact_id": artifact_id,
                "kind": entry["kind"],
                "local_path": local_path,
                "remote_path": entry["remote_path"],
                "size_bytes": entry.get("size_bytes"),
            }
        )
    return sanitized_output, restored, errors


def build_backend_command(
    task: Dict[str, Any],
    target: Optional[Dict[str, Any]],
) -> Tuple[List[str], str]:
    if target is None:
        command = list(task["command"])
        return command, shlex.join(command)

    backend = dict(target.get("backend") or {})
    backend_type = str(backend.get("type") or "local")
    command = list(task["command"])
    cwd = task.get("cwd") or os.getcwd()
    if backend_type == "local":
        return command, shlex.join(command)
    if backend_type == "template":
        rendered_command = shlex.join(command)
        substitutions = {
            "command": rendered_command,
            "cwd": cwd,
            "task_id": task["task_id"],
            "output_path": task["output_path"],
            "log_path": task["log_path"],
            "target_name": target["name"],
        }
        command_template = list(backend["command_template"])
        has_placeholder = any("{" in part and "}" in part for part in command_template)
        if has_placeholder:
            formatted = [part.format(**substitutions) for part in command_template]
            return formatted, shlex.join(formatted)
        wrapped = command_template + command
        return wrapped, shlex.join(wrapped)
    if backend_type == "ssh":
        remote_command = _render_shell_command(
            command,
            str(backend.get("remote_working_dir") or cwd),
        )
        wrapped = [
            backend.get("ssh_binary", "ssh"),
            *list(backend.get("ssh_args") or []),
            backend["host"],
            remote_command,
        ]
        return wrapped, shlex.join(wrapped)
    if backend_type == "ray_job":
        submission_id = _ray_submission_id(task, backend)
        wrapped = [
            backend.get("ray_binary", "ray"),
            *list(backend.get("ray_args") or []),
            "job",
            "submit",
            "--address",
            backend["address"],
            "--submission-id",
            submission_id,
        ]
        if backend.get("working_dir"):
            wrapped.extend(["--working-dir", str(backend["working_dir"])])
        if backend.get("runtime_env_json"):
            wrapped.extend(["--runtime-env-json", str(backend["runtime_env_json"])])
        wrapped.extend(
            [
                "--",
                "sh",
                "-lc",
                _render_shell_command(
                    command,
                    str(backend.get("job_working_dir") or cwd),
                ),
            ]
        )
        return wrapped, shlex.join(wrapped)
    if backend_type == "k8s_job":
        kubectl_binary = backend.get("kubectl_binary", "kubectl")
        job_name = _k8s_job_name(task, backend)
        namespace = backend.get("namespace")
        create_cmd = [
            kubectl_binary,
            *list(backend.get("kubectl_args") or []),
            "create",
            "job",
            job_name,
            "--image",
            backend["image"],
        ]
        if namespace:
            create_cmd.extend(["--namespace", str(namespace)])
        create_cmd.extend(
            [
                "--",
                "sh",
                "-lc",
                _render_shell_command(
                    command,
                    str(backend.get("container_working_dir") or cwd),
                ),
            ]
        )
        return create_cmd, shlex.join(create_cmd)
    raise ValueError(f"Unsupported backend type: {backend_type}")


def _select_dispatch_target(
    task: Dict[str, Any],
    executor: Dict[str, Any],
    active_remote: Dict[str, int],
    active_local: int,
    round_robin_state: Dict[str, int],
) -> Optional[Dict[str, Any]]:
    stage = task["stage"]
    configured_targets = [
        target
        for target in (executor.get("targets") or [])
        if stage in (target.get("stages") or [])
    ]
    if configured_targets:
        start = round_robin_state.get(stage, 0)
        for offset in range(len(configured_targets)):
            index = (start + offset) % len(configured_targets)
            target = configured_targets[index]
            if active_remote.get(target["name"], 0) < int(target["capacity"]):
                round_robin_state[stage] = (index + 1) % len(configured_targets)
                return target
        if not executor.get("allow_local_fallback_for_remote_stages", True):
            return None

    if active_local < int(executor.get("local_max_parallelism", 1)):
        return {
            "name": "local",
            "capacity": int(executor.get("local_max_parallelism", 1)),
            "stages": [],
            "backend": {"type": "local"},
        }
    return None


def _build_initial_execution_state(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "campaign_name": plan["campaign_name"],
        "generated_at": _utc_now(),
        "updated_at": _utc_now(),
        "state_path": (plan.get("executor") or {}).get("state_path"),
        "tasks": {
            task["task_id"]: {
                "status": "pending",
                "experiment_type": task["experiment_type"],
                "stage": task["stage"],
                "output_path": task["output_path"],
                "log_path": task["log_path"],
                "artifact_paths": list(task.get("artifact_paths") or []),
                "dependency_ids": list(task.get("dependency_ids", [])),
                "attempts": 0,
            }
            for task in plan.get("tasks", [])
        },
    }


def _write_execution_state(executor: Dict[str, Any], state: Dict[str, Any]) -> None:
    state["updated_at"] = _utc_now()
    state_path = executor.get("state_path")
    if not state_path:
        return
    _ensure_parent_dir(state_path)
    with open(state_path, "w", encoding="utf-8") as fp:
        json.dump(state, fp, indent=2)


def _run_subprocess_command(
    command: Sequence[str],
    cwd: Optional[str] = None,
) -> Dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    output_text = completed.stdout or ""
    if completed.stderr:
        output_text += completed.stderr
    return {
        "returncode": completed.returncode,
        "output_text": output_text,
    }


def _parse_ray_job_status(output_text: str) -> Optional[str]:
    known_statuses = {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "STOPPED"}
    matches = re.findall(r"\b([A-Z_]+)\b", output_text.upper())
    for token in reversed(matches):
        if token in known_statuses:
            return token
    return None


def _parse_k8s_job_status(output_text: str) -> Tuple[str, Dict[str, Any]]:
    payload = json.loads(output_text)
    status = payload.get("status") or {}
    conditions = {
        str(item.get("type")): str(item.get("status"))
        for item in (status.get("conditions") or [])
        if item.get("type")
    }
    if conditions.get("Failed") == "True" or int(status.get("failed") or 0) > 0:
        return "FAILED", payload
    if conditions.get("Complete") == "True" or int(status.get("succeeded") or 0) > 0:
        return "SUCCEEDED", payload
    if int(status.get("active") or 0) > 0:
        return "RUNNING", payload
    return "PENDING", payload


def _apply_artifact_restore(
    result: Dict[str, Any],
) -> Dict[str, Any]:
    (
        sanitized_output,
        restored_artifacts,
        artifact_errors,
    ) = _restore_artifacts_from_output(result.get("output_text", ""))
    result["output_text"] = sanitized_output
    result["artifacts"] = restored_artifacts
    result["artifact_errors"] = artifact_errors
    if artifact_errors and int(result.get("returncode", 0)) == 0:
        result["returncode"] = 65
        error_block = "\n".join(f"[artifact-sync] {error}" for error in artifact_errors)
        if result["output_text"] and not result["output_text"].endswith("\n"):
            result["output_text"] += "\n"
        result["output_text"] += error_block + "\n"
    return result


def _execute_basic_backend(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    effective_task, artifact_entries = _prepare_task_for_execution(task, target)
    command, launched_command = build_backend_command(
        effective_task,
        None if target.get("name") == "local" else target,
    )
    result = _run_subprocess_command(command, cwd=effective_task.get("cwd") or None)
    result.update(
        {
            "launched_command": launched_command,
            "assigned_target": target["name"],
            "backend_type": (
                "local"
                if target.get("name") == "local"
                else str((target.get("backend") or {}).get("type") or "local")
            ),
            "backend_handle": None,
            "status_history": [],
            "artifacts": [],
            "artifact_errors": [],
        }
    )
    if artifact_entries:
        result = _apply_artifact_restore(result)
    return result


def _execute_ray_job_backend(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    backend = dict(target["backend"])
    effective_task, artifact_entries = _prepare_task_for_execution(task, target)
    submit_command, launched_command = build_backend_command(effective_task, target)
    submission_id = _ray_submission_id(task, backend)
    status_history: List[Dict[str, Any]] = []
    output_chunks: List[str] = []

    submit_result = _run_subprocess_command(
        submit_command,
        cwd=effective_task.get("cwd") or None,
    )
    output_chunks.append(submit_result["output_text"])
    if submit_result["returncode"] != 0:
        return {
            "returncode": submit_result["returncode"],
            "output_text": "".join(output_chunks),
            "launched_command": launched_command,
            "assigned_target": target["name"],
            "backend_type": "ray_job",
            "backend_handle": submission_id,
            "status_history": status_history,
            "artifacts": [],
            "artifact_errors": [],
        }

    deadline = time.monotonic() + float(backend.get("timeout_s", 3600.0))
    poll_interval_s = float(backend.get("poll_interval_s", 5.0))
    terminal_status: Optional[str] = None
    while True:
        status_command = [
            backend.get("ray_binary", "ray"),
            *list(backend.get("ray_args") or []),
            "job",
            "status",
            "--address",
            backend["address"],
            submission_id,
        ]
        status_result = _run_subprocess_command(
            status_command,
            cwd=effective_task.get("cwd") or None,
        )
        output_chunks.append(status_result["output_text"])
        status = _parse_ray_job_status(status_result["output_text"]) or "UNKNOWN"
        status_history.append(
            {
                "status": status,
                "command": shlex.join(status_command),
                "returncode": status_result["returncode"],
                "observed_at": _utc_now(),
            }
        )
        if status_result["returncode"] != 0:
            return {
                "returncode": status_result["returncode"],
                "output_text": "".join(output_chunks),
                "launched_command": launched_command,
                "assigned_target": target["name"],
                "backend_type": "ray_job",
                "backend_handle": submission_id,
                "status_history": status_history,
                "artifacts": [],
                "artifact_errors": [],
            }
        if status in {"SUCCEEDED", "FAILED", "STOPPED"}:
            terminal_status = status
            break
        if time.monotonic() >= deadline:
            output_chunks.append(
                f"\n[timeout] ray job {submission_id} exceeded {backend.get('timeout_s', 3600.0)}s\n"
            )
            return {
                "returncode": 124,
                "output_text": "".join(output_chunks),
                "launched_command": launched_command,
                "assigned_target": target["name"],
                "backend_type": "ray_job",
                "backend_handle": submission_id,
                "status_history": status_history,
                "artifacts": [],
                "artifact_errors": [],
            }
        time.sleep(poll_interval_s)

    if backend.get("collect_logs", True):
        logs_command = [
            backend.get("ray_binary", "ray"),
            *list(backend.get("ray_args") or []),
            "job",
            "logs",
            "--address",
            backend["address"],
            submission_id,
        ]
        logs_result = _run_subprocess_command(
            logs_command,
            cwd=effective_task.get("cwd") or None,
        )
        output_chunks.append(logs_result["output_text"])

    result = {
        "returncode": 0 if terminal_status == "SUCCEEDED" else 1,
        "output_text": "".join(output_chunks),
        "launched_command": launched_command,
        "assigned_target": target["name"],
        "backend_type": "ray_job",
        "backend_handle": submission_id,
        "status_history": status_history,
        "artifacts": [],
        "artifact_errors": [],
    }
    if artifact_entries:
        result = _apply_artifact_restore(result)
    return result


def _execute_k8s_job_backend(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    backend = dict(target["backend"])
    effective_task, artifact_entries = _prepare_task_for_execution(task, target)
    create_command, launched_command = build_backend_command(effective_task, target)
    job_name = _k8s_job_name(task, backend)
    namespace = backend.get("namespace")
    kubectl_binary = backend.get("kubectl_binary", "kubectl")
    status_history: List[Dict[str, Any]] = []
    output_chunks: List[str] = []

    create_result = _run_subprocess_command(
        create_command,
        cwd=effective_task.get("cwd") or None,
    )
    output_chunks.append(create_result["output_text"])
    if create_result["returncode"] != 0:
        return {
            "returncode": create_result["returncode"],
            "output_text": "".join(output_chunks),
            "launched_command": launched_command,
            "assigned_target": target["name"],
            "backend_type": "k8s_job",
            "backend_handle": job_name,
            "status_history": status_history,
            "artifacts": [],
            "artifact_errors": [],
        }

    deadline = time.monotonic() + float(backend.get("timeout_s", 3600.0))
    poll_interval_s = float(backend.get("poll_interval_s", 5.0))
    terminal_status: Optional[str] = None
    while True:
        get_command = [kubectl_binary, "get", "job", job_name, "-o", "json"]
        if backend.get("kubectl_args"):
            get_command = [
                kubectl_binary,
                *list(backend.get("kubectl_args") or []),
                "get",
                "job",
                job_name,
                "-o",
                "json",
            ]
        if namespace:
            get_command.extend(["--namespace", str(namespace)])
        get_result = _run_subprocess_command(
            get_command,
            cwd=effective_task.get("cwd") or None,
        )
        output_chunks.append(get_result["output_text"])
        if get_result["returncode"] != 0:
            return {
                "returncode": get_result["returncode"],
                "output_text": "".join(output_chunks),
                "launched_command": launched_command,
                "assigned_target": target["name"],
                "backend_type": "k8s_job",
                "backend_handle": job_name,
                "status_history": status_history,
                "artifacts": [],
                "artifact_errors": [],
            }
        status, payload = _parse_k8s_job_status(get_result["output_text"])
        status_history.append(
            {
                "status": status,
                "command": shlex.join(get_command),
                "observed_at": _utc_now(),
                "raw_status": payload.get("status") or {},
            }
        )
        if status in {"SUCCEEDED", "FAILED"}:
            terminal_status = status
            break
        if time.monotonic() >= deadline:
            output_chunks.append(
                f"\n[timeout] k8s job {job_name} exceeded {backend.get('timeout_s', 3600.0)}s\n"
            )
            return {
                "returncode": 124,
                "output_text": "".join(output_chunks),
                "launched_command": launched_command,
                "assigned_target": target["name"],
                "backend_type": "k8s_job",
                "backend_handle": job_name,
                "status_history": status_history,
                "artifacts": [],
                "artifact_errors": [],
            }
        time.sleep(poll_interval_s)

    if backend.get("collect_logs", True):
        logs_command = [kubectl_binary, "logs", f"job/{job_name}"]
        if backend.get("kubectl_args"):
            logs_command = [
                kubectl_binary,
                *list(backend.get("kubectl_args") or []),
                "logs",
                f"job/{job_name}",
            ]
        if namespace:
            logs_command.extend(["--namespace", str(namespace)])
        logs_result = _run_subprocess_command(
            logs_command,
            cwd=effective_task.get("cwd") or None,
        )
        output_chunks.append(logs_result["output_text"])

    if backend.get("cleanup", False):
        delete_command = [kubectl_binary, "delete", "job", job_name]
        if backend.get("kubectl_args"):
            delete_command = [
                kubectl_binary,
                *list(backend.get("kubectl_args") or []),
                "delete",
                "job",
                job_name,
            ]
        if namespace:
            delete_command.extend(["--namespace", str(namespace)])
        delete_result = _run_subprocess_command(
            delete_command,
            cwd=effective_task.get("cwd") or None,
        )
        output_chunks.append(delete_result["output_text"])

    result = {
        "returncode": 0 if terminal_status == "SUCCEEDED" else 1,
        "output_text": "".join(output_chunks),
        "launched_command": launched_command,
        "assigned_target": target["name"],
        "backend_type": "k8s_job",
        "backend_handle": job_name,
        "status_history": status_history,
        "artifacts": [],
        "artifact_errors": [],
    }
    if artifact_entries:
        result = _apply_artifact_restore(result)
    return result


def _run_campaign_task(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    if target.get("name") == "local":
        return _execute_basic_backend(task, target)
    backend_type = str((target.get("backend") or {}).get("type") or "local")
    if backend_type in {"template", "ssh", "local"}:
        return _execute_basic_backend(task, target)
    if backend_type == "ray_job":
        return _execute_ray_job_backend(task, target)
    if backend_type == "k8s_job":
        return _execute_k8s_job_backend(task, target)
    raise ValueError(f"Unsupported backend type for execution: {backend_type}")


def execute_campaign_plan(
    plan: Dict[str, Any],
    resume: bool = True,
    dry_run: bool = False,
    log_fn=print,
) -> Dict[str, Any]:
    materialize_campaign_support_files(plan)
    executor = plan.get("executor") or {
        "max_parallelism": 1,
        "local_max_parallelism": 1,
        "targets": [],
        "allow_local_fallback_for_remote_stages": True,
        "state_path": os.path.join(plan["output_dir"], "execution_state.json"),
    }
    task_results: List[Dict[str, Any]] = []
    tasks_by_id = {task["task_id"]: task for task in plan.get("tasks", [])}
    pending_ids = list(tasks_by_id)
    completed_ids: set[str] = set()
    submitted_ids: set[str] = set()
    active_local = 0
    active_remote: Dict[str, int] = {}
    round_robin_state: Dict[str, int] = {}
    state = _build_initial_execution_state(plan)
    state_lock = Lock()
    _write_execution_state(executor, state)

    def _update_task_state(
        task_id: str,
        status: str,
        **extra: Any,
    ) -> None:
        with state_lock:
            task_state = state["tasks"][task_id]
            task_state["status"] = status
            if status == "running":
                task_state["started_at"] = _utc_now()
                task_state["attempts"] = int(task_state.get("attempts", 0)) + 1
            if status in {"completed", "failed", "skipped_existing", "dry_run"}:
                task_state["finished_at"] = _utc_now()
            task_state.update(extra)
            _write_execution_state(executor, state)

    futures: Dict[Future, Tuple[str, Dict[str, Any]]] = {}
    max_workers = int(executor.get("max_parallelism", 1))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while len(completed_ids) < len(tasks_by_id):
            progress_made = False
            for task_id in pending_ids:
                if task_id in completed_ids or task_id in submitted_ids:
                    continue
                task = tasks_by_id[task_id]
                if any(
                    dep not in completed_ids for dep in task.get("dependency_ids", [])
                ):
                    continue

                output_exists = _task_output_exists(task)
                if (
                    resume
                    and task.get("resume_skip_if_output_exists", True)
                    and output_exists
                ):
                    log_fn(
                        f"[skip] {task['task_id']} output exists: {task['output_path']}"
                    )
                    task_results.append(
                        {
                            "task_id": task["task_id"],
                            "status": "skipped_existing",
                            "output_path": task["output_path"],
                        }
                    )
                    _update_task_state(
                        task["task_id"],
                        "skipped_existing",
                        output_path=task["output_path"],
                    )
                    completed_ids.add(task["task_id"])
                    progress_made = True
                    continue

                if dry_run:
                    preview_task = task
                    preview_target: Optional[Dict[str, Any]] = None
                    if executor.get("targets"):
                        maybe_target = _select_dispatch_target(
                            task,
                            executor,
                            {},
                            0,
                            {},
                        )
                        if maybe_target is not None:
                            preview_task, _ = _prepare_task_for_execution(
                                task, maybe_target
                            )
                            preview_target = (
                                None
                                if maybe_target.get("name") == "local"
                                else maybe_target
                            )
                    display_command = build_backend_command(
                        preview_task, preview_target
                    )[1]
                    log_fn(f"[dry-run] {task['task_id']}: {display_command}")
                    task_results.append(
                        {
                            "task_id": task["task_id"],
                            "status": "dry_run",
                            "output_path": task["output_path"],
                        }
                    )
                    _update_task_state(
                        task["task_id"],
                        "dry_run",
                        output_path=task["output_path"],
                    )
                    completed_ids.add(task["task_id"])
                    progress_made = True
                    continue

                dispatch_target = _select_dispatch_target(
                    task,
                    executor,
                    active_remote,
                    active_local,
                    round_robin_state,
                )
                if dispatch_target is None:
                    continue

                _ensure_parent_dir(task["log_path"])
                _ensure_parent_dir(task["output_path"])
                log_fn(f"[run] {task['task_id']} -> {dispatch_target['name']}")
                submitted_ids.add(task["task_id"])
                if dispatch_target["name"] == "local":
                    active_local += 1
                else:
                    active_remote[dispatch_target["name"]] = (
                        active_remote.get(dispatch_target["name"], 0) + 1
                    )
                launch_preview = build_backend_command(
                    _prepare_task_for_execution(task, dispatch_target)[0],
                    None if dispatch_target["name"] == "local" else dispatch_target,
                )[1]
                _update_task_state(
                    task["task_id"],
                    "running",
                    assigned_target=dispatch_target["name"],
                    launched_command=launch_preview,
                )
                futures[pool.submit(_run_campaign_task, task, dispatch_target)] = (
                    task["task_id"],
                    dispatch_target,
                )
                progress_made = True

            if futures:
                done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
                for future in done:
                    task_id, dispatch_target = futures.pop(future)
                    task = tasks_by_id[task_id]
                    if dispatch_target["name"] == "local":
                        active_local = max(active_local - 1, 0)
                    else:
                        active_remote[dispatch_target["name"]] = max(
                            active_remote.get(dispatch_target["name"], 1) - 1,
                            0,
                        )
                    result = future.result()
                    with open(task["log_path"], "w", encoding="utf-8") as fp:
                        fp.write(result["output_text"])
                    if result["returncode"] != 0:
                        _update_task_state(
                            task_id,
                            "failed",
                            assigned_target=result["assigned_target"],
                            backend_type=result.get("backend_type"),
                            backend_handle=result.get("backend_handle"),
                            status_history=result.get("status_history"),
                            artifacts=result.get("artifacts"),
                            artifact_errors=result.get("artifact_errors"),
                            launched_command=result["launched_command"],
                            returncode=result["returncode"],
                            output_path=task["output_path"],
                            log_path=task["log_path"],
                        )
                        raise RuntimeError(
                            f"Task {task_id} failed with exit code {result['returncode']}. "
                            f"See log: {task['log_path']}"
                        )
                    task_results.append(
                        {
                            "task_id": task_id,
                            "status": "completed",
                            "output_path": task["output_path"],
                            "log_path": task["log_path"],
                            "assigned_target": result["assigned_target"],
                            "backend_type": result.get("backend_type"),
                            "backend_handle": result.get("backend_handle"),
                            "artifacts": result.get("artifacts") or [],
                        }
                    )
                    _update_task_state(
                        task_id,
                        "completed",
                        assigned_target=result["assigned_target"],
                        backend_type=result.get("backend_type"),
                        backend_handle=result.get("backend_handle"),
                        status_history=result.get("status_history"),
                        artifacts=result.get("artifacts"),
                        artifact_errors=result.get("artifact_errors"),
                        launched_command=result["launched_command"],
                        returncode=result["returncode"],
                        output_path=task["output_path"],
                        log_path=task["log_path"],
                    )
                    completed_ids.add(task_id)
                    progress_made = True
                continue

            if not progress_made:
                remaining = [
                    task_id
                    for task_id in pending_ids
                    if task_id not in completed_ids and task_id not in submitted_ids
                ]
                raise RuntimeError(
                    "Campaign execution made no progress. Remaining tasks: "
                    + ", ".join(remaining)
                )

    return {
        "campaign_name": plan["campaign_name"],
        "executed_at": _utc_now(),
        "state_path": executor.get("state_path"),
        "task_results": task_results,
    }
