from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

SCHEMA_VERSION = "1.0.0"
BENCHMARK_NAME = "ray_data_cbo"


class ExperimentType(str, Enum):
    """Top-level benchmark experiment kinds."""

    PER_RULE = "per_rule"
    END_TO_END = "end_to_end"
    R_VALUE_MATRIX = "r_value_matrix"
    MEMORY_PROFILE = "memory_profile"


class RecordType(str, Enum):
    """Normalized record types stored under ``payload['records']``."""

    PER_RULE_RESULT = "per_rule_result"
    EXECUTION_RUN = "execution_run"


@dataclass
class BenchmarkRecord:
    """Normalized benchmark record used across all experiment types."""

    record_type: str
    workload_id: str
    plan_id: str
    cbo_enabled: Optional[bool]
    duration_s: float
    operator_count: Optional[int] = None
    data_scale_rows: Optional[int] = None
    data_scale_bytes: Optional[int] = None
    peak_object_store_bytes: Optional[int] = None
    peak_heap_bytes: Optional[int] = None
    spill_bytes: Optional[int] = None
    derived_r: Optional[float] = None
    best_r_from_sweep: Optional[float] = None
    r_error: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_id(prefix: str, *parts: Any) -> str:
    joined = "||".join(str(part) for part in parts if part not in (None, ""))
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _parse_ratio(label: Optional[str]) -> Optional[float]:
    if not label:
        return None
    match = re.search(r"R=([0-9]+(?:\.[0-9]+)?)", label)
    if match is None:
        return None
    return float(match.group(1))


def _parse_num_partitions(label: Optional[str]) -> Optional[int]:
    if not label:
        return None
    match = re.search(r"N=([0-9]+)", label)
    if match is None:
        return None
    return int(match.group(1))


def _find_best_sweep_entry(
    entries: Optional[List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if not entries:
        return None
    candidates = [entry for entry in entries if entry.get("time_s") is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda entry: entry["time_s"])


def _validation_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    errors = validate_payload(payload)
    return {
        "is_valid": not errors,
        "errors": errors,
        "record_count": len(payload.get("records", [])),
    }


def normalize_per_rule_records(
    results: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    num_rows = config.get("num_rows")

    for result in results:
        rule_name = result["rule_name"]
        pipeline_desc = result["pipeline_desc"]
        sweep_detail = result.get("sweep_detail")
        best_sweep = _find_best_sweep_entry(sweep_detail)
        spill_bytes = None
        derived_r = None
        best_r = None
        r_error = None

        metadata: Dict[str, Any] = {
            "rule_name": rule_name,
            "rule_category": result.get("rule_category"),
            "optimization_goal": result.get("optimization_goal"),
            "pipeline_desc": pipeline_desc,
            "baseline_label": result.get("baseline_label"),
            "optimized_label": result.get("optimized_label"),
            "baseline_time_s": result.get("baseline_time_s"),
            "optimized_time_s": result.get("optimized_time_s"),
            "improvement_pct": result.get("improvement_pct"),
        }

        if rule_name == "DeriveReservationRatio":
            best_r = (
                float(best_sweep["ratio"])
                if best_sweep is not None and best_sweep.get("ratio") is not None
                else None
            )
            metadata["default_r"] = _parse_ratio(result.get("baseline_label"))
            metadata["optimized_label_r"] = _parse_ratio(result.get("optimized_label"))
            metadata["derivation_mode"] = "parameter_sweep_only"
        elif rule_name == "DeriveShufflePartitions":
            metadata["default_num_partitions"] = _parse_num_partitions(
                result.get("baseline_label")
            )
            metadata["best_num_partitions_from_sweep"] = (
                int(best_sweep["num_partitions"])
                if best_sweep is not None
                and best_sweep.get("num_partitions") is not None
                else None
            )
            metadata["derivation_mode"] = "parameter_sweep_only"

        record = BenchmarkRecord(
            record_type=RecordType.PER_RULE_RESULT.value,
            workload_id=_stable_id("workload", rule_name, num_rows),
            plan_id=_stable_id("plan", pipeline_desc),
            cbo_enabled=None,
            duration_s=float(result.get("optimized_time_s", 0.0)),
            operator_count=None,
            data_scale_rows=num_rows,
            data_scale_bytes=None,
            peak_object_store_bytes=None,
            peak_heap_bytes=None,
            spill_bytes=spill_bytes,
            derived_r=derived_r,
            best_r_from_sweep=best_r,
            r_error=r_error,
            metadata=metadata,
        )
        normalized.append(record.to_dict())

    return normalized


def normalize_end_to_end_records(
    runs: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    for run in runs:
        mode = run["mode"]
        data_scale_rows = config.get(f"{mode}_rows")
        spill_bytes = max(
            int(run.get("global_bytes_spilled", 0) or 0),
            int(run.get("dataset_bytes_spilled", 0) or 0),
        )
        cbo_enabled = bool(run.get("enable_cbo"))
        derived_r = (
            float(run["reservation_ratio"])
            if cbo_enabled and run.get("reservation_ratio") is not None
            else None
        )

        metadata: Dict[str, Any] = {
            "mode": mode,
            "latency_p50_ms": run.get("latency_p50_ms"),
            "latency_p95_ms": run.get("latency_p95_ms"),
            "latency_p99_ms": run.get("latency_p99_ms"),
            "latency_mean_ms": run.get("latency_mean_ms"),
            "streaming_schedule_s": run.get("streaming_schedule_s"),
            "total_input_rows": run.get("total_input_rows"),
        }

        record = BenchmarkRecord(
            record_type=RecordType.EXECUTION_RUN.value,
            workload_id=_stable_id("workload", mode, data_scale_rows),
            plan_id=_stable_id("plan", mode),
            cbo_enabled=cbo_enabled,
            duration_s=float(run.get("duration_s", 0.0)),
            operator_count=run.get("operator_count"),
            data_scale_rows=data_scale_rows,
            data_scale_bytes=None,
            peak_object_store_bytes=None,
            peak_heap_bytes=None,
            spill_bytes=spill_bytes,
            derived_r=derived_r,
            best_r_from_sweep=None,
            r_error=None,
            metadata=metadata,
        )
        normalized.append(record.to_dict())

    return normalized


def build_per_rule_payload(
    results: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_name": BENCHMARK_NAME,
        "experiment_type": ExperimentType.PER_RULE.value,
        "generated_at": _utc_now(),
        "mode": "per-rule",
        "config": config,
        "results": results,
        "records": normalize_per_rule_records(results, config),
    }
    payload["validation"] = _validation_block(payload)
    return payload


def build_end_to_end_payload(
    runs: List[Dict[str, Any]],
    aggregates: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_name": BENCHMARK_NAME,
        "experiment_type": ExperimentType.END_TO_END.value,
        "generated_at": _utc_now(),
        "mode": "end-to-end",
        "config": config,
        "runs": runs,
        "aggregates": aggregates,
        "records": normalize_end_to_end_records(runs, config),
    }
    payload["validation"] = _validation_block(payload)
    return payload


def normalize_r_value_matrix_records(
    results: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    for result in results:
        num_rows = result.get("num_rows")
        transform_ops = result.get("transform_ops")
        workload_id = result.get("workload_id") or _stable_id(
            "workload",
            "r_matrix",
            num_rows,
            transform_ops,
        )
        pipeline_desc = result.get("pipeline_desc") or (
            f"filter->map_batches*x{transform_ops}->repartition->sort"
        )

        record = BenchmarkRecord(
            record_type=RecordType.EXECUTION_RUN.value,
            workload_id=workload_id,
            plan_id=_stable_id("plan", pipeline_desc, transform_ops),
            cbo_enabled=True,
            duration_s=float(result.get("derived_duration_s", 0.0)),
            operator_count=result.get("operator_count"),
            data_scale_rows=num_rows,
            data_scale_bytes=result.get("data_scale_bytes"),
            peak_object_store_bytes=result.get("peak_object_store_bytes"),
            peak_heap_bytes=result.get("peak_heap_bytes"),
            spill_bytes=result.get("spill_bytes"),
            derived_r=result.get("derived_ratio"),
            best_r_from_sweep=result.get("best_ratio"),
            r_error=result.get("r_error"),
            metadata={
                "pipeline_desc": pipeline_desc,
                "transform_ops": transform_ops,
                "default_ratio": result.get("default_ratio"),
                "default_duration_s": result.get("default_duration_s"),
                "best_duration_s": result.get("best_duration_s"),
                "sweep_detail": result.get("sweep_detail"),
                "subtask_index": result.get("subtask_index"),
                "subtask_count": result.get("subtask_count"),
                # Measurement quality: a best ratio only means something when the
                # paired delta clears the same-setting noise floor.
                "blocks": result.get("blocks"),
                "noise_floor_s": result.get("noise_floor_s"),
                "noise_floor_pct": result.get("noise_floor_pct"),
                "best_is_significant": result.get("best_is_significant"),
                "derived_vs_default_pct": result.get("derived_vs_default_pct"),
                "derived_vs_default_ci95": result.get("derived_vs_default_ci95"),
                "derived_vs_default_significant": result.get(
                    "derived_vs_default_significant"
                ),
            },
        )
        normalized.append(record.to_dict())

    return normalized


def build_r_value_matrix_payload(
    results: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_name": BENCHMARK_NAME,
        "experiment_type": ExperimentType.R_VALUE_MATRIX.value,
        "generated_at": _utc_now(),
        "mode": "r-value-matrix",
        "config": config,
        "results": results,
        "records": normalize_r_value_matrix_records(results, config),
    }
    payload["validation"] = _validation_block(payload)
    return payload


def normalize_memory_profile_records(
    runs: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    for run in runs:
        metadata = dict(run.get("metadata") or {})
        workload_id = run.get("workload_id") or _stable_id(
            "workload",
            "memory_profile",
            metadata.get("transform_ops"),
            run.get("total_input_rows"),
            run.get("data_scale_bytes"),
        )
        plan_desc = (
            run.get("plan_description")
            or metadata.get("pipeline_desc")
            or (
                f"filter->map_batches*x{metadata.get('transform_ops')}->repartition->sort"
            )
        )
        spill_bytes = max(
            int(run.get("global_bytes_spilled", 0) or 0),
            int(run.get("dataset_bytes_spilled", 0) or 0),
        )

        record = BenchmarkRecord(
            record_type=RecordType.EXECUTION_RUN.value,
            workload_id=workload_id,
            plan_id=_stable_id("plan", plan_desc, metadata.get("transform_ops")),
            cbo_enabled=bool(run.get("enable_cbo")),
            duration_s=float(run.get("duration_s", 0.0)),
            operator_count=run.get("operator_count"),
            data_scale_rows=run.get("total_input_rows"),
            data_scale_bytes=run.get("data_scale_bytes"),
            peak_object_store_bytes=run.get("peak_object_store_bytes"),
            peak_heap_bytes=run.get("peak_heap_bytes"),
            spill_bytes=spill_bytes,
            derived_r=(
                float(run["reservation_ratio"])
                if run.get("enable_cbo") and run.get("reservation_ratio") is not None
                else None
            ),
            best_r_from_sweep=None,
            r_error=None,
            metadata={
                "mode": run.get("mode"),
                "pipeline_desc": plan_desc,
                "transform_ops": metadata.get("transform_ops"),
                "payload_bytes_per_row": metadata.get("payload_bytes_per_row"),
                "run_id": metadata.get("run_id"),
                "repetition": metadata.get("repetition"),
                "run_group_id": metadata.get("run_group_id"),
                "subtask_index": metadata.get("subtask_index"),
                "subtask_count": metadata.get("subtask_count"),
                "preset_name": metadata.get("preset_name"),
            },
        )
        normalized.append(record.to_dict())

    return normalized


def build_memory_profile_payload(
    runs: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_name": BENCHMARK_NAME,
        "experiment_type": ExperimentType.MEMORY_PROFILE.value,
        "generated_at": _utc_now(),
        "mode": "memory-profile",
        "config": config,
        "runs": runs,
        "records": normalize_memory_profile_records(runs, config),
    }
    payload["validation"] = _validation_block(payload)
    return payload


def validate_payload(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append(
            f"schema_version must be '{SCHEMA_VERSION}', got {payload.get('schema_version')!r}"
        )
    if payload.get("benchmark_name") != BENCHMARK_NAME:
        errors.append(
            f"benchmark_name must be '{BENCHMARK_NAME}', got {payload.get('benchmark_name')!r}"
        )
    if payload.get("experiment_type") not in {item.value for item in ExperimentType}:
        errors.append(
            f"experiment_type must be one of {[item.value for item in ExperimentType]}"
        )
    if not isinstance(payload.get("records"), list):
        errors.append("records must be a list")
        return errors

    for idx, record in enumerate(payload["records"]):
        record_errors = validate_record(record)
        errors.extend(f"records[{idx}]: {error}" for error in record_errors)

    return errors


def validate_record(record: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    for key in ("record_type", "workload_id", "plan_id"):
        if not isinstance(record.get(key), str) or not record[key]:
            errors.append(f"{key} must be a non-empty string")

    if record.get("record_type") not in {item.value for item in RecordType}:
        errors.append(
            f"record_type must be one of {[item.value for item in RecordType]}"
        )

    duration_s = record.get("duration_s")
    if not isinstance(duration_s, (int, float)) or duration_s < 0:
        errors.append("duration_s must be a non-negative number")

    cbo_enabled = record.get("cbo_enabled")
    if cbo_enabled is not None and not isinstance(cbo_enabled, bool):
        errors.append("cbo_enabled must be bool or None")

    for key in (
        "operator_count",
        "data_scale_rows",
        "data_scale_bytes",
        "peak_object_store_bytes",
        "peak_heap_bytes",
        "spill_bytes",
    ):
        value = record.get(key)
        if value is not None and (not isinstance(value, int) or value < 0):
            errors.append(f"{key} must be a non-negative integer or None")

    for key in ("derived_r", "best_r_from_sweep"):
        value = record.get(key)
        if value is not None and (
            not isinstance(value, (int, float)) or not 0.0 <= float(value) <= 1.0
        ):
            errors.append(f"{key} must be within [0.0, 1.0] or None")

    r_error = record.get("r_error")
    if r_error is not None and (
        not isinstance(r_error, (int, float)) or float(r_error) < 0.0
    ):
        errors.append("r_error must be a non-negative number or None")

    metadata = record.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        errors.append("metadata must be a dict")

    return errors
