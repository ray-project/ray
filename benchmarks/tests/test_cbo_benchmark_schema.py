import importlib.util
import sys
from pathlib import Path


def _load_schema_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = (
        repo_root / "python" / "ray" / "data" / "benchmarks" / "cbo_benchmark_schema.py"
    )
    spec = importlib.util.spec_from_file_location("cbo_benchmark_schema", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


schema = _load_schema_module()


def test_build_per_rule_payload_adds_normalized_records():
    results = [
        {
            "rule_name": "DeriveReservationRatio",
            "rule_category": "CBO",
            "optimization_goal": "Resource",
            "pipeline_desc": "filter->map->sort @ sweep R",
            "baseline_label": "R=0.5",
            "optimized_label": "R=0.3",
            "baseline_time_s": 0.63,
            "optimized_time_s": 0.297,
            "improvement_pct": 52.9,
            "sweep_detail": [
                {"ratio": 0.5, "time_s": 0.63, "is_default": True},
                {"ratio": 0.3, "time_s": 0.297, "is_default": False},
            ],
        }
    ]

    payload = schema.build_per_rule_payload(
        results, {"num_rows": 100000, "repetitions": 2}
    )

    assert payload["schema_version"] == schema.SCHEMA_VERSION
    assert payload["benchmark_name"] == schema.BENCHMARK_NAME
    assert payload["validation"]["is_valid"] is True
    assert len(payload["records"]) == 1
    record = payload["records"][0]
    assert record["record_type"] == "per_rule_result"
    assert record["data_scale_rows"] == 100000
    assert record["best_r_from_sweep"] == 0.3
    assert record["derived_r"] is None
    assert record["metadata"]["derivation_mode"] == "parameter_sweep_only"


def test_build_end_to_end_payload_adds_cbo_run_records():
    runs = [
        {
            "mode": "batch",
            "enable_cbo": False,
            "duration_s": 1.2,
            "reservation_ratio": 0.5,
            "operator_count": 4,
            "global_bytes_spilled": 0,
            "dataset_bytes_spilled": 0,
            "streaming_schedule_s": 0.0,
            "total_input_rows": 1000,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_p99_ms": 0.0,
            "latency_mean_ms": 0.0,
        },
        {
            "mode": "batch",
            "enable_cbo": True,
            "duration_s": 0.9,
            "reservation_ratio": 0.33,
            "operator_count": 4,
            "global_bytes_spilled": 64,
            "dataset_bytes_spilled": 32,
            "streaming_schedule_s": 0.0,
            "total_input_rows": 1000,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_p99_ms": 0.0,
            "latency_mean_ms": 0.0,
        },
    ]

    payload = schema.build_end_to_end_payload(
        runs,
        {"batch-cbo-False": {"duration_s_mean": 1.2}},
        {"batch_rows": 1000, "streaming_rows": 500, "repetitions": 1},
    )

    assert payload["validation"]["is_valid"] is True
    assert len(payload["records"]) == 2
    off_record, on_record = payload["records"]
    assert off_record["cbo_enabled"] is False
    assert off_record["derived_r"] is None
    assert on_record["cbo_enabled"] is True
    assert on_record["derived_r"] == 0.33
    assert on_record["spill_bytes"] == 64


def test_validate_record_rejects_invalid_ranges():
    errors = schema.validate_record(
        {
            "record_type": "execution_run",
            "workload_id": "w1",
            "plan_id": "p1",
            "cbo_enabled": "yes",
            "duration_s": -1.0,
            "derived_r": 1.5,
            "best_r_from_sweep": -0.1,
            "r_error": -2.0,
            "metadata": [],
        }
    )

    assert errors
    assert any("cbo_enabled" in error for error in errors)
    assert any("duration_s" in error for error in errors)
    assert any("derived_r" in error for error in errors)


def test_validate_payload_rejects_unknown_experiment_type():
    payload = {
        "schema_version": schema.SCHEMA_VERSION,
        "benchmark_name": schema.BENCHMARK_NAME,
        "experiment_type": "unknown",
        "records": [],
    }

    errors = schema.validate_payload(payload)
    assert errors
    assert any("experiment_type" in error for error in errors)


def test_build_r_value_matrix_payload_records_r_error_and_best_r():
    payload = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows1000_ops4",
                "num_rows": 1000,
                "transform_ops": 4,
                "derived_ratio": 0.33,
                "derived_duration_s": 1.1,
                "best_ratio": 0.3,
                "best_duration_s": 1.0,
                "default_ratio": 0.5,
                "default_duration_s": 1.4,
                "r_error": 0.03,
                "operator_count": 6,
                "data_scale_bytes": 8192,
                "peak_object_store_bytes": 4096,
                "peak_heap_bytes": 2048,
                "spill_bytes": 512,
                "pipeline_desc": "range->filter->map_batches*x4->repartition->sort",
                "sweep_detail": [{"ratio": 0.3, "time_s": 1.0, "is_default": False}],
            }
        ],
        {"row_scales": [1000], "operator_counts": [4]},
    )

    assert payload["experiment_type"] == "r_value_matrix"
    assert payload["validation"]["is_valid"] is True
    record = payload["records"][0]
    assert record["derived_r"] == 0.33
    assert record["best_r_from_sweep"] == 0.3
    assert record["r_error"] == 0.03
    assert record["peak_heap_bytes"] == 2048


def test_build_memory_profile_payload_populates_peak_memory_fields():
    payload = schema.build_memory_profile_payload(
        [
            {
                "workload_id": "chain_rows1000_ops4",
                "plan_description": "range->filter->payload(2048B)->map_batches*x4->repartition->sort",
                "mode": "batch",
                "enable_cbo": True,
                "duration_s": 1.5,
                "reservation_ratio": 0.34,
                "operator_count": 6,
                "global_bytes_spilled": 2048,
                "dataset_bytes_spilled": 1024,
                "total_input_rows": 1000,
                "data_scale_bytes": 2_048_000,
                "peak_object_store_bytes": 8192,
                "peak_heap_bytes": 4096,
                "metadata": {
                    "transform_ops": 4,
                    "payload_bytes_per_row": 2048,
                    "run_id": "chain_rows1000_ops4-rep1-cbo-on",
                    "repetition": 1,
                    "run_group_id": "chain_rows1000_ops4:rep1",
                },
            }
        ],
        {"payload_bytes_per_row": 2048},
    )

    assert payload["experiment_type"] == "memory_profile"
    assert payload["validation"]["is_valid"] is True
    record = payload["records"][0]
    assert record["peak_object_store_bytes"] == 8192
    assert record["peak_heap_bytes"] == 4096
    assert record["spill_bytes"] == 2048
    assert record["metadata"]["payload_bytes_per_row"] == 2048
    assert record["metadata"]["run_id"] == "chain_rows1000_ops4-rep1-cbo-on"
    assert record["metadata"]["repetition"] == 1


def test_phase3_payloads_preserve_subtask_metadata():
    matrix_payload = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows1000_ops4",
                "num_rows": 1000,
                "transform_ops": 4,
                "derived_ratio": 0.33,
                "derived_duration_s": 1.1,
                "best_ratio": 0.3,
                "best_duration_s": 1.0,
                "default_ratio": 0.5,
                "default_duration_s": 1.4,
                "r_error": 0.03,
                "operator_count": 6,
                "subtask_index": 1,
                "subtask_count": 4,
            }
        ],
        {"subtask_index": 1, "subtask_count": 4},
    )
    memory_payload = schema.build_memory_profile_payload(
        [
            {
                "workload_id": "chain_rows1000_ops4",
                "mode": "batch",
                "enable_cbo": False,
                "duration_s": 1.2,
                "operator_count": 5,
                "total_input_rows": 1000,
                "metadata": {
                    "transform_ops": 4,
                    "payload_bytes_per_row": 64,
                    "subtask_index": 2,
                    "subtask_count": 4,
                    "preset_name": "10gb_10m",
                },
            }
        ],
        {"subtask_index": 2, "subtask_count": 4},
    )

    assert matrix_payload["records"][0]["metadata"]["subtask_index"] == 1
    assert matrix_payload["records"][0]["metadata"]["subtask_count"] == 4
    assert memory_payload["records"][0]["metadata"]["subtask_index"] == 2
    assert memory_payload["records"][0]["metadata"]["preset_name"] == "10gb_10m"
