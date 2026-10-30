import importlib.util
import json
import sys
from pathlib import Path


def _load_module(module_name: str, relative_path: str):
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


harness = _load_module(
    "cbo_benchmark_harness",
    "python/ray/data/benchmarks/cbo_benchmark_harness.py",
)
schema = _load_module(
    "cbo_benchmark_schema",
    "python/ray/data/benchmarks/cbo_benchmark_schema.py",
)
postprocess = _load_module(
    "cbo_benchmark_postprocess",
    "python/ray/data/benchmarks/cbo_benchmark_postprocess.py",
)


def test_merge_matrix_payloads_builds_runtime_feedback_report(tmp_path):
    payload0 = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows1000_ops2",
                "num_rows": 1000,
                "transform_ops": 2,
                "derived_ratio": 0.50,
                "derived_duration_s": 1.4,
                "best_ratio": 0.30,
                "best_duration_s": 1.0,
                "default_ratio": 0.50,
                "default_duration_s": 1.4,
                "r_error": 0.20,
                "operator_count": 4,
                "subtask_index": 0,
                "subtask_count": 2,
            }
        ],
        {
            "row_scales": [1000, 2000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5, 0.7],
            "repetitions": 1,
            "subtask_index": 0,
            "subtask_count": 2,
            "selected_workload_ids": ["chain_rows1000_ops2"],
        },
    )
    payload1 = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows2000_ops2",
                "num_rows": 2000,
                "transform_ops": 2,
                "derived_ratio": 0.34,
                "derived_duration_s": 1.8,
                "best_ratio": 0.30,
                "best_duration_s": 1.6,
                "default_ratio": 0.50,
                "default_duration_s": 2.2,
                "r_error": 0.04,
                "operator_count": 4,
                "subtask_index": 1,
                "subtask_count": 2,
            }
        ],
        {
            "row_scales": [1000, 2000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5, 0.7],
            "repetitions": 1,
            "subtask_index": 1,
            "subtask_count": 2,
            "selected_workload_ids": ["chain_rows2000_ops2"],
        },
    )
    manifest = {
        "r_value_matrix": [
            {
                "experiment_type": "r_value_matrix",
                "subtask_index": 0,
                "subtask_count": 2,
                "item_ids": ["chain_rows1000_ops2"],
            },
            {
                "experiment_type": "r_value_matrix",
                "subtask_index": 1,
                "subtask_count": 2,
                "item_ids": ["chain_rows2000_ops2"],
            },
        ]
    }

    payload0_path = tmp_path / "matrix_shard0.json"
    payload1_path = tmp_path / "matrix_shard1.json"
    manifest_path = tmp_path / "matrix_manifest.json"
    payload0_path.write_text(json.dumps(payload0), encoding="utf-8")
    payload1_path.write_text(json.dumps(payload1), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    merged = postprocess.merge_benchmark_payload_files(
        [str(payload0_path), str(payload1_path)],
        manifest_path=str(manifest_path),
    )

    assert merged["validation"]["is_valid"] is True
    assert len(merged["results"]) == 2
    assert merged["merge_summary"]["coverage"]["is_complete"] is True
    assert merged["merge_summary"]["coverage"]["observed_subtask_indices"] == [0, 1]
    assert merged["runtime_feedback"]["workloads"][0]["iterations_to_converge"] == 2

    report = postprocess.render_markdown_report(
        merged,
        title="Phase4 Matrix Report",
    )
    assert "# Phase4 Matrix Report" in report
    assert "Runtime Feedback Convergence" in report
    assert "chain_rows1000_ops2" in report


def test_merge_memory_profile_payloads_reports_missing_shards(tmp_path):
    payload = schema.build_memory_profile_payload(
        [
            {
                "workload_id": "chain_rows1000_ops4",
                "plan_description": "range->filter->payload(64B)->map_batches*x4->repartition->sort",
                "mode": "batch",
                "enable_cbo": False,
                "duration_s": 2.0,
                "reservation_ratio": 0.50,
                "operator_count": 6,
                "global_bytes_spilled": 2048,
                "dataset_bytes_spilled": 1024,
                "total_input_rows": 1000,
                "data_scale_bytes": 64_000,
                "peak_object_store_bytes": 8192,
                "peak_heap_bytes": 4096,
                "metadata": {
                    "transform_ops": 4,
                    "payload_bytes_per_row": 64,
                    "run_id": "chain_rows1000_ops4-rep1-cbo-off",
                    "repetition": 1,
                    "run_group_id": "chain_rows1000_ops4:rep1",
                    "subtask_index": 0,
                    "subtask_count": 2,
                    "preset_name": "10gb_10m",
                },
            },
            {
                "workload_id": "chain_rows1000_ops4",
                "plan_description": "range->filter->payload(64B)->map_batches*x4->repartition->sort",
                "mode": "batch",
                "enable_cbo": True,
                "duration_s": 1.6,
                "reservation_ratio": 0.33,
                "operator_count": 6,
                "global_bytes_spilled": 1024,
                "dataset_bytes_spilled": 512,
                "total_input_rows": 1000,
                "data_scale_bytes": 64_000,
                "peak_object_store_bytes": 6144,
                "peak_heap_bytes": 3072,
                "metadata": {
                    "transform_ops": 4,
                    "payload_bytes_per_row": 64,
                    "run_id": "chain_rows1000_ops4-rep1-cbo-on",
                    "repetition": 1,
                    "run_group_id": "chain_rows1000_ops4:rep1",
                    "subtask_index": 0,
                    "subtask_count": 2,
                    "preset_name": "10gb_10m",
                },
            },
        ],
        {
            "row_scales": [1000],
            "operator_counts": [4],
            "payload_bytes_per_row": 64,
            "memory_poll_interval_s": 0.1,
            "repetitions": 1,
            "profile_preset": "10gb_10m",
            "target_total_bytes": 10 * 1024**3,
            "target_rows": 10_000_000,
            "subtask_index": 0,
            "subtask_count": 2,
            "selected_run_ids": [
                "chain_rows1000_ops4-rep1-cbo-off",
                "chain_rows1000_ops4-rep1-cbo-on",
            ],
        },
    )
    manifest = {
        "memory_profile": [
            {
                "experiment_type": "memory_profile",
                "subtask_index": 0,
                "subtask_count": 2,
                "item_ids": ["chain_rows1000_ops4-rep1-cbo-off", "chain_rows1000_ops4-rep1-cbo-on"],
            },
            {
                "experiment_type": "memory_profile",
                "subtask_index": 1,
                "subtask_count": 2,
                "item_ids": ["chain_rows2000_ops4-rep1-cbo-off", "chain_rows2000_ops4-rep1-cbo-on"],
            },
        ]
    }

    payload_path = tmp_path / "memory_shard0.json"
    manifest_path = tmp_path / "memory_manifest.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    merged = postprocess.merge_benchmark_payload_files(
        [str(payload_path)],
        manifest_path=str(manifest_path),
    )

    coverage = merged["merge_summary"]["coverage"]
    assert coverage["is_complete"] is False
    assert coverage["missing_subtask_indices"] == [1]

    report = postprocess.render_markdown_report(merged)
    assert "Memory Profile Summary" in report
    assert "| Missing shards | [1] |" in report
    assert "chain_rows1000_ops4:rep1" in report


def test_merge_rejects_duplicate_matrix_workloads(tmp_path):
    payload = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows1000_ops2",
                "num_rows": 1000,
                "transform_ops": 2,
                "derived_ratio": 0.33,
                "derived_duration_s": 1.2,
                "best_ratio": 0.30,
                "best_duration_s": 1.0,
                "default_ratio": 0.50,
                "default_duration_s": 1.4,
                "r_error": 0.03,
                "operator_count": 4,
                "subtask_index": 0,
                "subtask_count": 1,
            }
        ],
        {
            "row_scales": [1000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5],
            "repetitions": 1,
            "subtask_index": 0,
            "subtask_count": 1,
            "selected_workload_ids": ["chain_rows1000_ops2"],
        },
    )
    path0 = tmp_path / "dup0.json"
    path1 = tmp_path / "dup1.json"
    path0.write_text(json.dumps(payload), encoding="utf-8")
    path1.write_text(json.dumps(payload), encoding="utf-8")

    try:
        postprocess.merge_benchmark_payload_files([str(path0), str(path1)])
    except ValueError as exc:
        assert "Duplicate benchmark entry" in str(exc)
    else:
        raise AssertionError("Expected duplicate matrix workload merge to fail")


def test_write_publication_bundle_writes_all_benchmark_artifacts(tmp_path):
    merged = postprocess.merge_benchmark_payloads(
        [
            schema.build_r_value_matrix_payload(
                [
                    {
                        "workload_id": "chain_rows1000_ops2",
                        "num_rows": 1000,
                        "transform_ops": 2,
                        "derived_ratio": 0.50,
                        "derived_duration_s": 1.4,
                        "best_ratio": 0.30,
                        "best_duration_s": 1.0,
                        "default_ratio": 0.50,
                        "default_duration_s": 1.4,
                        "r_error": 0.20,
                        "operator_count": 4,
                        "subtask_index": 0,
                        "subtask_count": 1,
                    }
                ],
                {
                    "row_scales": [1000],
                    "operator_counts": [2],
                    "sweep_ratios": [0.3, 0.5],
                    "repetitions": 1,
                    "subtask_index": 0,
                    "subtask_count": 1,
                    "selected_workload_ids": ["chain_rows1000_ops2"],
                },
            )
        ]
    )

    bundle_manifest = postprocess.write_publication_bundle(
        str(tmp_path / "bundle"),
        merged,
        title="benchmark Bundle",
        environment_metadata={
            "ray_version": "2.54.0-dev",
            "hardware": "32 cores / 128GiB",
            "command": "python3 cbo_benchmark.py --merge-results ...",
        },
        input_result_paths=["/tmp/shard0.json"],
        manifest_path="/tmp/manifest.json",
    )

    artifacts = bundle_manifest["artifacts"]
    for path in artifacts.values():
        assert Path(path).exists()

    report = Path(artifacts["report_markdown"]).read_text(encoding="utf-8")
    execution_log = Path(artifacts["execution_log_markdown"]).read_text(
        encoding="utf-8"
    )
    community_qa = Path(artifacts["community_qa_markdown"]).read_text(
        encoding="utf-8"
    )
    environment_metadata = json.loads(
        Path(artifacts["environment_metadata_json"]).read_text(encoding="utf-8")
    )
    environment_template = json.loads(
        Path(artifacts["environment_metadata_template_json"]).read_text(
            encoding="utf-8"
        )
    )

    assert "# benchmark Bundle" in report
    assert "Execution Summary" in execution_log
    assert "Environment Metadata" in execution_log
    assert "对社区反馈的直接响应" in community_qa
    assert "reservation_ratio 是否可信" in community_qa
    assert environment_metadata["ray_version"] == "2.54.0-dev"
    assert environment_template["hardware"] == ""
