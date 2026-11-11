import importlib.util
import sys
from pathlib import Path


def _load_harness_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = (
        repo_root
        / "python"
        / "ray"
        / "data"
        / "benchmarks"
        / "cbo_benchmark_harness.py"
    )
    spec = importlib.util.spec_from_file_location("cbo_benchmark_harness", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


harness = _load_harness_module()


def test_select_rule_benchmark_cases_rejects_unknown_rules():
    def runner(num_rows, repetitions):
        return harness.RuleBenchResult(
            rule_name="A",
            rule_category="RBO",
            optimization_goal="Throughput",
            pipeline_desc="x",
            baseline_label="b",
            optimized_label="o",
            baseline_time_s=1.0,
            optimized_time_s=0.5,
            improvement_pct=50.0,
        )

    cases = [
        harness.RuleBenchmarkCase("RuleA", "RBO", "Throughput", runner),
        harness.RuleBenchmarkCase("RuleB", "CBO", "Resource", runner),
    ]

    selected = harness.select_rule_benchmark_cases(cases, ["RuleB", "rulea"])
    assert [case.name for case in selected] == ["RuleB", "RuleA"]

    try:
        harness.select_rule_benchmark_cases(cases, ["MissingRule"])
    except ValueError as exc:
        assert "MissingRule" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown rule selection")


def test_build_end_to_end_execution_plan_expands_matrix():
    plan = harness.build_end_to_end_execution_plan(
        batch_rows=1000,
        streaming_rows=500,
        repetitions=2,
    )

    # 2 workloads x 2 settings x 2 duplicate slots x 2 blocks.
    assert len(plan) == 16
    assert plan[0].run_id == "batch_primary-rep1-cbo-off-a"
    assert plan[1].run_id == "batch_primary-rep1-cbo-on-a"
    assert plan[2].run_id == "batch_primary-rep1-cbo-off-b"
    assert plan[3].run_id == "batch_primary-rep1-cbo-on-b"
    assert plan[4].run_id == "streaming_primary-rep1-cbo-off-a"
    # The second block reverses the order so CBO never always runs second.
    assert plan[8].run_id == "batch_primary-rep2-cbo-on-a"
    assert plan[9].run_id == "batch_primary-rep2-cbo-off-a"
    assert plan[-1].run_id == "streaming_primary-rep2-cbo-off-b"


def test_build_end_to_end_execution_plan_pairs_every_setting():
    plan = harness.build_end_to_end_execution_plan(
        batch_rows=1000,
        streaming_rows=500,
        repetitions=2,
    )

    for block in (1, 2):
        for workload_id in ("batch_primary", "streaming_primary"):
            slots = {
                case.slot
                for case in plan
                if case.block == block and case.workload_id == workload_id
            }
            assert slots == {"off_a", "off_b", "on_a", "on_b"}


def test_execute_end_to_end_plan_uses_runner_registry():
    plan = [
        harness.WorkloadExecutionCase(
            run_id="batch_primary-rep1-cbo-off",
            workload_id="batch_primary",
            mode="batch",
            num_rows=123,
            cbo_enabled=False,
            repetition=1,
            runner_key="batch",
        ),
        harness.WorkloadExecutionCase(
            run_id="streaming_primary-rep1-cbo-on",
            workload_id="streaming_primary",
            mode="streaming",
            num_rows=456,
            cbo_enabled=True,
            repetition=1,
            runner_key="streaming",
        ),
    ]
    seen = []

    def batch_runner(num_rows, enable_cbo):
        seen.append(("batch", num_rows, enable_cbo))
        return harness.RunMetrics(mode="batch", enable_cbo=enable_cbo, duration_s=1.0)

    def streaming_runner(num_rows, enable_cbo):
        seen.append(("streaming", num_rows, enable_cbo))
        return harness.RunMetrics(
            mode="streaming",
            enable_cbo=enable_cbo,
            duration_s=2.0,
        )

    runs = harness.execute_end_to_end_plan(
        plan,
        {"batch": batch_runner, "streaming": streaming_runner},
        repetitions=1,
        log_fn=lambda _: None,
    )

    assert seen == [("batch", 123, False), ("streaming", 456, True)]
    assert [run.mode for run in runs] == ["batch", "streaming"]


def test_aggregate_runs_computes_expected_means():
    runs = [
        harness.RunMetrics(
            mode="batch",
            enable_cbo=False,
            duration_s=2.0,
            reservation_ratio=0.5,
            operator_count=4,
            global_bytes_spilled=10,
            dataset_bytes_spilled=20,
        ),
        harness.RunMetrics(
            mode="batch",
            enable_cbo=False,
            duration_s=4.0,
            reservation_ratio=0.5,
            operator_count=4,
            global_bytes_spilled=30,
            dataset_bytes_spilled=40,
        ),
        harness.RunMetrics(
            mode="streaming",
            enable_cbo=True,
            duration_s=3.0,
            reservation_ratio=0.3,
            operator_count=5,
            global_bytes_spilled=50,
            dataset_bytes_spilled=60,
            latency_p50_ms=10.0,
            latency_p95_ms=20.0,
            latency_p99_ms=30.0,
        ),
        harness.RunMetrics(
            mode="streaming",
            enable_cbo=True,
            duration_s=5.0,
            reservation_ratio=0.4,
            operator_count=5,
            global_bytes_spilled=70,
            dataset_bytes_spilled=80,
            latency_p50_ms=14.0,
            latency_p95_ms=24.0,
            latency_p99_ms=34.0,
        ),
    ]

    aggregates = harness.aggregate_runs(runs)

    assert aggregates["batch-cbo-False"]["duration_s_mean"] == 3.0
    assert aggregates["batch-cbo-False"]["global_bytes_spilled_mean"] == 20
    assert aggregates["streaming-cbo-True"]["reservation_ratio_mean"] == 0.35
    assert aggregates["streaming-cbo-True"]["latency_p95_ms_mean"] == 22.0
    # Runs without interleaving metadata carry no paired comparison.
    assert "batch-paired" not in aggregates


def _slotted_run(mode, block, slot, duration_s):
    metrics = harness.RunMetrics(
        mode=mode,
        enable_cbo=slot.startswith("on"),
        duration_s=duration_s,
    )
    metrics.metadata.update({"block": block, "slot": slot})
    return metrics


def test_paired_analysis_reports_delta_ci_and_noise_floor():
    runs = []
    for block in (1, 2):
        runs.extend(
            [
                _slotted_run("batch", block, "off_a", 10.0),
                _slotted_run("batch", block, "off_b", 10.2),
                _slotted_run("batch", block, "on_a", 8.0),
                _slotted_run("batch", block, "on_b", 8.1),
            ]
        )

    paired = harness.aggregate_runs(runs)["batch-paired"]
    assert paired["blocks"] == 2
    assert paired["delta_pct_mean"] < 0
    assert 0 < paired["noise_floor_pct"] < 3
    ci_low, ci_high = paired["delta_pct_ci95"]
    assert ci_high < 0
    assert paired["significant"] is True


def test_paired_analysis_does_not_treat_one_outlier_block_as_a_result():
    # Shape of an observed 20k-row batch run: one block is ~34% slower while the
    # other two are within a few percent, and same-setting duplicates vary by
    # ~10%. The mean interval excludes zero but the median does not clear the
    # noise floor, so the effect is not reported as real.
    blocks = [
        (1.2411, 1.1534, 1.1338, 1.3186),
        (1.1564, 1.1682, 1.2056, 1.2517),
        (1.2920, 1.1329, 1.9334, 1.3185),
    ]
    runs = []
    for block, (off_a, off_b, on_a, on_b) in enumerate(blocks, start=1):
        runs.extend(
            [
                _slotted_run("batch", block, "off_a", off_a),
                _slotted_run("batch", block, "off_b", off_b),
                _slotted_run("batch", block, "on_a", on_a),
                _slotted_run("batch", block, "on_b", on_b),
            ]
        )

    paired = harness.aggregate_runs(runs)["batch-paired"]
    assert paired["delta_pct_ci95"][0] > 0
    assert paired["delta_pct_median"] < paired["noise_floor_pct"]
    assert paired["significant"] is False


def test_paired_analysis_ignores_incomplete_blocks():
    runs = [
        _slotted_run("batch", 1, "off_a", 10.0),
        _slotted_run("batch", 1, "off_b", 10.1),
        _slotted_run("batch", 1, "on_a", 8.0),
    ]
    assert "batch-paired" not in harness.aggregate_runs(runs)


def test_bootstrap_mean_ci95_is_deterministic_and_brackets_the_mean():
    values = [1.0, 2.0, 3.0, 4.0]
    first = harness.bootstrap_mean_ci95(values)
    second = harness.bootstrap_mean_ci95(values)
    assert first == second
    assert first[0] <= 2.5 <= first[1]
    assert harness.bootstrap_mean_ci95([]) is None


def test_build_memory_profile_plan_expands_cbo_pairs():
    plan = harness.build_memory_profile_plan(
        row_counts=[1000],
        operator_counts=[2, 4],
        repetitions=2,
        payload_bytes_per_row=1024,
    )

    assert len(plan) == 8
    assert plan[0].run_id == "chain_rows1000_ops2-rep1-cbo-off"
    assert plan[1].run_id == "chain_rows1000_ops2-rep1-cbo-on"
    # The second repetition reverses the order to remove the fixed-runner bias.
    assert plan[4].run_id == "chain_rows1000_ops2-rep2-cbo-on"
    assert plan[5].run_id == "chain_rows1000_ops2-rep2-cbo-off"
    assert plan[-1].run_id == "chain_rows1000_ops4-rep2-cbo-off"


def test_format_r_value_matrix_table_contains_error_column():
    results = [
        harness.RValueMatrixResult(
            workload_id="chain_rows1000_ops4",
            num_rows=1000,
            transform_ops=4,
            derived_ratio=0.33,
            derived_duration_s=1.2,
            best_ratio=0.3,
            best_duration_s=1.0,
            default_ratio=0.5,
            default_duration_s=1.5,
            r_error=0.03,
            operator_count=6,
        )
    ]

    table = harness.format_r_value_matrix_table(results)
    assert "R Error" in table
    assert "chain_rows1000_ops4" in table


def test_format_memory_profile_table_uses_peak_memory_metrics():
    runs = [
        harness.RunMetrics(
            workload_id="chain_rows1000_ops4",
            mode="batch",
            enable_cbo=True,
            duration_s=1.0,
            reservation_ratio=0.3,
            total_input_rows=1000,
            peak_heap_bytes=8 * 1024 * 1024,
            peak_object_store_bytes=16 * 1024 * 1024,
            metadata={"transform_ops": 4},
        )
    ]

    table = harness.format_memory_profile_table(runs)
    assert "Peak Heap MB" in table
    assert "16.0" in table


def test_resolve_memory_profile_preset_builds_10gb_10m_defaults():
    preset = harness.resolve_memory_profile_preset("10gb_10m", operator_counts=[2, 4])

    assert preset["preset_name"] == "10gb_10m"
    assert preset["row_counts"] == [10_000_000]
    assert preset["operator_counts"] == [2, 4]
    assert preset["target_total_bytes"] == 10 * 1024**3
    assert preset["payload_bytes_per_row"] == 1066


def test_shard_parameterized_workloads_splits_evenly_and_preserves_order():
    workloads = harness.build_parameterized_workloads(
        row_counts=[100, 200, 300],
        operator_counts=[2, 4],
        payload_bytes_per_row=0,
    )

    shard0 = harness.shard_parameterized_workloads(
        workloads,
        subtask_index=0,
        subtask_count=2,
    )
    shard1 = harness.shard_parameterized_workloads(
        workloads,
        subtask_index=1,
        subtask_count=2,
    )

    assert [item.workload_id for item in shard0] == [
        "chain_rows100_ops2",
        "chain_rows200_ops2",
        "chain_rows300_ops2",
    ]
    assert [item.workload_id for item in shard1] == [
        "chain_rows100_ops4",
        "chain_rows200_ops4",
        "chain_rows300_ops4",
    ]


def test_shard_memory_profile_plan_keeps_cbo_pairs_together():
    plan = harness.build_memory_profile_plan(
        row_counts=[1000, 2000],
        operator_counts=[2],
        repetitions=2,
        payload_bytes_per_row=64,
    )

    shard0 = harness.shard_memory_profile_plan(plan, subtask_index=0, subtask_count=2)
    shard1 = harness.shard_memory_profile_plan(plan, subtask_index=1, subtask_count=2)

    shard0_ids = [case.run_id for case in shard0]
    shard1_ids = [case.run_id for case in shard1]
    assert len(shard0_ids) == 4
    assert len(shard1_ids) == 4
    assert set(shard0_ids).isdisjoint(shard1_ids)
    assert set(shard0_ids) | set(shard1_ids) == {case.run_id for case in plan}
    for run_id in (
        "chain_rows1000_ops2-rep1-cbo",
        "chain_rows1000_ops2-rep2-cbo",
        "chain_rows2000_ops2-rep1-cbo",
        "chain_rows2000_ops2-rep2-cbo",
    ):
        in_shard0 = [item for item in shard0_ids if item.startswith(run_id)]
        in_shard1 = [item for item in shard1_ids if item.startswith(run_id)]
        assert len(in_shard0) in (0, 2)
        assert len(in_shard1) in (0, 2)


def test_build_subtask_manifest_exposes_workload_ids():
    workloads = harness.build_parameterized_workloads(
        row_counts=[1000],
        operator_counts=[2, 4],
        payload_bytes_per_row=0,
    )

    manifest = harness.build_parameterized_subtask_manifest(
        workloads,
        subtask_count=2,
        experiment_type="r_value_matrix",
    )

    assert manifest[0].experiment_type == "r_value_matrix"
    assert manifest[0].item_ids == ("chain_rows1000_ops2",)
    assert manifest[1].item_ids == ("chain_rows1000_ops4",)
