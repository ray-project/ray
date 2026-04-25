"""Curated list of Ray Data tests to gate scheduling-loop optimizations.

Each entry is a pytest nodeid (file path, optionally with ``::classname``
or ``::function_name`` suffix). Paths are relative to the artifact root.

The list covers test files that exercise code the agent may modify
(scheduling loop, resource manager, backpressure policies, operators,
issue detection, ranker, progress manager, autoscaler, stats) and tests
that indirectly depend on those code paths (e2e dataset iteration, limits,
object GC, schema export).

Intentionally excluded: format-specific datasource tests (CSV/Parquet/etc.),
cloud integration tests, pure-data aggregations, shuffle-internal tests,
and logical optimization tests. These do not exercise scheduling.
"""

FAST_TEST_NODES = [
    # Fast subset (~5-6 min) for mid-session correctness pre-checks.
    # Curated to catch the classes of regressions we've observed in real
    # agent runs, not just generic breakage. Each file maps to a distinct
    # failure mode:
    #
    # - streaming_executor: scheduling loop structure, inner-dispatch
    #   ordering, process_completed_tasks plumbing.
    # - backpressure_policies: per-policy decisions (resource budget, queue
    #   size, output capacity). Catches regressions to backpressure
    #   semantics independent of the resource allocator.
    # - ranker: operator-selection ordering. Cheap and directly exercised
    #   by any change to select_operator_to_run.
    # - reservation_based_resource_allocator: catches budget-math
    #   regressions such as the _create_raw/safe_round rounding bug
    #   (opt-002 class). Includes test_basic and
    #   test_reservation_accounts_for_completed_ops_complex_graph.
    # - resource_manager: catches changes to update_usages, completed-ops
    #   accounting, and the ExecutionResources arithmetic that the hot
    #   path depends on.
    # - dataset_iter: catches scheduling-determinism regressions via
    #   test_iter_batches_local_shuffle (seeded shuffle requires
    #   deterministic block arrival order — timing-sensitive optimizations
    #   like reducing ray.wait timeout break this).
    # - actor_pool_map_operator: catches dispatch-path changes in the
    #   actor-pool branch (opt-018 class: cached dispatch options).
    #
    # Intentionally excluded from fast:
    # - issue_detection_manager: not relevant to scheduler correctness.
    # - backpressure_e2e: slow (full pipeline) and needs the state API.
    # - streaming_integration: long-running integration checks.
    #
    # Note: `tests/unit/test_resource_manager.py` is also included because
    # it shares a basename with `tests/test_resource_manager.py`. The gate's
    # nodeid normalizer collapses both to `test_resource_manager::*`, so if
    # only one of the two files is run the other's tests appear as
    # "missing" in current, and the gate reports them as false-positive
    # regressions. Running both files in fast keeps keys populated.
    "python/ray/data/tests/test_streaming_executor.py",
    "python/ray/data/tests/test_backpressure_policies.py",
    "python/ray/data/tests/test_ranker.py",
    "python/ray/data/tests/test_reservation_based_resource_allocator.py",
    "python/ray/data/tests/test_resource_manager.py",
    "python/ray/data/tests/unit/test_resource_manager.py",
    "python/ray/data/tests/test_dataset_iter.py",
    "python/ray/data/tests/test_actor_pool_map_operator.py",
]


# Tests that either (a) flake probabilistically on this hardware due to
# timing sensitivity that is not under Ray Data's control (e.g.
# ``test_spilled_stats`` asserts on a backpressure-time string that depends on
# per-host timing precision), or (b) encode user-facing determinism contracts
# whose failure mode is probabilistic rather than deterministic (shuffle
# determinism). Both classes must be retried at full depth on BOTH the
# baseline and the gate so that the comparison is symmetric; a single-run
# pass on the baseline against a 10-run-retry on the gate is a
# retry-policy artifact, not a real regression.
#
# Normalized nodeid form: ``<test_file_basename_without_ext>::<function_name>``
# (matches the ``_parse_junit`` output).
KNOWN_FLAKY_TESTS = {
    "test_stats::test_spilled_stats[True]",
    "test_stats::test_spilled_stats[False]",
    "test_consumption::test_read_write_local_node_ray_client",
    "test_dataset_iter::test_iter_batches_local_shuffle[pandas]",
    "test_dataset_iter::test_iter_batches_local_shuffle[arrow]",
}


TEST_NODES = [
    # === Core scheduling & execution ===
    "python/ray/data/tests/test_streaming_executor.py",
    "python/ray/data/tests/test_streaming_integration.py",

    # === Backpressure ===
    "python/ray/data/tests/test_backpressure_e2e.py",
    "python/ray/data/tests/test_backpressure_policies.py",
    "python/ray/data/tests/test_downstream_capacity_backpressure_policy.py",

    # === Resource management ===
    "python/ray/data/tests/test_resource_manager.py",
    "python/ray/data/tests/test_reservation_based_resource_allocator.py",
    "python/ray/data/tests/unit/test_resource_manager.py",

    # === Bundle queue ===
    "python/ray/data/tests/test_bundle_queue.py",
    "python/ray/data/tests/unit/test_fifo_bundle_queue.py",
    "python/ray/data/tests/unit/test_reordering_bundle_queue.py",

    # === Scheduling support systems ===
    "python/ray/data/tests/test_progress_manager.py",
    "python/ray/data/tests/test_ranker.py",
    "python/ray/data/tests/test_autoscaler.py",
    "python/ray/data/tests/test_issue_detection_manager.py",

    # === Metrics & stats ===
    "python/ray/data/tests/test_stats.py",
    "python/ray/data/tests/test_op_runtime_metrics.py",

    # === Operators ===
    "python/ray/data/tests/test_map_operator.py",
    "python/ray/data/tests/test_limit_operator.py",
    "python/ray/data/tests/test_actor_pool_map_operator.py",
    # test_splitblocks catches a subtle M3 (cached dispatch options) regression
    # where the cached `options(name=...)` captures the operator name before
    # SetReadParallelismRule renames the op to `Read*->SplitBlocks(k)`. Without
    # this file in the gate the stale-name bug surfaces only as a Ray core
    # task-metric miscount inside SplitBlocks' assertions.
    "python/ray/data/tests/test_splitblocks.py",

    # === Dataset-level e2e (indirect coverage) ===
    "python/ray/data/tests/test_consumption.py",
    "python/ray/data/tests/test_dataset_iter.py",
    "python/ray/data/tests/test_dataset_limits.py",
    "python/ray/data/tests/test_object_gc.py",
    "python/ray/data/tests/test_operator_schema_export.py",
]
