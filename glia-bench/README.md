# glia-bench

Reproducible benchmark + correctness harness for evaluating the scheduler
optimizations on this branch against stock Ray 2.55.0.

**See [`OPTIMIZATION_REPORT.md`](OPTIMIZATION_REPORT.md)** for:

- What was changed and why (§2)
- Methodology (§3)
- Results on two hardware profiles (§4)
- Install + run instructions (§5)

## Directory map

- **`OPTIMIZATION_REPORT.md`** — the canonical report. Start here.
- `benchmark_scheduler.py` — runs one of four Ray Data workloads and emits
  wall time, throughput, driver CPU per wall-second, scheduler efficiency,
  and a SHA-256 of sorted output rows.
- `workload_config.json` — parameters for each workload.
- `run_optimization_bench.sh` — perf driver. Walks the pristine and M6
  trees, writes 40 runs (2 configs × 4 workloads × 5 reps) to
  `results/optimization_perf.jsonl`.
- `run_optimization_gate.sh` — correctness driver. Records a pristine
  baseline, runs the curated test gate against M6, writes the diff to
  `results/optimization_gate_{baseline,m6}.json`.
- `aggregate_perf.py` — aggregates a perf JSONL into the §4.1 table
  (mean/stdev/Δ%/Welch's-t + output-hash check).
- `run_gated_tests.py` — gate harness called by `run_optimization_gate.sh`.
  Tests in `test_list.KNOWN_FLAKY_TESTS` are always retried 10× on both
  sides so flaky-test comparisons are symmetric.
- `test_list.py` — curated test file list + `KNOWN_FLAKY_TESTS` set.
- `stage_ray_artifacts.sh` — symlinks ray-2.55.0's compiled artifacts
  (`_raylet.so`, protobuf-generated code, dashboard build, etc.) into a
  source tree so `SKIP_BAZEL_BUILD=1 pip install -e python/` succeeds.
- `_tree_switch.sh` — sourced helper. Swaps the editable-install MAPPING
  between the pristine and M6 `python/ray` trees so the same venv can run
  either side without reinstall.
- `generate_parquet.py` — generates the small parquet fixtures that some
  workloads reference.
- `run_bench.sh` — lightweight single-tree wrapper for dev-loop iteration
  (not used by the report).
- `results/` — raw per-run JSONL + gate JSON artifacts for both hardware
  profiles.
