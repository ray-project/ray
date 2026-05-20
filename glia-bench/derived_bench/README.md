# derived_bench/

Per-test benchmark portfolios derived from upstream Ray Data release tests
using the `test_to_benchmark` skill (see
[claude-plugins](https://github.com/Glia-AI/claude-plugins/tree/add-test-to-benchmark-skill/experimental/skills/test_to_benchmark)).

Each subdirectory `<testname>_bench/` is a Phase 4 deliverable for one
upstream test from `release/nightly_tests/dataset/`. The benchmarks inside
engage specific operating regimes the upstream test enters, calibrated to
run cheaply on a single-node devpod (24 vCPU / 256 GB RAM) while still
exercising the regime at intensity.

Each `<testname>_bench/` directory contains:
- `BENCHMARKS.md` — compact per-benchmark summary (regime targeted, run
  command, correctness/perf evaluation guidance)
- One subdir per benchmark with `run.py` and supporting files

## Running against different code trees

The benchmarks `import ray` and rely on the editable install's `MAPPING`
to determine which source tree is loaded. To switch trees without
reinstalling, use `glia-bench/_tree_switch.sh` (one level up from this
directory).

By design, these benchmarks are **calibrated against pristine
ray-2.55.0** — engineers iterating on optimization candidates run them
with `MAPPING` set to a candidate tree and compare against the pristine
baseline.

## How portfolios are added

Run the `test_to_benchmark` skill with the target upstream test as input
and `derived_bench/<testname>_bench/` as the output location for Phase 4.
The skill produces the BENCHMARKS.md plus per-benchmark code; commit the
result.

## Current portfolios

(Populated as portfolios are produced.)
