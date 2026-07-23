# Profile Analysis Scripts

CLI tools for analyzing profiling output after a benchmark run. These
operate on standard formats (speedscope JSON, collapsed stacks) and
don't depend on the profiling module -- they can be used standalone.

## Scripts

### `analyze_pyspy_profile.py`

Analyzes speedscope JSON profiles (from py-spy or converted perf data).
Supports leaf (self-time) analysis, inclusive time, caller/callee stacks,
call graphs, category grouping, and full stack dumps.

```bash
# Top 30 leaf functions for the StreamingExecutor thread:
./analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --top 30

# List all threads and their CPU time:
./analyze_pyspy_profile.py pyspy_driver.speedscope.json --list-threads

# Caller stacks for a specific function:
./analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --callers readinto

# Call graph (callers + self-time + callees):
./analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --call-graph detect
```

### `collapsed_to_speedscope.py`

Converts collapsed stack format (output of `perf.generate_collapsed_stacks()`)
to speedscope JSON, so it can be loaded in [speedscope.app](https://www.speedscope.app)
or analyzed with `analyze_pyspy_profile.py`.

```bash
./collapsed_to_speedscope.py perf_gcs_collapsed.txt -o gcs.speedscope.json
```

### `download_job_output.sh`

Downloads Anyscale job logs and S3 telemetry for a completed job into a
local directory named after the job ID.

```bash
./download_job_output.sh prodjob_abc123 image-embedding-jsonl/prodjob_abc123
# Creates prodjob_abc123/ with logs and telemetry files
```

Respects `PROFILING_S3_BUCKET` env var (same default as `telemetry.py`).

### `analyze_perf_profiles.sh`

Batch-converts all `perf_*_collapsed.txt` files in the current directory to
speedscope JSON and generates a thread summary. Run from a directory
containing perf collapsed stack files (downloaded from S3 telemetry).

```bash
cd /path/to/downloaded/telemetry
analyze_perf_profiles.sh
# Produces: *.speedscope.json files + perf_thread_summary.txt
```

## Typical workflow

1. Run a benchmark with `PERF_PROFILING_ENABLED=1` and/or `PYSPY_ENABLED=1`
2. Download job output:
   ```bash
   ./download_job_output.sh prodjob_abc123 image-embedding-jsonl/prodjob_abc123
   cd prodjob_abc123
   ```
3. Analyze py-spy output:
   ```bash
   ./analyze_pyspy_profile.py pyspy_driver.speedscope.json --list-threads
   ./analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --top 30
   ```
4. Convert and analyze perf output:
   ```bash
   ./analyze_perf_profiles.sh
   ./analyze_pyspy_profile.py perf_gcs.speedscope.json --list-threads
   ```
