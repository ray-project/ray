import json
import logging
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from typing import Any, List

import ray
from ray.serve._private.benchmarks.locust_utils import (
    LocustStage,
    LocustTestResults,
    PerformanceStats,
)

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


@dataclass
class LocustLoadTestConfig:
    num_workers: int
    host_url: str
    auth_token: str
    data: Any
    stages: List[LocustStage]
    wait_for_workers_timeout_s: float = 600


@ray.remote(num_cpus=1)
class LocustProcess:
    def __init__(
        self,
        worker_type: str,
        host_url: str,
        token: str,
        expected_num_workers: int = None,
        stages: List[LocustStage] = None,
        wait_for_workers_timeout_s: float = None,
        data: Any = None,
        master_address: str = None,
    ):
        self.worker_type = worker_type
        self.host_url = host_url
        self.token = token
        self.expected_num_workers = expected_num_workers
        self.stages = stages
        self.wait_for_workers_timeout_s = wait_for_workers_timeout_s
        self.data = data
        self.master_address = master_address

    def run(self):
        # Create a temporary file for results
        import tempfile

        results_file = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        )
        results_file.close()

        # Prepare the subprocess script
        if self.worker_type == "master":
            script = f"""
import sys
import json
from ray.serve._private.benchmarks.locust_utils import run_locust_master, run_locust_worker, LocustStage

stages = json.loads(sys.argv[1])
stages = [LocustStage(**stage) for stage in stages]
results = run_locust_master(
    host_url="{self.host_url}",
    token="{self.token}",
    expected_num_workers={self.expected_num_workers},
    stages=stages,
    wait_for_workers_timeout_s={self.wait_for_workers_timeout_s}
)

with open("{results_file.name}", 'w') as f:
    json.dump(results, f)
"""
            stages = json.dumps([asdict(stage) for stage in self.stages])
            cmd_args = [sys.executable, "-c", script, stages]
        else:
            script = f"""
import sys
import json
from ray.serve._private.benchmarks.locust_utils import run_locust_master, run_locust_worker, LocustStage

data = sys.argv[1]
results = run_locust_worker(
    master_address="{self.master_address}",
    host_url="{self.host_url}",
    token="{self.token}",
    data=data,
)
"""
            data = json.dumps(self.data)
            cmd_args = [sys.executable, "-c", script, data]

        # Start the Locust process
        self.process = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        print(f"Started {self.worker_type} subprocess ({self.process.pid})")

        try:
            # Wait for the process to complete first
            for line in self.process.stdout:  # yields as the child prints
                sys.stdout.write(line)  # stream to our stdout

            return_code = self.process.wait()
            if return_code != 0:
                # Clean up the results file on error
                try:
                    os.unlink(results_file.name)
                except OSError:
                    pass
                raise RuntimeError(f"Subprocess failed with return code {return_code}.")

            # Read the result from the results file
            with open(results_file.name, "r") as f:
                result_data = f.read()

            if result_data:
                result_data = json.loads(result_data)
                stats_in_stages = [
                    PerformanceStats(**stage)
                    for stage in result_data.pop("stats_in_stages")
                ]
                result = LocustTestResults(
                    **result_data, stats_in_stages=stats_in_stages
                )
                return result
        finally:
            os.unlink(results_file.name)


def run_locust_load_test(config: LocustLoadTestConfig) -> LocustTestResults:
    """Runs a Locust load test against a service.

    Returns:
        Performance results (e.g. throughput and latency) from the test.
    Raises:
        RuntimeError if any requests failed during the load test.
    """

    logger.info(f"Spawning {config.num_workers} Locust worker Ray tasks.")
    master_address = ray.util.get_node_ip_address()
    worker_refs = []

    # Start Locust workers
    for i in range(config.num_workers):
        locust_worker = LocustProcess.options(name=f"LocustWorker-{i}").remote(
            worker_type="worker",
            host_url=config.host_url,
            token=config.auth_token,
            master_address=master_address,
            data=config.data,
        )
        worker_refs.append(locust_worker.run.remote())
        print(f"Started worker {i}")

    # Start Locust master
    master_worker = LocustProcess.options(name="LocustMaster").remote(
        worker_type="master",
        host_url=config.host_url,
        token=config.auth_token,
        expected_num_workers=config.num_workers,
        stages=config.stages,
        wait_for_workers_timeout_s=config.wait_for_workers_timeout_s,
    )
    master_ref = master_worker.run.remote()

    # Collect results and metrics
    stats: LocustTestResults = ray.get(master_ref)
    ray.get(worker_refs)
    return stats


def generate_multi_endpoint_locust_script(
    warmup_endpoints,
    ramp_endpoints,
    ramp_profile,
    warmup_sec,
    warmup_spawn_rate=2,
    ramp_base_users=9,
    ramp_spawn_rate=20,
):
    """Generate a locust script for multi-endpoint weighted load testing."""
    warmup_json = json.dumps(warmup_endpoints)
    ramp_json = json.dumps(ramp_endpoints)
    profile_json = json.dumps(ramp_profile)

    return f"""
import json, os
from locust import HttpUser, task, between, LoadTestShape

PAYLOAD = {{"x": 1}}
WARMUP_SEC = {warmup_sec}

def _make_user(name, route, w=1):
    class U(HttpUser):
        host = os.environ["LOCUST_HOST"]
        wait_time = between(0.001, 0.002)
        weight = w
        def on_start(self):
            token = os.environ.get("LOCUST_AUTH_TOKEN", "")
            if token:
                self.client.headers["Authorization"] = f"Bearer {{token}}"
        @task
        def predict(self):
            self.client.post(route, json=PAYLOAD, name=name, timeout=0.5)
    U.__name__ = name.replace("-", "_") + "_User"
    return U

WARMUP_CLASSES = [_make_user(n, r, w) for n, r, w in {warmup_json}]
RAMP_CLASSES = [_make_user(n, r, w) for n, r, w in {ramp_json}]
for _i, _cls in enumerate(WARMUP_CLASSES + RAMP_CLASSES):
    globals()[f"_locust_user_{{_i}}"] = _cls

_RAMP_PROFILE = {profile_json}

def _interpolate(t, profile):
    if t <= 0:
        return float(profile[0][1])
    for i, (pt, pv) in enumerate(profile):
        if t <= pt:
            if i == 0:
                return float(pv)
            prev_t, prev_v = profile[i - 1]
            frac = (t - prev_t) / (pt - prev_t)
            return prev_v + frac * (pv - prev_v)
    return 0.0

class MultiEndpointLoadShape(LoadTestShape):
    def tick(self):
        run_time = self.get_run_time()
        if run_time < WARMUP_SEC:
            return (len(WARMUP_CLASSES), {warmup_spawn_rate}, WARMUP_CLASSES)
        ramp_time = run_time - WARMUP_SEC
        ramp_users = _interpolate(ramp_time, _RAMP_PROFILE)
        total_users = {ramp_base_users} + int(round(ramp_users))
        if ramp_users <= 0 and ramp_time >= _RAMP_PROFILE[-1][0]:
            return None
        return (total_users, {ramp_spawn_rate}, RAMP_CLASSES)
"""


def run_locust_subprocess(
    host_url, auth_token, script_content, num_processes=16, stages=None
):
    """Run a locust script as a subprocess and return parsed CSV stats."""
    import tempfile

    csv_dir = tempfile.mkdtemp()
    csv_prefix = os.path.join(csv_dir, "output")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = f.name

    env = os.environ.copy()
    env["LOCUST_HOST"] = host_url
    env["LOCUST_AUTH_TOKEN"] = auth_token or ""

    try:
        logger.info(
            f"Running locust subprocess: {num_processes} processes, "
            f"target {host_url}"
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "locust",
                "-f",
                script_path,
                "--headless",
                "--processes",
                str(num_processes),
                "--csv",
                csv_prefix,
            ],
            check=True,
            env=env,
        )
    finally:
        os.unlink(script_path)

    stats = parse_locust_csv(csv_prefix)

    if stages:
        stats["stages"] = parse_locust_stats_history(csv_prefix, stages)

    for suffix in (
        "_stats.csv",
        "_failures.csv",
        "_stats_history.csv",
        "_exceptions.csv",
    ):
        path = csv_prefix + suffix
        if os.path.exists(path):
            os.unlink(path)
    os.rmdir(csv_dir)

    return stats


def parse_locust_csv(csv_prefix):
    import csv

    per_endpoint = {}
    aggregated = {}
    with open(csv_prefix + "_stats.csv", newline="") as f:
        for row in csv.DictReader(f):
            entry = {
                "request_count": int(row["Request Count"]),
                "failure_count": int(row["Failure Count"]),
                "p50_latency": float(row["50%"]),
                "p90_latency": float(row["90%"]),
                "p99_latency": float(row["99%"]),
                "avg_latency": float(row["Average Response Time"]),
                "rps": float(row["Requests/s"]),
            }
            if row["Name"] == "Aggregated":
                aggregated = entry
            else:
                per_endpoint[row["Name"]] = entry

    total_failures = 0
    with open(csv_prefix + "_failures.csv", newline="") as f:
        for row in csv.DictReader(f):
            total_failures += int(row["Occurrences"])

    return {
        "total_requests": aggregated.get("request_count", 0),
        "num_failures": total_failures,
        "p50_latency": aggregated.get("p50_latency", 0),
        "p90_latency": aggregated.get("p90_latency", 0),
        "p99_latency": aggregated.get("p99_latency", 0),
        "avg_latency": aggregated.get("avg_latency", 0),
        "avg_rps": aggregated.get("rps", 0),
        "per_endpoint": per_endpoint,
    }


def parse_locust_stats_history(csv_prefix, stages):
    """Parse _stats_history.csv and compute per-stage metrics."""
    import csv

    # Read all Aggregated rows
    rows = []
    with open(csv_prefix + "_stats_history.csv", newline="") as f:
        for row in csv.DictReader(f):
            if row["Name"] == "Aggregated":
                rows.append(row)

    if not rows:
        return {
            name: {"avg_rps": 0, "p99_latency": 0, "avg_users": 0}
            for name, _, _ in stages
        }

    # Find effective start: first row with Requests/s > 0
    start_ts = int(rows[0]["Timestamp"])
    for row in rows:
        rps = float(row["Requests/s"])
        if rps > 0:
            start_ts = int(row["Timestamp"])
            break

    result = {}
    for stage_name, start_s, end_s in stages:
        rps_values = []
        p99_values = []
        user_values = []

        for row in rows:
            offset = int(row["Timestamp"]) - start_ts
            if offset < start_s or offset >= end_s:
                continue

            rps_values.append(float(row["Requests/s"]))
            user_values.append(float(row["User Count"]))

            p99_str = row["99%"]
            if p99_str != "N/A":
                p99_values.append(float(p99_str))

        result[stage_name] = {
            "avg_rps": sum(rps_values) / len(rps_values) if rps_values else 0,
            "p99_latency": max(p99_values) if p99_values else 0,
            "avg_users": (sum(user_values) / len(user_values) if user_values else 0),
        }

    return result


if __name__ == "__main__":
    ray.init(address="auto")
    results = run_locust_load_test(
        LocustLoadTestConfig(
            num_workers=9,
            host_url="https://services-canary-pinger-aws-zugs7.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/info",
            auth_token="v9M8jb3tBbHOGoWrg7X1fCwF8wYn7gqZR5VZ1_h4t50",
            data=None,
            stages=[LocustStage(duration_s=10, users=10, spawn_rate=1)],
        )
    )
    print(results)
