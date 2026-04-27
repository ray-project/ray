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

data = json.loads(sys.argv[1])
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


@ray.remote(num_cpus=1)
class MultiEndpointLocustProcess:
    """Ray actor that runs a multi-endpoint locust master or worker subprocess."""

    def __init__(
        self,
        worker_type: str,
        host_url: str,
        auth_token: str,
        warmup_endpoints: List[tuple],
        ramp_endpoints: List[tuple],
        master_address: str = None,
        expected_num_workers: int = None,
        wait_for_workers_timeout_s: float = 600,
        ramp_profile: List[tuple] = None,
        warmup_sec: int = 40,
        warmup_spawn_rate: int = 2,
        ramp_base_users: int = 9,
        ramp_spawn_rate: int = 20,
        stages: List[tuple] = None,
    ):
        self.worker_type = worker_type
        self.host_url = host_url
        self.auth_token = auth_token
        self.warmup_endpoints = warmup_endpoints
        self.ramp_endpoints = ramp_endpoints
        self.master_address = master_address
        self.expected_num_workers = expected_num_workers
        self.wait_for_workers_timeout_s = wait_for_workers_timeout_s
        self.ramp_profile = ramp_profile
        self.warmup_sec = warmup_sec
        self.warmup_spawn_rate = warmup_spawn_rate
        self.ramp_base_users = ramp_base_users
        self.ramp_spawn_rate = ramp_spawn_rate
        self.stages = stages

    def run(self) -> Any:
        import tempfile

        results_file = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        )
        results_file.close()

        if self.worker_type == "master":
            warmup_json = json.dumps(self.warmup_endpoints)
            ramp_json = json.dumps(self.ramp_endpoints)
            ramp_profile_json = json.dumps(self.ramp_profile)
            stages_json = json.dumps(self.stages)
            script = f"""
import sys, json
from ray.serve._private.benchmarks.locust_utils import run_multi_endpoint_master

warmup_eps = json.loads(sys.argv[1])
ramp_eps = json.loads(sys.argv[2])
ramp_profile = [(float(t), float(v)) for t, v in json.loads(sys.argv[3])]
stages = json.loads(sys.argv[4])
stages = [(n, s, e) for n, s, e in stages] if stages else None

results = run_multi_endpoint_master(
    host_url="{self.host_url}",
    token="{self.auth_token or ''}",
    expected_num_workers={self.expected_num_workers},
    warmup_endpoints=[tuple(e) for e in warmup_eps],
    ramp_endpoints=[tuple(e) for e in ramp_eps],
    ramp_profile=ramp_profile,
    warmup_sec={self.warmup_sec},
    wait_for_workers_timeout_s={self.wait_for_workers_timeout_s},
    payload={{"x": 1}},
    warmup_spawn_rate={self.warmup_spawn_rate},
    ramp_base_users={self.ramp_base_users},
    ramp_spawn_rate={self.ramp_spawn_rate},
    stages=stages,
)

with open("{results_file.name}", "w") as f:
    json.dump(results, f)
"""
            cmd_args = [
                sys.executable,
                "-c",
                script,
                warmup_json,
                ramp_json,
                ramp_profile_json,
                stages_json,
            ]

        else:  # worker
            warmup_json = json.dumps(self.warmup_endpoints)
            ramp_json = json.dumps(self.ramp_endpoints)
            script = f"""
import sys, json
from ray.serve._private.benchmarks.locust_utils import run_multi_endpoint_worker

warmup_eps = [tuple(e) for e in json.loads(sys.argv[1])]
ramp_eps = [tuple(e) for e in json.loads(sys.argv[2])]

run_multi_endpoint_worker(
    master_address="{self.master_address}",
    host_url="{self.host_url}",
    token="{self.auth_token or ''}",
    warmup_endpoints=warmup_eps,
    ramp_endpoints=ramp_eps,
    payload={{"x": 1}},
)
"""
            cmd_args = [
                sys.executable,
                "-c",
                script,
                warmup_json,
                ramp_json,
            ]

        self.process = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        print(
            f"Started multi-endpoint {self.worker_type} subprocess "
            f"({self.process.pid})"
        )

        try:
            for line in self.process.stdout:
                sys.stdout.write(line)

            return_code = self.process.wait()
            if return_code != 0:
                try:
                    os.unlink(results_file.name)
                except OSError:
                    pass
                raise RuntimeError(f"Subprocess failed with return code {return_code}.")

            with open(results_file.name, "r") as f:
                result_data = f.read()

            if result_data:
                return json.loads(result_data)
        finally:
            try:
                os.unlink(results_file.name)
            except OSError:
                pass


def run_multi_endpoint_load_test(
    num_workers: int,
    host_url: str,
    auth_token: str,
    warmup_endpoints: List[tuple],
    ramp_endpoints: List[tuple],
    ramp_profile: List[tuple],
    warmup_sec: int,
    stages: List[tuple] = None,
    warmup_spawn_rate: int = 2,
    ramp_base_users: int = 9,
    ramp_spawn_rate: int = 20,
    wait_for_workers_timeout_s: float = 600,
) -> dict:
    """Run a multi-endpoint locust load test using distributed master/worker pattern."""

    logger.info(f"Spawning {num_workers} multi-endpoint locust workers.")
    master_address = ray.util.get_node_ip_address()
    worker_refs = []

    for i in range(num_workers):
        worker = MultiEndpointLocustProcess.options(
            name=f"LocustMultiWorker-{i}"
        ).remote(
            worker_type="worker",
            host_url=host_url,
            auth_token=auth_token,
            warmup_endpoints=warmup_endpoints,
            ramp_endpoints=ramp_endpoints,
            master_address=master_address,
        )
        worker_refs.append(worker.run.remote())
        logger.info(f"Started multi-endpoint worker: {i} of {num_workers} total")

    master = MultiEndpointLocustProcess.options(name="LocustMultiMaster").remote(
        worker_type="master",
        host_url=host_url,
        auth_token=auth_token,
        warmup_endpoints=warmup_endpoints,
        ramp_endpoints=ramp_endpoints,
        expected_num_workers=num_workers,
        wait_for_workers_timeout_s=wait_for_workers_timeout_s,
        ramp_profile=ramp_profile,
        warmup_sec=warmup_sec,
        warmup_spawn_rate=warmup_spawn_rate,
        ramp_base_users=ramp_base_users,
        ramp_spawn_rate=ramp_spawn_rate,
        stages=stages,
    )
    master_ref = master.run.remote()

    stats = ray.get(master_ref)
    ray.get(worker_refs)
    return stats


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
