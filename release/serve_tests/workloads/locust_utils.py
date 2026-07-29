from dataclasses import asdict, dataclass
import os
import sys
import subprocess
import json
import logging
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
