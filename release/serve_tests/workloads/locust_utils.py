from dataclasses import asdict, dataclass
from multiprocessing import Process, Queue
from itertools import chain
import json
import logging
from typing import Any

import ray
from ray.serve._private.benchmarks.locust_utils import (
    run_locust_master,
    run_locust_worker,
    LocustStage,
    LocustTestResults,
    LocustStages,
)


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


@dataclass
class LocustLoadTestConfig:
    num_workers: int
    host_url: str
    auth_token: str
    data: Any
    stages: LocustStages
    wait_for_workers_timeout_s: float = 600


@ray.remote(num_cpus=1)
class LocustProcess:
    def __init__(
        self,
        worker_type: str,
        host_url: str,
        token: str,
        **kwargs,
    ):
        self.q = Queue()
        self.p = Process(
            target=run_locust_master if worker_type == "master" else run_locust_worker,
            kwargs={"q": self.q, **kwargs},
        )

    def run(self):
        self.p.start()
        print(f"Started {self.p.name} ({self.p.pid})")
        self.p.join()
        return self.q.get()


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
    errors = sorted(chain(*ray.get(worker_refs)), key=lambda e: e.start_time_s)

    # If there were any requests that failed, raise error.
    if stats.num_failures > 0:
        errors_json = [asdict(err) for err in errors]
        raise RuntimeError(
            f"There were failed requests: {json.dumps(errors_json, indent=4)}"
        )

    return stats


if __name__ == "__main__":
    ray.init(address="auto")
    results = run_locust_load_test(
        LocustLoadTestConfig(
            num_workers=10,
            host_url="https://services-canary-pinger-aws-zugs7.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/info",
            auth_token="v9M8jb3tBbHOGoWrg7X1fCwF8wYn7gqZR5VZ1_h4t50",
            data=None,
            stages=LocustStages(
                stages=[LocustStage(duration_s=10, users=10, spawn_rate=1)]
            ),
        )
    )
    print(results)
