#!/usr/bin/env python3

import click
from itertools import chain
import json
import logging
from tqdm import tqdm
from typing import Optional

from anyscale import AnyscaleSDK, service
import ray

from serve_test_utils import save_test_results
from anyscale_service_utils import start_service
from locust_utils import LocustWorker, LocustMaster


logger = logging.getLogger(__file__)

DEFAULT_FULL_TEST_NUM_REPLICA = 1000
DEFAULT_FULL_TEST_TRIAL_LENGTH_S = 300


@click.command()
@click.option("--num-replicas", type=int)
@click.option("--trial-length", type=str)
def main(num_replicas: Optional[int], trial_length: Optional[str]):
    sdk = AnyscaleSDK()
    num_replicas = num_replicas or DEFAULT_FULL_TEST_NUM_REPLICA
    trial_length = trial_length or DEFAULT_FULL_TEST_TRIAL_LENGTH_S
    service_name = f"scalability-1k-replicas-{ray.__commit__[:7]}"
    noop_1k_application = {
        "name": "default",
        "import_path": "noop:app",
        "route_prefix": "/",
        "runtime_env": {"working_dir": "workloads"},
        "deployments": [
            {
                "name": "Noop",
                "num_replicas": num_replicas,
                "ray_actor_options": {"resources": {"worker_resource": 0.01}},
            }
        ],
    }

    stages = [
        {"duration": trial_length, "users": num_replicas // 10, "spawn_rate": 10},
        {"duration": 2 * trial_length, "users": num_replicas, "spawn_rate": 10},
        {"duration": 3 * trial_length, "users": num_replicas * 2, "spawn_rate": 10},
    ]

    with start_service(
        sdk=sdk,
        service_name=service_name,
        compute_config="cindy-all32-min0worker:2",
        applications=[noop_1k_application],
    ):
        ray.init("auto")
        status = service.status(name=service_name)

        # Start the locust workload
        num_locust_workers = int(ray.available_resources()["CPU"]) - 1
        logger.info(f"Spawning {num_locust_workers} Locust worker Ray tasks.")
        master_address = ray.util.get_node_ip_address()
        worker_refs = []

        # Start Locust workers
        for _ in tqdm(range(num_locust_workers)):
            locust_worker = LocustWorker.remote(
                host_url=status.query_url,
                token=status.query_auth_token,
                master_address=master_address,
            )
            worker_refs.append(locust_worker.run.remote())

        # Start Locust master
        master_worker = LocustMaster.remote(
            host_url=status.query_url,
            token=status.query_auth_token,
            expected_num_workers=num_locust_workers,
            stages=stages,
        )
        master_ref = master_worker.run.remote()

        # Collect results and metrics
        stats = ray.get(master_ref)
        errors = sorted(chain(*ray.get(worker_refs)), key=lambda e: e["start_time"])

        if stats.get("num_failures") > 0:
            raise RuntimeError(
                f"There were failed requests: {json.dumps(errors, indent=4)}"
            )
        else:
            results = {
                "total_requests": stats["total_requests"],
                "history": stats["history"],
                "perf_metrics": [
                    {
                        "perf_metric_name": "avg_latency",
                        "perf_metric_value": stats["avg_latency"],
                        "perf_metric_type": "LATENCY",
                    },
                    {
                        "perf_metric_name": "p50_latency",
                        "perf_metric_value": stats["p50_latency"],
                        "perf_metric_type": "LATENCY",
                    },
                    {
                        "perf_metric_name": "p90_latency",
                        "perf_metric_value": stats["p90_latency"],
                        "perf_metric_type": "LATENCY",
                    },
                    {
                        "perf_metric_name": "p99_latency",
                        "perf_metric_value": stats["p99_latency"],
                        "perf_metric_type": "LATENCY",
                    },
                    {
                        "perf_metric_name": "avg_rps",
                        "perf_metric_value": stats["avg_rps"],
                        "perf_metric_type": "THROUGHPUT",
                    },
                ],
            }
            logger.info(f"Final aggregated metrics: {results}")
            save_test_results(results)


if __name__ == "__main__":
    main()
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
