#!/usr/bin/env python3
"""
Benchmark test.
"""

import json
import logging
from itertools import chain

import click
from anyscale import AnyscaleSDK, service
from anyscale_service_utils import start_service
from locust_utils import LocustMaster, LocustWorker
from serve_test_utils import save_test_results
from tqdm import tqdm

import ray


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


IMAGE_URI = "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"  # noqa


@click.command()
def main():
    sdk = AnyscaleSDK()
    service_name = f"autoscaling-locust-test-{ray.__commit__[:7]}"
    resnet_application = {
        "name": "default",
        "import_path": "resnet_50:app",
        "route_prefix": "/",
        "runtime_env": {"working_dir": "workloads"},
        "deployments": [{"name": "Model", "num_replicas": "auto"}],
    }
    locust_stages = [
        {"duration": 1200, "users": 10, "spawn_rate": 1},
        {"duration": 2400, "users": 50, "spawn_rate": 10},
        {"duration": 3600, "users": 100, "spawn_rate": 10},
    ]

    with start_service(
        sdk=sdk,
        service_name=service_name,
        compute_config="cindy-all32-min0worker:1",
        applications=[resnet_application],
    ):
        ray.init(address="auto")
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
                data={"uri": IMAGE_URI},
            )
            worker_refs.append(locust_worker.run.remote())

        # Start Locust master
        master_worker = LocustMaster.remote(
            host_url=status.query_url,
            token=status.query_auth_token,
            expected_num_workers=num_locust_workers,
            stages=locust_stages,
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
