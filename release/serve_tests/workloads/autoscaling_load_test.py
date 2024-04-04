#!/usr/bin/env python3
"""
Benchmark test.
"""

import logging

from anyscale import service
from anyscale.compute_config.models import (
    ComputeConfig,
    HeadNodeConfig,
    WorkerNodeGroupConfig,
)
import ray

from anyscale_service_utils import start_service
from locust_utils import (
    LocustLoadTestConfig,
    LocustStage,
    LocustTestResults,
    run_locust_load_test,
)
from serve_test_utils import save_test_results


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


URI = "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"


def main():
    resnet_application = {
        "import_path": "resnet_50:app",
        "deployments": [
            {
                "name": "Model",
                "num_replicas": "auto",
            }
        ],
    }
    compute_config = ComputeConfig(
        cloud="anyscale_v2_default_cloud",
        head_node=HeadNodeConfig(instance_type="m5.8xlarge"),
        worker_nodes=[
            WorkerNodeGroupConfig(
                instance_type="m5.8xlarge", min_nodes=0, max_nodes=10
            ),
        ],
    )

    with start_service(
        "autoscaling-locust-test",
        compute_config=compute_config,
        applications=[resnet_application],
    ) as service_name:
        ray.init(address="auto")
        status = service.status(name=service_name)

        # Start the locust workload
        num_locust_workers = int(ray.available_resources()["CPU"]) - 1
        stats: LocustTestResults = run_locust_load_test(
            LocustLoadTestConfig(
                num_workers=num_locust_workers,
                host_url=status.query_url,
                auth_token=status.query_auth_token,
                data={"uri": URI},
                stages=[
                    LocustStage(duration_s=1200, users=10, spawn_rate=1),
                    LocustStage(duration_s=2400, users=50, spawn_rate=10),
                    LocustStage(duration_s=3600, users=100, spawn_rate=10),
                ],
            )
        )
        results = {
            "total_requests": stats.total_requests,
            "history": stats.history,
            "perf_metrics": [
                {
                    "perf_metric_name": "avg_latency",
                    "perf_metric_value": stats.avg_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "p50_latency",
                    "perf_metric_value": stats.p50_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "p90_latency",
                    "perf_metric_value": stats.p90_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "p99_latency",
                    "perf_metric_value": stats.p99_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "avg_rps",
                    "perf_metric_value": stats.avg_rps,
                    "perf_metric_type": "THROUGHPUT",
                },
            ],
        }
        logger.info(f"Final aggregated metrics: {results}")
        save_test_results(results)


if __name__ == "__main__":
    main()
