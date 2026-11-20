#!/usr/bin/env python3
"""
Benchmark test.
"""

import click
import json
import logging
from typing import Optional

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
CLOUD = "serve_release_tests_cloud"
# Worker instance families, in order of preference. All are x86 with the same
# vCPU and memory per size; the later ones are fallbacks for when the earlier
# ones have no capacity.
WORKER_FAMILIES = ["m7i", "m6i", "m5", "m6a"]
MAX_WORKERS = 10


@click.command()
@click.option("--output-path", "-o", type=str, default=None)
@click.option("--image-uri", type=str, default=None)
def main(output_path: Optional[str], image_uri: Optional[str]):
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
        cloud=CLOUD,
        head_node=HeadNodeConfig(instance_type="m7i.8xlarge"),
        # One logical pool of 32-CPU workers. Anyscale tries the groups in
        # `group_order`, so a later group is used only when the ones before it
        # can't be launched. It may try an earlier group in another zone first.
        worker_nodes=[
            WorkerNodeGroupConfig(
                name=f"worker-{family}",
                instance_type=f"{family}.8xlarge",
                min_nodes=0,
                max_nodes=MAX_WORKERS,
            )
            for family in WORKER_FAMILIES
        ],
        # Keeps the pool at MAX_WORKERS in total rather than per group. The
        # head is not counted: Anyscale gives it 0 logical CPUs by default.
        max_resources={"CPU": 32 * MAX_WORKERS},
        flags={
            "instance_ranking_strategy": [
                {
                    "ranker_type": "custom_group_order",
                    "ranker_config": {
                        "group_order": [f"worker-{f}" for f in WORKER_FAMILIES]
                    },
                }
            ]
        },
    )
    stages = [
        LocustStage(duration_s=1200, users=10, spawn_rate=1),
        LocustStage(duration_s=1200, users=50, spawn_rate=10),
        LocustStage(duration_s=1200, users=100, spawn_rate=10),
    ]

    with start_service(
        "autoscaling-load-test",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=[resnet_application],
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        ray.init(address="auto")
        status = service.status(name=service_name, cloud=CLOUD)

        # Start the locust workload
        num_locust_workers = int(ray.available_resources()["CPU"]) - 1
        stats: LocustTestResults = run_locust_load_test(
            LocustLoadTestConfig(
                num_workers=num_locust_workers,
                host_url=status.query_url,
                auth_token=status.query_auth_token,
                data={"uri": URI},
                stages=stages,
            )
        )
        results_per_stage = [
            [
                {
                    "perf_metric_name": f"stage_{i+1}_p50_latency",
                    "perf_metric_value": stats.stats_in_stages[i].p50_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": f"stage_{i+1}_p90_latency",
                    "perf_metric_value": stats.stats_in_stages[i].p90_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": f"stage_{i+1}_p99_latency",
                    "perf_metric_value": stats.stats_in_stages[i].p99_latency,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": f"stage_{i+1}_rps",
                    "perf_metric_value": stats.stats_in_stages[i].rps,
                    "perf_metric_type": "THROUGHPUT",
                },
            ]
            for i in range(len(stages))
        ]
        results = {
            "total_requests": stats.total_requests,
            "service_id": status.id,
            "perf_metrics": sum(
                results_per_stage,
                [
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
            ),
        }
        logger.info(f"Stats history: {json.dumps(stats.history, indent=4)}")
        logger.info(f"Final aggregated metrics: {json.dumps(results, indent=4)}")
        save_test_results(results, output_path=output_path)


if __name__ == "__main__":
    main()
