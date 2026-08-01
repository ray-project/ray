#!/usr/bin/env python3

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
from locust_utils import LocustLoadTestConfig, LocustStage, run_locust_load_test
from serve_test_utils import save_test_results


logger = logging.getLogger(__file__)

DEFAULT_FULL_TEST_NUM_REPLICA = 1000
DEFAULT_FULL_TEST_TRIAL_LENGTH_S = 60
CLOUD = "serve_release_tests_cloud"


@click.command()
@click.option("--num-replicas", type=int, default=DEFAULT_FULL_TEST_NUM_REPLICA)
@click.option("--trial-length", type=int, default=DEFAULT_FULL_TEST_TRIAL_LENGTH_S)
@click.option("--output-path", "-o", type=str, default=None)
@click.option("--image-uri", type=str, default=None)
def main(
    num_replicas: Optional[int],
    trial_length: Optional[int],
    output_path: Optional[str],
    image_uri: Optional[str],
):
    noop_1k_application = {
        "name": "default",
        "import_path": "noop:app",
        "route_prefix": "/",
        "runtime_env": {"working_dir": "workloads"},
        "deployments": [
            {
                "name": "Noop",
                "ray_actor_options": {"resources": {"worker_resource": 0.01}},
                "autoscaling_config": {
                    "min_replicas": num_replicas,
                    "max_replicas": num_replicas,
                },
            }
        ],
    }
    compute_config = ComputeConfig(
        cloud=CLOUD,
        head_node=HeadNodeConfig(instance_type="m5.8xlarge"),
        worker_nodes=[
            WorkerNodeGroupConfig(
                instance_type="m5.xlarge",
                min_nodes=0,
                max_nodes=1000,
                resources={"worker_resource": 1},
            ),
        ],
    )
    stages = [
        LocustStage(
            duration_s=trial_length,
            users=50,
            spawn_rate=10,
        ),
        LocustStage(
            duration_s=trial_length,
            users=100,
            spawn_rate=20,
        ),
        LocustStage(
            duration_s=trial_length,
            users=500,
            spawn_rate=100,
        ),
        LocustStage(
            duration_s=trial_length,
            users=1000,
            spawn_rate=200,
        ),
    ]

    with start_service(
        service_name="replica-scalability",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=[noop_1k_application],
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        ray.init("auto")
        status = service.status(name=service_name, cloud=CLOUD)

        # Start the locust workload
        num_locust_workers = int(ray.available_resources()["CPU"]) - 1
        stats = run_locust_load_test(
            LocustLoadTestConfig(
                num_workers=num_locust_workers,
                host_url=status.query_url,
                auth_token=status.query_auth_token,
                data=None,
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
