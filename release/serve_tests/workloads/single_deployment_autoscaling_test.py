"""
Single-deployment autoscaling release test.
"""

import json
import logging
from typing import Any, Dict, Optional

import click
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

CLOUD = "serve_release_tests_cloud"

APPLICATION: Dict[str, Any] = {
    "name": "app_6",
    "import_path": "simulated_ml_inference:app_6",
    "route_prefix": "/",
    "deployments": [
        {
            "name": "SimulatedMLInference_app_6",
            "num_replicas": "auto",
            "max_queued_requests": -1,
            "graceful_shutdown_wait_loop_s": 2.0,
            "graceful_shutdown_timeout_s": 20.0,
            "health_check_period_s": 10.0,
            "health_check_timeout_s": 30.0,
            "ray_actor_options": {"num_cpus": 2.0},
            "max_ongoing_requests": 1,
            "autoscaling_config": {
                "metrics_interval_s": 10.0,
                "look_back_period_s": 30.0,
                "smoothing_factor": 1.0,
                "downscale_delay_s": 30.0,
                "upscale_delay_s": 5.0,
                "min_replicas": 50,
                "max_replicas": 2000,
                "target_ongoing_requests": 0.1,
                "upscaling_factor": 10.0,
            },
        }
    ],
}

# Approximates the multi-deployment test's ramp profile with discrete stages.
# Total ~14 min, peak 109 users.
STAGES = [
    LocustStage(duration_s=40, users=8, spawn_rate=2),  # warmup
    LocustStage(duration_s=300, users=58, spawn_rate=20),  # ramp midpoint
    LocustStage(duration_s=10, users=109, spawn_rate=20),  # sharp jump
    LocustStage(duration_s=490, users=109, spawn_rate=20),  # sustain at peak
]


def build_results(stats: LocustTestResults, service_id: str) -> Dict[str, Any]:
    results_per_stage = [
        [
            {
                "perf_metric_name": f"stage_{i + 1}_p50_latency",
                "perf_metric_value": stats.stats_in_stages[i].p50_latency,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": f"stage_{i + 1}_p90_latency",
                "perf_metric_value": stats.stats_in_stages[i].p90_latency,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": f"stage_{i + 1}_p99_latency",
                "perf_metric_value": stats.stats_in_stages[i].p99_latency,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": f"stage_{i + 1}_rps",
                "perf_metric_value": stats.stats_in_stages[i].rps,
                "perf_metric_type": "THROUGHPUT",
            },
        ]
        for i in range(len(STAGES))
    ]

    return {
        "total_requests": stats.total_requests,
        "num_failures": stats.num_failures,
        "service_id": service_id,
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


def log_and_assert_results(stats: LocustTestResults) -> None:
    logger.info(
        f"Aggregated: {stats.total_requests} requests, "
        f"{stats.num_failures} failures, "
        f"p50={stats.p50_latency:.1f}ms, "
        f"p99={stats.p99_latency:.1f}ms"
    )

    for i, stage_stats in enumerate(stats.stats_in_stages):
        logger.info(
            f"Stage {i + 1}: rps={stage_stats.rps:.0f}, "
            f"p50={stage_stats.p50_latency:.1f}ms, "
            f"p99={stage_stats.p99_latency:.1f}ms"
        )

    assert stats.num_failures == 0, (
        f"Expected 0 failures, got {stats.num_failures} "
        f"out of {stats.total_requests}."
    )
    assert (
        stats.p99_latency <= 200
    ), f"p99 latency {stats.p99_latency:.1f}ms exceeds 200ms."
    assert (
        stats.p50_latency <= 100
    ), f"p50 latency {stats.p50_latency:.1f}ms exceeds 100ms."

    logger.info("All assertions passed.")


@click.command()
@click.option("--output-path", "-o", type=str, default=None)
def main(output_path: Optional[str]):
    compute_config = ComputeConfig(
        cloud=CLOUD,
        head_node=HeadNodeConfig(instance_type="m5.2xlarge"),
        worker_nodes=[
            WorkerNodeGroupConfig(
                instance_type="m5.8xlarge",
                min_nodes=5,
                max_nodes=200,
            ),
        ],
    )

    with start_service(
        "single-deployment-autoscaling",
        compute_config=compute_config,
        applications=[APPLICATION],
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        ray.init(address="auto")
        status = service.status(name=service_name, cloud=CLOUD)
        logger.info(f"Service {service_name} running at {status.query_url}")

        num_locust_workers = min(
            16, max(1, int(ray.available_resources().get("CPU", 0)) - 4)
        )
        stats: LocustTestResults = run_locust_load_test(
            LocustLoadTestConfig(
                num_workers=num_locust_workers,
                host_url=status.query_url,
                auth_token=status.query_auth_token,
                # SimulatedMLInference reads request.json() — pass a small body.
                data={"x": 1},
                stages=STAGES,
            )
        )

        log_and_assert_results(stats)

        results = build_results(stats, status.id)
        logger.info(f"Final aggregated metrics: {json.dumps(results, indent=2)}")
        save_test_results(results, output_path=output_path)


if __name__ == "__main__":
    main()
