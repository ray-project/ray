"""
Multi-deployment proxy autoscaling release test.

Deploys 8 Ray Serve applications via Anyscale Service and runs a
multi-endpoint locust load test with a warm-up + ramp profile.
Validates that latencies remain stable under autoscaling.
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
from locust_utils import run_multi_endpoint_load_test
from serve_test_utils import save_test_results

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

CLOUD = "serve_release_tests_cloud"


def _make_application(
    app_name: str,
    route: str,
    num_cpus: float,
    max_ongoing_requests: int,
    **autoscaling_overrides: Any,
) -> Dict[str, Any]:
    return {
        "name": app_name,
        "import_path": "simulated_ml_inference:app",
        "route_prefix": route,
        "deployments": [
            {
                "name": "SimulatedMLInference",
                "num_replicas": "auto",
                "max_queued_requests": -1,
                "graceful_shutdown_wait_loop_s": 2.0,
                "graceful_shutdown_timeout_s": 20.0,
                "health_check_period_s": 10.0,
                "health_check_timeout_s": 30.0,
                "ray_actor_options": {"num_cpus": num_cpus},
                "max_ongoing_requests": max_ongoing_requests,
                "autoscaling_config": {
                    "metrics_interval_s": 10.0,
                    "look_back_period_s": 30.0,
                    "smoothing_factor": 1.0,
                    "downscale_delay_s": 30.0,
                    "upscale_delay_s": 5.0,
                    **autoscaling_overrides,
                },
            }
        ],
    }


APPLICATIONS = [
    _make_application(
        app_name="app-1",
        route="/app-1/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=10,
        max_replicas=800,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app-2",
        route="/app-2/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=20,
        max_replicas=800,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app-3",
        route="/app-3/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=15,
        max_replicas=800,
        target_ongoing_requests=0.2,
        upscaling_factor=1.5,
    ),
    _make_application(
        app_name="app-4",
        route="/app-4/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=250,
        max_replicas=1000,
        target_ongoing_requests=0.1,
        upscaling_factor=10.0,
    ),
    _make_application(
        app_name="app-5",
        route="/app-5/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=250,
        max_replicas=1000,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app-6",
        route="/app-6/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=50,
        max_replicas=2000,
        target_ongoing_requests=0.1,
        upscaling_factor=10.0,
    ),
    _make_application(
        app_name="app-7",
        route="/app-7/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=50,
        max_replicas=2000,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app-8",
        route="/app-8/predict",
        num_cpus=0.3,
        max_ongoing_requests=5,
        min_replicas=1,
        max_replicas=200,
        target_ongoing_requests=0.4,
    ),
]

# (app-name, route, weight)
WARMUP_ENDPOINTS = [
    ("app-1", "/app-1/predict", 1),
    ("app-2", "/app-2/predict", 1),
    ("app-3", "/app-3/predict", 1),
    ("app-4", "/app-4/predict", 1),
    ("app-5", "/app-5/predict", 1),
    ("app-6", "/app-6/predict", 1),
    ("app-7", "/app-7/predict", 1),
    ("app-8", "/app-8/predict", 1),
]

# (app-name, route, weight)
RAMP_ENDPOINTS = [
    ("app-1", "/app-1/predict", 4),
    ("app-2", "/app-2/predict", 1),
    ("app-3", "/app-3/predict", 1),
    ("app-4", "/app-4/predict", 38),
    ("app-5", "/app-5/predict", 1),
    ("app-6", "/app-6/predict", 62),
    ("app-7", "/app-7/predict", 1),
    ("app-8", "/app-8/predict", 1),
]

WARMUP_SEC = 40

# (time_offset_s, ramp_users)
RAMP_PROFILE = [
    (0, 0),
    (300, 49),
    (310, 100),
    (800, 100),
    (810, 0),
]

# Per-stage time windows (from test start), derived from RAMP_PROFILE
LOAD_STAGES = [
    ("warmup", 0, WARMUP_SEC),
    ("ramp_up", WARMUP_SEC, WARMUP_SEC + 310),
    ("sustain", WARMUP_SEC + 310, WARMUP_SEC + 800),
    ("ramp_down", WARMUP_SEC + 800, WARMUP_SEC + 810),
]


def assert_results(stats: Dict[str, Any]) -> None:
    errors = stats["num_failures"]
    total = stats["total_requests"]
    p99 = stats["p99_latency"]
    p50 = stats["p50_latency"]

    # Assertions on the aggregated results
    assert errors == 0, f"Expected 0 failures, got {errors} out of {total} requests."
    assert p99 <= 200, f"p99 latency {p99:.1f}ms exceeds 200ms."
    assert p50 <= 100, f"p50 latency {p50:.1f}ms exceeds 100ms."
    assert total >= 1000, f"Total requests {total} below minimum 1000."

    # Per-stage assertions
    if "stages" in stats:
        for stage_name, stage_stats in stats["stages"].items():
            logger.info(
                f"Stage '{stage_name}': avg_rps={stage_stats['avg_rps']:.0f}, "
                f"p99={stage_stats['p99_latency']:.1f}ms, "
                f"avg_users={stage_stats['avg_users']:.0f}"
            )

        sustain = stats["stages"].get("sustain", {})
        if sustain:
            assert (
                sustain["avg_rps"] >= 500
            ), f"Sustain stage avg_rps {sustain['avg_rps']:.0f} below 500."
            assert (
                sustain["p99_latency"] <= 200
            ), f"Sustain stage p99 {sustain['p99_latency']:.1f}ms exceeds 200ms."

    logger.info(
        f"Assertions passed: {total} requests, 0 failures, "
        f"p50={p50:.1f}ms, p99={p99:.1f}ms"
    )


def build_results(stats: Dict[str, Any], service_id: str) -> Dict[str, Any]:
    metrics = [
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
    ]

    for ep_name, ep_stats in stats["per_endpoint"].items():
        metrics.append(
            {
                "perf_metric_name": f"{ep_name}_p99_latency",
                "perf_metric_value": ep_stats["p99_latency"],
                "perf_metric_type": "LATENCY",
            }
        )
        metrics.append(
            {
                "perf_metric_name": f"{ep_name}_rps",
                "perf_metric_value": ep_stats["rps"],
                "perf_metric_type": "THROUGHPUT",
            }
        )

    if "stages" in stats:
        for stage_name, stage_stats in stats["stages"].items():
            metrics.append(
                {
                    "perf_metric_name": f"{stage_name}_avg_rps",
                    "perf_metric_value": stage_stats["avg_rps"],
                    "perf_metric_type": "THROUGHPUT",
                }
            )
            metrics.append(
                {
                    "perf_metric_name": f"{stage_name}_p99_latency",
                    "perf_metric_value": stage_stats["p99_latency"],
                    "perf_metric_type": "LATENCY",
                }
            )

    return {
        "total_requests": stats["total_requests"],
        "num_failures": stats["num_failures"],
        "service_id": service_id,
        "perf_metrics": metrics,
    }


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
        "proxy-autoscaling",
        compute_config=compute_config,
        applications=APPLICATIONS,
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        ray.init(address="auto")
        status = service.status(name=service_name, cloud=CLOUD)
        logger.info(f"Service {service_name} running at {status.query_url}")

        num_locust_workers = max(1, int(ray.available_resources().get("CPU", 0)) - 4)
        stats = run_multi_endpoint_load_test(
            num_workers=num_locust_workers,
            host_url=status.query_url,
            auth_token=status.query_auth_token,
            warmup_endpoints=WARMUP_ENDPOINTS,
            ramp_endpoints=RAMP_ENDPOINTS,
            ramp_profile=RAMP_PROFILE,
            warmup_sec=WARMUP_SEC,
            stages=LOAD_STAGES,
        )

        logger.info(
            f"Load test done: {stats['total_requests']} requests, "
            f"{stats['num_failures']} failures, "
            f"p99={stats['p99_latency']:.1f}ms"
        )

        assert_results(stats)

        results = build_results(stats, status.id)
        logger.info(f"Results: {json.dumps(results, indent=2)}")
        save_test_results(results, output_path=output_path)


if __name__ == "__main__":
    main()
