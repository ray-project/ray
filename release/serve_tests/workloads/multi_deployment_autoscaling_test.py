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
        "import_path": f"simulated_ml_inference:{app_name}",
        "route_prefix": route,
        "deployments": [
            {
                "name": f"SimulatedMLInference_{app_name}",
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
        app_name="app_1",
        route="/app_1/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=10,
        max_replicas=800,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app_2",
        route="/app_2/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=20,
        max_replicas=800,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app_3",
        route="/app_3/predict",
        num_cpus=1.0,
        max_ongoing_requests=1,
        min_replicas=15,
        max_replicas=800,
        target_ongoing_requests=0.2,
        upscaling_factor=1.5,
    ),
    _make_application(
        app_name="app_4",
        route="/app_4/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=250,
        max_replicas=1000,
        target_ongoing_requests=0.1,
        upscaling_factor=10.0,
    ),
    _make_application(
        app_name="app_5",
        route="/app_5/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=250,
        max_replicas=1000,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app_6",
        route="/app_6/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=50,
        max_replicas=2000,
        target_ongoing_requests=0.1,
        upscaling_factor=10.0,
    ),
    _make_application(
        app_name="app_7",
        route="/app_7/predict",
        num_cpus=2.0,
        max_ongoing_requests=1,
        min_replicas=50,
        max_replicas=2000,
        target_ongoing_requests=0.1,
    ),
    _make_application(
        app_name="app_8",
        route="/app_8/predict",
        num_cpus=0.3,
        max_ongoing_requests=5,
        min_replicas=1,
        max_replicas=200,
        target_ongoing_requests=0.4,
    ),
]

# (app-name, route, weight)
WARMUP_ENDPOINTS = [
    ("app_1", "/app_1/predict", 1),
    ("app_2", "/app_2/predict", 1),
    ("app_3", "/app_3/predict", 1),
    ("app_4", "/app_4/predict", 1),
    ("app_5", "/app_5/predict", 1),
    ("app_6", "/app_6/predict", 1),
    ("app_7", "/app_7/predict", 1),
    ("app_8", "/app_8/predict", 1),
]

# (app-name, route, weight)
RAMP_ENDPOINTS = [
    ("app_1", "/app_1/predict", 4),
    ("app_2", "/app_2/predict", 1),
    ("app_3", "/app_3/predict", 1),
    ("app_4", "/app_4/predict", 38),
    ("app_5", "/app_5/predict", 1),
    ("app_6", "/app_6/predict", 62),
    ("app_7", "/app_7/predict", 1),
    ("app_8", "/app_8/predict", 1),
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

# Per-stage time windows (from test start)
# (stage, stage-start, stage-end)
LOAD_STAGES = [
    ("warmup", 0, WARMUP_SEC),
    ("ramp_up", WARMUP_SEC, WARMUP_SEC + 310),
    ("sustain", WARMUP_SEC + 310, WARMUP_SEC + 800),
    ("ramp_down", WARMUP_SEC + 800, WARMUP_SEC + 810),
]


def log_and_assert_results(stats: Dict[str, Any]) -> None:
    errors = stats["num_failures"]
    total = stats["total_requests"]
    p50 = stats["p50_latency"]
    p99 = stats["p99_latency"]

    # ToDo(kamil) - for now only report max_latency, once stable add checks.
    max_latency = stats["max_latency"]

    logger.info(
        f"Aggregated: {total} requests, {errors} failures, "
        f"p50={p50:.1f}ms, "
        f"p99={p99:.1f}ms, "
        f"max_latency={max_latency:.1f}ms"
    )

    if "stages" in stats:
        for stage_name, stage_stats in stats["stages"].items():
            logger.info(
                f"Stage '{stage_name}': "
                f"avg_rps={stage_stats['avg_rps']:.0f}, "
                f"p99={stage_stats['p99_latency']:.1f}ms, "
                f"max_latency={stage_stats['max_latency']:.1f}ms, "
                f"avg_users={stage_stats['avg_users']:.0f}"
            )

    # Per-stage assertions
    stage_thresholds = {
        "warmup": {"p99_latency": 200},
        "ramp_up": {"p99_latency": 200, "avg_rps": 100},
        "sustain": {"p99_latency": 200, "avg_rps": 1500},
        "ramp_down": {"p99_latency": 200},
    }
    if "stages" in stats:
        for stage_name, thresholds in stage_thresholds.items():
            stage = stats["stages"].get(stage_name, {})
            if not stage:
                logger.warning(f"No results for stage: '{stage_name}'.")
                continue
            if "p99_latency" in thresholds:
                assert stage["p99_latency"] <= thresholds["p99_latency"], (
                    f"{stage_name} p99_latency={stage['p99_latency']:.1f}ms "
                    f"exceeds: {thresholds['p99_latency']}ms."
                )
            if "avg_rps" in thresholds:
                assert stage["avg_rps"] >= thresholds["avg_rps"], (
                    f"{stage_name} avg_rps={stage['avg_rps']:.0f} "
                    f"below: {thresholds['avg_rps']}."
                )

    # Assertions on aggregated results
    assert errors == 0, f"Expected 0 failures, got {errors} out of {total} requests."
    assert p50 <= 100, f"p50 latency {p50:.1f}ms exceeds 100ms."
    assert p99 <= 200, f"p99 latency {p99:.1f}ms exceeds 200ms."
    assert total >= 2_000_000, f"Total requests {total} below minimum 2,000,000."

    logger.info("All assertions passed.")


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
            "perf_metric_name": "max_latency",
            "perf_metric_value": stats["max_latency"],
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
            metrics.append(
                {
                    "perf_metric_name": f"{stage_name}_max_latency",
                    "perf_metric_value": stage_stats["max_latency"],
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
        service_name="multi-deployment-autoscaling",
        compute_config=compute_config,
        applications=APPLICATIONS,
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        ray.init(address="auto")
        status = service.status(name=service_name, cloud=CLOUD)
        logger.info(f"Service {service_name} running at {status.query_url}")

        num_locust_workers = min(
            16, max(1, int(ray.available_resources().get("CPU", 0)) - 4)
        )
        logger.info(f"Running with: {num_locust_workers=}")
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

        log_and_assert_results(stats)

        results = build_results(stats, status.id)
        logger.info(f"Results: {json.dumps(results, indent=2)}")
        save_test_results(results, output_path=output_path)


if __name__ == "__main__":
    main()
