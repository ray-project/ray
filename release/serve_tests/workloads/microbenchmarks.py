"""Runs benchmarks.

Latency benchmarks:
    Runs a no-op workload with 1 replica.
    Sends 100 requests to it and records average, P50, P90, P95, P99 latencies.

Throughput benchmarks:
    Asynchronously send batches of 100 requests.
    Calculate the average throughput achieved on 10 batches of requests.
"""
import asyncio
import click
from functools import partial
import json
import logging
import os

import grpc
import pandas as pd
import requests
from typing import Dict, List, Optional
from collections import defaultdict

from ray import serve
from ray.serve._private.benchmarks.common import (
    Benchmarker,
    DirectStreamingRouter,
    do_single_grpc_batch,
    do_single_http_batch,
    do_single_http_streaming_with_per_chunk_timing,
    generate_payload,
    Noop,
    ModelComp,
    GrpcDeployment,
    GrpcModelComp,
    IntermediateRouter,
    run_controller_benchmark,
    run_latency_benchmark,
    run_throughput_benchmark,
    Streamer,
)
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    RAY_SERVE_ENABLE_HA_PROXY,
)
from ray.serve._private.test_utils import get_application_url
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.config import gRPCOptions
from ray.serve.handle import DeploymentHandle

from serve_test_utils import save_test_results


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


# For latency benchmarks
NUM_REQUESTS = 500

# For throughput benchmarks
BATCH_SIZE = 100
NUM_TRIALS = 50
TRIAL_RUNTIME_S = 5

# For streaming benchmarks
STREAMING_BATCH_SIZE = 150
STREAMING_HTTP_BATCH_SIZE = 500
STREAMING_TOKENS_PER_REQUEST = 1000
STREAMING_NUM_TRIALS = 10

# For per-chunk timing passes (TTFT + inter-token jitter). Two batch sizes:
#   bs=1: clean per-stream signal, isolates server-side first-token + emission cadence.
#   bs=64: same metrics under moderate client concurrency. Expect the bs=64
#     inter-token p99.9 to be 10-100x larger than bs=1 even on a healthy
#     system since asyncio interleaves 64 streams; that's the point — we
#     want to see if jitter stays bounded under load.
STREAMING_PER_CHUNK_BS1 = 1
STREAMING_PER_CHUNK_BS1_TRIALS = 20
STREAMING_PER_CHUNK_BS64 = 64
STREAMING_PER_CHUNK_BS64_TRIALS = 5


def convert_throughput_to_perf_metrics(
    name: str,
    mean: float,
    std: float,
    stream: bool = False,
) -> List[Dict]:
    return [
        {
            "perf_metric_name": f"{name}_avg_tps" if stream else f"{name}_avg_rps",
            "perf_metric_value": mean,
            "perf_metric_type": "THROUGHPUT",
        },
        {
            "perf_metric_name": f"{name}_throughput_std",
            "perf_metric_value": std,
            "perf_metric_type": "THROUGHPUT",
        },
    ]


def convert_latencies_to_perf_metrics(name: str, latencies: pd.Series) -> List[Dict]:
    return [
        {
            "perf_metric_name": f"{name}_p50_latency",
            "perf_metric_value": latencies.quantile(0.5),
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{name}_p90_latency",
            "perf_metric_value": latencies.quantile(0.9),
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{name}_p95_latency",
            "perf_metric_value": latencies.quantile(0.95),
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{name}_p99_latency",
            "perf_metric_value": latencies.quantile(0.99),
            "perf_metric_type": "LATENCY",
        },
    ]


def convert_per_chunk_timings_to_perf_metrics(
    name: str,
    bs_suffix: str,
    ttft_ms: List[float],
    inter_chunk_ms: List[float],
) -> List[Dict]:
    """Build TTFT + inter-token jitter perf metrics for one streaming pass.

    Loopback caveat: even with TCP_NODELAY=0 the kernel uses a much shorter
    effective delayed-ACK on lo than on a real WAN, so absolute values
    under-report Nagle's true cost. The benchmark is intended to give a
    directional signal (NODELAY=0 vs =1), not a production estimate.
    """
    ttft_series = pd.Series([x for x in ttft_ms if x == x])  # drop NaN
    gaps_series = pd.Series(inter_chunk_ms)
    metrics: List[Dict] = []
    for q, label in [(0.5, "p50"), (0.9, "p90"), (0.95, "p95"), (0.99, "p99")]:
        metrics.append(
            {
                "perf_metric_name": f"{name}_ttft_{label}_{bs_suffix}",
                "perf_metric_value": (
                    float(ttft_series.quantile(q)) if len(ttft_series) else 0.0
                ),
                "perf_metric_type": "LATENCY",
            }
        )
    for q, label in [(0.5, "p50"), (0.9, "p90"), (0.99, "p99"), (0.999, "p999")]:
        metrics.append(
            {
                "perf_metric_name": f"{name}_inter_token_{label}_{bs_suffix}",
                "perf_metric_value": (
                    float(gaps_series.quantile(q)) if len(gaps_series) else 0.0
                ),
                "perf_metric_type": "LATENCY",
            }
        )
    return metrics


def convert_controller_samples_to_perf_metrics(
    samples: List[Dict],
) -> List[Dict]:
    """Convert controller benchmark raw samples to perf_metrics with std and sample_size."""

    def _mean(vals: List[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    def _std(vals: List[float]) -> float:
        if len(vals) < 2:
            return 0.0
        m = _mean(vals)
        return (sum((v - m) ** 2 for v in vals) / len(vals)) ** 0.5

    groups: Dict[int, List[Dict]] = defaultdict(list)
    for row in samples:
        groups[int(row["target_replicas"])].append(row)

    perf_metrics: List[Dict] = []
    for replicas in sorted(groups.keys()):
        samples_list = groups[replicas]
        n = len(samples_list)
        suffix = f"_{replicas}_replicas"

        def _get_vals(key: str) -> List[float]:
            return [
                float(s[key])
                for s in samples_list
                if isinstance(s.get(key), (int, float))
            ]

        def _add_metric(name: str, key: str, metric_type: str) -> None:
            vals = _get_vals(key)
            perf_metrics.append(
                {
                    "perf_metric_name": name,
                    "perf_metric_value": _mean(vals),
                    "perf_metric_type": metric_type,
                    "perf_metric_std": _std(vals),
                    "perf_metric_sample_size": n,
                }
            )

        _add_metric(
            f"controller_autoscale_duration_s{suffix}",
            "autoscale_duration_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_actual_replicas{suffix}",
            "actual_replicas",
            "THROUGHPUT",
        )
        _add_metric(
            f"controller_loops_per_second{suffix}",
            "loops_per_second",
            "THROUGHPUT",
        )
        _add_metric(
            f"controller_loop_duration_mean_s{suffix}",
            "loop_duration_mean_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_event_loop_delay_s{suffix}",
            "event_loop_delay_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_num_asyncio_tasks{suffix}",
            "num_asyncio_tasks",
            "THROUGHPUT",
        )
        _add_metric(
            f"controller_deployment_state_update_mean_s{suffix}",
            "deployment_state_update_mean_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_application_state_update_mean_s{suffix}",
            "application_state_update_mean_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_proxy_state_update_mean_s{suffix}",
            "proxy_state_update_mean_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_proxy_state_update_std_s{suffix}",
            "proxy_state_update_std_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_node_update_min_s{suffix}",
            "node_update_min_s",
            "LATENCY",
        )
        _add_metric(
            f"controller_handle_metrics_delay_mean_ms{suffix}",
            "handle_metrics_delay_mean_ms",
            "LATENCY",
        )
        _add_metric(
            f"controller_replica_metrics_delay_mean_ms{suffix}",
            "replica_metrics_delay_mean_ms",
            "LATENCY",
        )
        _add_metric(
            f"controller_process_memory_mb{suffix}",
            "process_memory_mb",
            "LATENCY",
        )

    return perf_metrics


def get_throughput_test_name(test_type: str, max_ongoing_requests: int) -> str:
    if max_ongoing_requests == DEFAULT_MAX_ONGOING_REQUESTS:
        return test_type
    else:
        return f"{test_type}_{max_ongoing_requests:_}_max_ongoing_requests"


async def _run_http_per_chunk_timing_passes(
    name: str, url: str, perf_metrics: List[Dict]
) -> None:
    """Run bs=1 and bs=64 per-chunk timing passes against an already-running app."""
    for bs, trials, bs_suffix in [
        (STREAMING_PER_CHUNK_BS1, STREAMING_PER_CHUNK_BS1_TRIALS, "bs1"),
        (STREAMING_PER_CHUNK_BS64, STREAMING_PER_CHUNK_BS64_TRIALS, "bs64"),
    ]:
        ttfts: List[float] = []
        inter_chunks: List[float] = []
        for _ in range(trials):
            t, g, _ = await do_single_http_streaming_with_per_chunk_timing(
                batch_size=bs, url=url
            )
            ttfts.extend(t)
            inter_chunks.extend(g)
        perf_metrics.extend(
            convert_per_chunk_timings_to_perf_metrics(
                name, bs_suffix, ttfts, inter_chunks
            )
        )


async def _run_handle_per_chunk_timing_passes(
    name: str, h: DeploymentHandle, perf_metrics: List[Dict]
) -> None:
    """Run bs=1 and bs=64 handle-side per-chunk timing passes."""
    for bs, trials, bs_suffix in [
        (STREAMING_PER_CHUNK_BS1, STREAMING_PER_CHUNK_BS1_TRIALS, "bs1"),
        (STREAMING_PER_CHUNK_BS64, STREAMING_PER_CHUNK_BS64_TRIALS, "bs64"),
    ]:
        ttfts, inter_tokens, _ = await h.run_streaming_with_per_chunk_timing.remote(
            batch_size=bs, num_trials=trials
        )
        perf_metrics.extend(
            convert_per_chunk_timings_to_perf_metrics(
                name, bs_suffix, ttfts, inter_tokens
            )
        )


async def _main(
    output_path: Optional[str],
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
    run_streaming: bool,
    run_direct_streaming: bool,
    run_controller: bool,
    throughput_max_ongoing_requests: List[int],
    concurrencies: List[int],
):
    perf_metrics = []
    payload_1mb = generate_payload(1000000)
    payload_10mb = generate_payload(10000000)

    # Controller benchmark (separate release test, excluded from --run-all)
    if run_controller:
        controller_samples = await run_controller_benchmark()
        perf_metrics.extend(
            convert_controller_samples_to_perf_metrics(controller_samples)
        )

    # HTTP
    if run_http:
        if run_latency:
            for payload, name in [
                (None, "http"),
                (payload_1mb, "http_1mb"),
                (payload_10mb, "http_10mb"),
            ]:
                serve.run(Noop.bind())
                url = get_application_url(use_localhost=True)
                latencies = await run_latency_benchmark(
                    lambda: requests.get(url, data=payload),
                    num_requests=NUM_REQUESTS,
                )
                perf_metrics.extend(convert_latencies_to_perf_metrics(name, latencies))
                await serve.shutdown_async()

        if run_throughput:
            # Microbenchmark: HTTP throughput
            for max_ongoing_requests, concurrency in zip(
                throughput_max_ongoing_requests, concurrencies
            ):
                workloads = {
                    "http": Noop.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(),
                    "http_model_comp": ModelComp.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(
                        Noop.options(max_ongoing_requests=max_ongoing_requests).bind()
                    ),
                }
                for name, app in workloads.items():
                    serve.run(app)
                    url = get_application_url(use_localhost=True)
                    mean, std, _ = await run_throughput_benchmark(
                        fn=partial(
                            do_single_http_batch, batch_size=concurrency, url=url
                        ),
                        multiplier=concurrency,
                        num_trials=NUM_TRIALS,
                        trial_runtime=TRIAL_RUNTIME_S,
                    )
                    test_name = get_throughput_test_name(name, max_ongoing_requests)
                    perf_metrics.extend(
                        convert_throughput_to_perf_metrics(test_name, mean, std)
                    )
                    await serve.shutdown_async()

        if run_streaming:
            # Direct streaming between replica
            serve.run(
                Streamer.options(max_ongoing_requests=1000).bind(
                    tokens_per_request=STREAMING_TOKENS_PER_REQUEST,
                    inter_token_delay_ms=10,
                )
            )
            url = get_application_url(use_localhost=True)
            # In each trial, complete only one batch of requests. Each
            # batch should take 10+ seconds to complete (because we are
            # streaming 1000 tokens per request with a 10ms inter token
            # delay). Then run STREAMING_NUM_TRIALS, which executes
            # exactly that number of batches, and calculate the average
            # throughput across them.
            mean, std, latencies = await run_throughput_benchmark(
                fn=partial(
                    do_single_http_batch,
                    batch_size=STREAMING_HTTP_BATCH_SIZE,
                    stream=True,
                    url=url,
                ),
                multiplier=STREAMING_HTTP_BATCH_SIZE * STREAMING_TOKENS_PER_REQUEST,
                num_trials=STREAMING_NUM_TRIALS,
                # 10 seconds is only enough time to complete a single batch
                trial_runtime=10,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "http_streaming", mean, std, stream=True
                )
            )
            perf_metrics.extend(
                convert_latencies_to_perf_metrics("http_streaming", latencies)
            )
            await _run_http_per_chunk_timing_passes("http_streaming", url, perf_metrics)
            await serve.shutdown_async()

            # Streaming with intermediate router
            serve.run(
                IntermediateRouter.options(max_ongoing_requests=1000).bind(
                    Streamer.options(max_ongoing_requests=1000).bind(
                        tokens_per_request=STREAMING_TOKENS_PER_REQUEST,
                        inter_token_delay_ms=10,
                    )
                )
            )
            url = get_application_url(use_localhost=True)
            mean, std, latencies = await run_throughput_benchmark(
                fn=partial(
                    do_single_http_batch,
                    batch_size=STREAMING_BATCH_SIZE,
                    stream=True,
                    url=url,
                ),
                multiplier=STREAMING_BATCH_SIZE * STREAMING_TOKENS_PER_REQUEST,
                num_trials=STREAMING_NUM_TRIALS,
                # 10 seconds is only enough time to complete a single batch
                trial_runtime=10,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "http_intermediate_streaming", mean, std, stream=True
                )
            )
            perf_metrics.extend(
                convert_latencies_to_perf_metrics(
                    "http_intermediate_streaming", latencies
                )
            )
            await _run_http_per_chunk_timing_passes(
                "http_intermediate_streaming", url, perf_metrics
            )
            await serve.shutdown_async()

        if run_direct_streaming:
            # Direct streaming: HAProxy-bypass topology where an ingress
            # request router (DirectStreamingRouter) is consulted only for
            # routing decisions and the proxy forwards the request body
            # straight to the picked replica. The token stream returns
            # directly from the replica to the proxy, never traversing the
            # router. This is the same topology used by Ray Serve LLM under
            # RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1, exercised here with a
            # CPU-only Streamer so CI without GPUs can regression-test it.
            #
            # Requires BOTH:
            #   - RAY_SERVE_ENABLE_HA_PROXY=1: ingress_request_router requires
            #     HAProxy and Serve will refuse to build the app otherwise.
            #   - RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1: tracking the
            #     production-path env var so the microbench measures the
            #     same configuration shape Ray Serve LLM ships with.
            # Caller (release_tests.yaml haproxy variant) is expected to set
            # both in runtime_env; we error fast if either is missing.
            assert RAY_SERVE_ENABLE_HA_PROXY, (
                "--run-direct-streaming requires RAY_SERVE_ENABLE_HA_PROXY=1; "
                "ingress_request_router topology only works with HAProxy."
            )
            assert os.environ.get("RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING") == "1", (
                "--run-direct-streaming requires "
                "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1 in the environment; "
                "this is the env var that gates the production direct-streaming "
                "path in Ray Serve LLM, and the microbench mirrors the same "
                "configuration so regressions surface against the right shape."
            )
            streamer_app = Streamer.options(max_ongoing_requests=1000).bind(
                tokens_per_request=STREAMING_TOKENS_PER_REQUEST,
                inter_token_delay_ms=10,
            )
            direct_app = streamer_app._with_ingress_request_router(
                DirectStreamingRouter.bind(streamer_app)
            )
            serve.run(direct_app)
            url = get_application_url(use_localhost=True)
            mean, std, latencies = await run_throughput_benchmark(
                fn=partial(
                    do_single_http_batch,
                    batch_size=STREAMING_HTTP_BATCH_SIZE,
                    stream=True,
                    url=url,
                ),
                multiplier=STREAMING_HTTP_BATCH_SIZE * STREAMING_TOKENS_PER_REQUEST,
                num_trials=STREAMING_NUM_TRIALS,
                # 10 seconds is only enough time to complete a single batch
                trial_runtime=10,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "direct_streaming", mean, std, stream=True
                )
            )
            perf_metrics.extend(
                convert_latencies_to_perf_metrics("direct_streaming", latencies)
            )
            await _run_http_per_chunk_timing_passes(
                "direct_streaming", url, perf_metrics
            )
            await serve.shutdown_async()

    # GRPC
    if run_grpc:
        serve_grpc_options = gRPCOptions(
            port=9000,
            grpc_servicer_functions=[
                "ray.serve.generated.serve_pb2_grpc.add_RayServeBenchmarkServiceServicer_to_server",  # noqa
            ],
        )
        if run_latency:
            grpc_payload_noop = serve_pb2.StringData(data="")
            grpc_payload_1mb = serve_pb2.StringData(data=payload_1mb)
            grpc_payload_10mb = serve_pb2.StringData(data=payload_10mb)

            for payload, name in [
                (grpc_payload_noop, "grpc"),
                (grpc_payload_1mb, "grpc_1mb"),
                (grpc_payload_10mb, "grpc_10mb"),
            ]:
                serve.start(grpc_options=serve_grpc_options)
                serve.run(GrpcDeployment.bind())
                target = get_application_url(
                    protocol=RequestProtocol.GRPC, use_localhost=True
                )
                channel = grpc.insecure_channel(target)
                stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
                latencies: pd.Series = await run_latency_benchmark(
                    lambda: stub.call_with_string(payload),
                    num_requests=NUM_REQUESTS,
                )
                perf_metrics.extend(convert_latencies_to_perf_metrics(name, latencies))
                await serve.shutdown_async()

        if run_throughput:
            # Microbenchmark: GRPC throughput
            for max_ongoing_requests, concurrency in zip(
                throughput_max_ongoing_requests, concurrencies
            ):
                workloads = {
                    "grpc": GrpcDeployment.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(),
                    "grpc_model_comp": GrpcModelComp.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(
                        Noop.options(max_ongoing_requests=max_ongoing_requests).bind()
                    ),
                }
                for name, app in workloads.items():
                    serve.start(grpc_options=serve_grpc_options)
                    serve.run(app)
                    target = get_application_url(
                        protocol=RequestProtocol.GRPC, use_localhost=True
                    )
                    mean, std, _ = await run_throughput_benchmark(
                        fn=partial(
                            do_single_grpc_batch, batch_size=concurrency, target=target
                        ),
                        multiplier=concurrency,
                        num_trials=NUM_TRIALS,
                        trial_runtime=TRIAL_RUNTIME_S,
                    )
                    test_name = get_throughput_test_name(name, max_ongoing_requests)
                    perf_metrics.extend(
                        convert_throughput_to_perf_metrics(test_name, mean, std)
                    )
                    await serve.shutdown_async()

    # Handle
    if run_handle:
        if run_latency:
            for payload, name in [
                (None, "handle"),
                (payload_1mb, "handle_1mb"),
                (payload_10mb, "handle_10mb"),
            ]:
                h: DeploymentHandle = serve.run(Benchmarker.bind(Noop.bind()))
                latencies = await h.run_latency_benchmark.remote(
                    num_requests=NUM_REQUESTS, payload=payload
                )
                perf_metrics.extend(convert_latencies_to_perf_metrics(name, latencies))
                await serve.shutdown_async()

        if run_throughput:
            # Microbenchmark: Handle throughput
            for max_ongoing_requests, concurrency in zip(
                throughput_max_ongoing_requests, concurrencies
            ):
                workloads = {
                    "handle": Benchmarker.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(
                        Noop.options(max_ongoing_requests=max_ongoing_requests).bind()
                    ),
                    "handle_model_comp": Benchmarker.options(
                        max_ongoing_requests=max_ongoing_requests
                    ).bind(
                        ModelComp.options(
                            max_ongoing_requests=max_ongoing_requests
                        ).bind(
                            Noop.options(
                                max_ongoing_requests=max_ongoing_requests
                            ).bind()
                        )
                    ),
                }
                for name, app in workloads.items():
                    h: DeploymentHandle = serve.run(app)

                    mean, std, _ = await h.run_throughput_benchmark.remote(
                        batch_size=concurrency,
                        num_trials=NUM_TRIALS,
                        trial_runtime=TRIAL_RUNTIME_S,
                    )
                    test_name = get_throughput_test_name(name, max_ongoing_requests)
                    perf_metrics.extend(
                        convert_throughput_to_perf_metrics(test_name, mean, std)
                    )
                    await serve.shutdown_async()

        if run_streaming:
            h: DeploymentHandle = serve.run(
                Benchmarker.bind(
                    Streamer.options(max_ongoing_requests=1000).bind(
                        tokens_per_request=STREAMING_TOKENS_PER_REQUEST,
                        inter_token_delay_ms=10,
                    ),
                    stream=True,
                )
            )
            mean, std, latencies = await h.run_throughput_benchmark.remote(
                batch_size=STREAMING_BATCH_SIZE,
                num_trials=STREAMING_NUM_TRIALS,
                # 10 seconds is only enough time to complete a single batch
                trial_runtime=10,
                tokens_per_request=STREAMING_TOKENS_PER_REQUEST,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "handle_streaming", mean, std, stream=True
                )
            )
            perf_metrics.extend(
                convert_latencies_to_perf_metrics("handle_streaming", latencies)
            )
            await _run_handle_per_chunk_timing_passes(
                "handle_streaming", h, perf_metrics
            )
            await serve.shutdown_async()

    logging.info(f"Perf metrics:\n {json.dumps(perf_metrics, indent=4)}")
    results = {"perf_metrics": perf_metrics}
    save_test_results(results, output_path=output_path)


@click.command()
@click.option("--output-path", "-o", type=str, default=None)
@click.option("--run-all", is_flag=True)
@click.option("--run-http", is_flag=True)
@click.option("--run-grpc", is_flag=True)
@click.option("--run-handle", is_flag=True)
@click.option("--run-latency", is_flag=True)
@click.option("--run-throughput", is_flag=True)
@click.option("--run-streaming", is_flag=True)
@click.option(
    "--run-direct-streaming",
    is_flag=True,
    help=(
        "Run the direct-streaming setup (ingress_request_router bypass). "
        "Requires RAY_SERVE_ENABLE_HA_PROXY=1. Nested under --run-http. "
        "Excluded from --run-all by default since it needs HAProxy."
    ),
)
@click.option(
    "--run-controller",
    is_flag=True,
    help="Run controller health benchmark only (separate from --run-all).",
)
@click.option(
    "--throughput-max-ongoing-requests",
    "-t",
    multiple=True,
    type=int,
    default=[5, 100, 800],
    help="Max ongoing requests for throughput benchmarks. Must be in the same order as --concurrencies. Default: [5, 100, 800]",
)
@click.option(
    "--concurrencies",
    "-c",
    multiple=True,
    type=int,
    default=[100, 100, 800],
    help="User concurrency for throughput benchmarks. Must be in the same order as --throughput-max-ongoing-requests. Default: [100, 100, 800]",
)
def main(
    output_path: Optional[str],
    run_all: bool,
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
    run_streaming: bool,
    run_direct_streaming: bool,
    run_controller: bool,
    throughput_max_ongoing_requests: List[int],
    concurrencies: List[int],
):
    assert len(throughput_max_ongoing_requests) == len(
        concurrencies
    ), "Must have the same number of --throughput-max-ongoing-requests and --concurrencies"

    # If none of the flags are set, default to run all (excluding controller
    # and direct-streaming, which both require special setup).
    if not (
        run_http
        or run_grpc
        or run_handle
        or run_latency
        or run_throughput
        or run_streaming
        or run_direct_streaming
        or run_controller
    ):
        run_all = True

    if run_all:
        run_http = True
        run_grpc = True
        run_handle = True
        run_latency = True
        run_throughput = True
        run_streaming = True
        # run_direct_streaming stays False - requires RAY_SERVE_ENABLE_HA_PROXY=1
        # and is only meaningful in the haproxy variant.
        # run_controller stays False - controller benchmark is a separate release test

    asyncio.run(
        _main(
            output_path,
            run_http,
            run_grpc,
            run_handle,
            run_latency,
            run_throughput,
            run_streaming,
            run_direct_streaming,
            run_controller,
            throughput_max_ongoing_requests,
            concurrencies,
        )
    )


if __name__ == "__main__":
    main()
