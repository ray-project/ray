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

import grpc
import pandas as pd
import requests
from typing import Dict, List, Optional

from ray import serve
from ray.serve._private.benchmarks.common import (
    Benchmarker,
    do_single_grpc_batch,
    do_single_http_batch,
    generate_payload,
    Noop,
    ModelComp,
    GrpcDeployment,
    GrpcModelComp,
    IntermediateRouter,
    run_latency_benchmark,
    run_throughput_benchmark,
    Streamer,
)
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import DEFAULT_MAX_ONGOING_REQUESTS
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


def get_throughput_test_name(test_type: str, max_ongoing_requests: int) -> str:
    if max_ongoing_requests == DEFAULT_MAX_ONGOING_REQUESTS:
        return test_type
    else:
        return f"{test_type}_{max_ongoing_requests:_}_max_ongoing_requests"


async def _main(
    output_path: Optional[str],
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
    run_streaming: bool,
    throughput_max_ongoing_requests: List[int],
    concurrencies: List[int],
):
    perf_metrics = []
    payload_1mb = generate_payload(1000000)
    payload_10mb = generate_payload(10000000)

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
    throughput_max_ongoing_requests: List[int],
    concurrencies: List[int],
):
    assert len(throughput_max_ongoing_requests) == len(
        concurrencies
    ), "Must have the same number of --throughput-max-ongoing-requests and --concurrencies"

    # If none of the flags are set, default to run all
    if not (
        run_http
        or run_grpc
        or run_handle
        or run_latency
        or run_throughput
        or run_streaming
    ):
        run_all = True

    if run_all:
        run_http = True
        run_grpc = True
        run_handle = True
        run_latency = True
        run_throughput = True
        run_streaming = True

    asyncio.run(
        _main(
            output_path,
            run_http,
            run_grpc,
            run_handle,
            run_latency,
            run_throughput,
            run_streaming,
            throughput_max_ongoing_requests,
            concurrencies,
        )
    )


if __name__ == "__main__":
    main()
