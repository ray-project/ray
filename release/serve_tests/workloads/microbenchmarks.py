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
    run_latency_benchmark,
    run_throughput_benchmark,
)
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


@serve.deployment
class GrpcDeployment:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    async def grpc_call(self, user_message):
        return serve_pb2.ModelOutput(output=9)

    async def call_with_string(self, user_message):
        return serve_pb2.ModelOutput(output=9)


def convert_throughput_to_perf_metrics(
    name: str, mean: float, std: float
) -> List[Dict]:
    return [
        {
            "perf_metric_name": f"{name}_avg_rps",
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


async def _main(
    output_path: Optional[str],
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
):
    # Start and configure Serve
    serve.start(
        grpc_options=gRPCOptions(
            port=9000,
            grpc_servicer_functions=[
                "ray.serve.generated.serve_pb2_grpc.add_RayServeBenchmarkServiceServicer_to_server",  # noqa
            ],
        )
    )
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
                latencies = await run_latency_benchmark(
                    lambda: requests.get("http://localhost:8000", data=payload),
                    num_requests=NUM_REQUESTS,
                )
                perf_metrics.extend(convert_latencies_to_perf_metrics(name, latencies))
                serve.shutdown()

        if run_throughput:
            # Microbenchmark: HTTP throughput
            serve.run(Noop.bind())
            mean, std = await run_throughput_benchmark(
                fn=partial(do_single_http_batch, batch_size=BATCH_SIZE),
                multiplier=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(convert_throughput_to_perf_metrics("http", mean, std))
            serve.shutdown()

            # Microbenchmark: HTTP throughput at max_ongoing_requests=100
            serve.run(Noop.options(max_ongoing_requests=100).bind())
            mean, std = await run_throughput_benchmark(
                fn=partial(do_single_http_batch, batch_size=BATCH_SIZE),
                multiplier=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "http_100_max_ongoing_requests", mean, std
                )
            )
            serve.shutdown()

    # GRPC
    if run_grpc:
        if run_latency:
            channel = grpc.insecure_channel("localhost:9000")
            stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
            grpc_payload_noop = serve_pb2.StringData(data="")
            grpc_payload_1mb = serve_pb2.StringData(data=payload_1mb)
            grpc_payload_10mb = serve_pb2.StringData(data=payload_10mb)

            for payload, name in [
                (grpc_payload_noop, "grpc"),
                (grpc_payload_1mb, "grpc_1mb"),
                (grpc_payload_10mb, "grpc_10mb"),
            ]:
                serve.run(GrpcDeployment.bind())
                latencies: pd.Series = await run_latency_benchmark(
                    lambda: stub.call_with_string(payload),
                    num_requests=NUM_REQUESTS,
                )
                perf_metrics.extend(convert_latencies_to_perf_metrics(name, latencies))
                serve.shutdown()

        if run_throughput:
            # Microbenchmark: GRPC throughput
            serve.run(GrpcDeployment.bind())
            mean, std = await run_throughput_benchmark(
                fn=partial(do_single_grpc_batch, batch_size=BATCH_SIZE),
                multiplier=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(convert_throughput_to_perf_metrics("grpc", mean, std))
            serve.shutdown()

            # Microbenchmark: GRPC throughput at max_ongoing_requests = 100
            serve.run(GrpcDeployment.options(max_ongoing_requests=100).bind())
            mean, std = await run_throughput_benchmark(
                fn=partial(do_single_grpc_batch, batch_size=BATCH_SIZE),
                multiplier=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "grpc_100_max_ongoing_requests", mean, std
                )
            )
            serve.shutdown()

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
                serve.shutdown()

        if run_throughput:
            # Microbenchmark: Handle throughput
            h: DeploymentHandle = serve.run(Benchmarker.bind(Noop.bind()))
            mean, std = await h.run_throughput_benchmark.remote(
                batch_size=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(convert_throughput_to_perf_metrics("handle", mean, std))
            serve.shutdown()

            # Microbenchmark: Handle throughput at max_ongoing_requests=100
            h: DeploymentHandle = serve.run(
                Benchmarker.options(max_ongoing_requests=100).bind(
                    Noop.options(max_ongoing_requests=100).bind()
                )
            )
            mean, std = await h.run_throughput_benchmark.remote(
                batch_size=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(
                convert_throughput_to_perf_metrics(
                    "handle_100_max_ongoing_requests", mean, std
                )
            )
            serve.shutdown()

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
def main(
    output_path: Optional[str],
    run_all: bool,
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
):
    # If none of the flags are set, default to run all
    if not (run_http or run_grpc or run_handle or run_latency or run_throughput):
        run_all = True

    if run_all:
        run_http = True
        run_grpc = True
        run_handle = True
        run_latency = True
        run_throughput = True

    asyncio.run(
        _main(output_path, run_http, run_grpc, run_handle, run_latency, run_throughput)
    )


if __name__ == "__main__":
    main()
