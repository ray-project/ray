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
    IntermediateRouter,
    run_latency_benchmark,
    run_throughput_benchmark,
    Streamer,
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

# For streaming benchmarks
STREAMING_BATCH_SIZE = 150
STREAMING_HTTP_BATCH_SIZE = 500
STREAMING_TOKENS_PER_REQUEST = 1000
STREAMING_NUM_TRIALS = 10


@serve.deployment
class GrpcDeployment:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    async def grpc_call(self, user_message):
        return serve_pb2.ModelOutput(output=9)

    async def call_with_string(self, user_message):
        return serve_pb2.ModelOutput(output=9)


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


async def _main(
    output_path: Optional[str],
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
    run_streaming: bool,
):
    perf_metrics = []
    payload_1mb = generate_payload(1000000)
    payload_10mb = generate_payload(10000000)

    # Handle
    if run_handle:
        if run_throughput:
            # Microbenchmark: Handle throughput
            h: DeploymentHandle = serve.run(Benchmarker.bind(Noop.bind()))
            mean, std, _ = await h.run_throughput_benchmark.remote(
                batch_size=BATCH_SIZE,
                num_trials=NUM_TRIALS,
                trial_runtime=TRIAL_RUNTIME_S,
            )
            perf_metrics.extend(convert_throughput_to_perf_metrics("handle", mean, std))
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
@click.option("--run-streaming", is_flag=True)
def main(
    output_path: Optional[str],
    run_all: bool,
    run_http: bool,
    run_grpc: bool,
    run_handle: bool,
    run_latency: bool,
    run_throughput: bool,
    run_streaming: bool,
):
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
        )
    )


if __name__ == "__main__":
    main()
