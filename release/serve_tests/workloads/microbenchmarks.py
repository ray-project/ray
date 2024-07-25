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


async def _main(output_path: Optional[str]):
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
    serve.run(Noop.bind())
    # Microbenchmark: HTTP noop latencies
    latencies = await run_latency_benchmark(
        lambda: requests.get("http://localhost:8000"), num_requests=NUM_REQUESTS
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("http", latencies))
    # HTTP latencies: 1MB payload
    latencies = await run_latency_benchmark(
        lambda: requests.post("http://localhost:8000", data=payload_1mb),
        num_requests=NUM_REQUESTS,
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("http_1mb", latencies))
    # HTTP latencies: 10MB payload
    latencies = await run_latency_benchmark(
        lambda: requests.post("http://localhost:8000", data=payload_10mb),
        num_requests=NUM_REQUESTS,
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("http_10mb", latencies))
    # Microbenchmark: HTTP throughput
    mean, std = await run_throughput_benchmark(
        fn=partial(do_single_http_batch, batch_size=BATCH_SIZE),
        multiplier=BATCH_SIZE,
        num_trials=NUM_TRIALS,
        trial_runtime=TRIAL_RUNTIME_S,
    )
    perf_metrics.extend(convert_throughput_to_perf_metrics("http", mean, std))

    # GRPC
    serve.run(GrpcDeployment.bind())
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
    # Microbenchmark: GRPC noop latencies
    latencies: pd.Series = await run_latency_benchmark(
        lambda: stub.call_with_string(serve_pb2.StringData(data="")),
        num_requests=NUM_REQUESTS,
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("grpc", latencies))
    # Microbenchmark: GRPC 1MB latencies
    latencies: pd.Series = await run_latency_benchmark(
        lambda: stub.call_with_string(serve_pb2.StringData(data=payload_1mb)),
        num_requests=NUM_REQUESTS,
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("grpc_1mb", latencies))
    # Microbenchmark: GRPC 10MB latencies
    latencies: pd.Series = await run_latency_benchmark(
        lambda: stub.call_with_string(serve_pb2.StringData(data=payload_10mb)),
        num_requests=NUM_REQUESTS,
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("grpc_10mb", latencies))
    # Microbenchmark: GRPC throughput
    mean, std = await run_throughput_benchmark(
        fn=partial(do_single_grpc_batch, batch_size=BATCH_SIZE),
        multiplier=BATCH_SIZE,
        num_trials=NUM_TRIALS,
        trial_runtime=TRIAL_RUNTIME_S,
    )
    perf_metrics.extend(convert_throughput_to_perf_metrics("grpc", mean, std))

    # Handle
    h: DeploymentHandle = serve.run(Benchmarker.bind(Noop.bind()))
    # Microbenchmark: Handle noop latencies
    latencies = await h.run_latency_benchmark.remote(num_requests=NUM_REQUESTS)
    perf_metrics.extend(convert_latencies_to_perf_metrics("handle", latencies))
    # Handle latencies: 1MB payload
    latencies = await h.run_latency_benchmark.remote(
        num_requests=NUM_REQUESTS, payload=payload_1mb
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("handle_1mb", latencies))
    # Handle latencies: 10MB payload
    latencies = await h.run_latency_benchmark.remote(
        num_requests=NUM_REQUESTS, payload=payload_10mb
    )
    perf_metrics.extend(convert_latencies_to_perf_metrics("handle_10mb", latencies))
    # Microbenchmark: Handle throughput
    mean, std = await h.run_throughput_benchmark.remote(
        batch_size=BATCH_SIZE, num_trials=NUM_TRIALS, trial_runtime=TRIAL_RUNTIME_S
    )
    perf_metrics.extend(convert_throughput_to_perf_metrics("handle", mean, std))

    # Testing throughput using previous config `max_ongoing_requests` at 100
    # Microbenchmark: HTTP throughput
    serve.run(Noop.options(max_ongoing_requests=100).bind())
    mean, std = await run_throughput_benchmark(
        fn=partial(do_single_http_batch, batch_size=BATCH_SIZE),
        multiplier=BATCH_SIZE,
        num_trials=NUM_TRIALS,
        trial_runtime=TRIAL_RUNTIME_S,
    )
    perf_metrics.extend(
        convert_throughput_to_perf_metrics("http_100_max_ongoing_requests", mean, std)
    )
    # Microbenchmark: GRPC throughput
    serve.run(GrpcDeployment.options(max_ongoing_requests=100).bind())
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
    mean, std = await run_throughput_benchmark(
        fn=partial(do_single_grpc_batch, batch_size=BATCH_SIZE),
        multiplier=BATCH_SIZE,
        num_trials=NUM_TRIALS,
        trial_runtime=TRIAL_RUNTIME_S,
    )
    perf_metrics.extend(
        convert_throughput_to_perf_metrics("grpc_100_max_ongoing_requests", mean, std)
    )
    # Microbenchmark: Handle throughput
    h: DeploymentHandle = serve.run(
        Benchmarker.options(max_ongoing_requests=100).bind(
            Noop.options(max_ongoing_requests=100).bind()
        )
    )
    mean, std = await h.run_throughput_benchmark.remote(
        batch_size=BATCH_SIZE, num_trials=NUM_TRIALS, trial_runtime=TRIAL_RUNTIME_S
    )
    perf_metrics.extend(
        convert_throughput_to_perf_metrics("handle_100_max_ongoing_requests", mean, std)
    )

    logging.info(f"Perf metrics:\n {json.dumps(perf_metrics, indent=4)}")
    results = {"perf_metrics": perf_metrics}
    save_test_results(results, output_path=output_path)


@click.command()
@click.option("--output-path", "-o", type=str, default=None)
def main(output_path: Optional[str]):
    asyncio.run(_main(output_path))


if __name__ == "__main__":
    main()
