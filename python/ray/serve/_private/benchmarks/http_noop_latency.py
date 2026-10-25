import asyncio

import click
import pandas as pd
import requests

from ray import serve
from ray.serve._private.benchmarks.common import Noop, run_latency_benchmark


@click.command(help="Benchmark no-op HTTP latency.")
@click.option("--num-replicas", type=int, default=1)
@click.option("--num-requests", type=int, default=100)
def main(num_replicas: int, num_requests: int):
    serve.run(Noop.options(num_replicas=num_replicas).bind())

    latencies: pd.Series = asyncio.new_event_loop().run_until_complete(
        run_latency_benchmark(
            lambda: requests.get("http://localhost:8000"),
            num_requests=num_requests,
        )
    )

    print(
        "Latency (ms) for noop HTTP requests "
        f"(num_replicas={num_replicas},num_requests={num_requests}):"
    )
    print(latencies.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))


if __name__ == "__main__":
    main()
