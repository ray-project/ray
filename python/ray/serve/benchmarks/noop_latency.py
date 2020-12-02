import time
from typing import Optional

import requests
import pandas as pd
from tqdm import tqdm
import click

from ray import serve
from ray.serve.constants import DEFAULT_HTTP_ADDRESS
from ray.serve import controller

controller._TRACING_ENABLED = True


def block_until_ready(url):
    while requests.get(url).status_code == 404:
        time.sleep(1)
        print("Waiting for noop route to showup.")


def run_http_benchmark(url, num_queries):
    latency = []
    for _ in tqdm(range(num_queries + 200)):
        start = time.perf_counter()
        requests.get(url)
        end = time.perf_counter()
        latency.append(end - start)

    # Remove initial samples
    latency = latency[200:]

    series = pd.Series(latency) * 1000
    print("Latency for single noop backend (ms)")
    print(series.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))


@click.command()
@click.option("--blocking", is_flag=True, required=False, help="Block forever")
@click.option("--num-queries", type=int, required=False)
@click.option("--num-replicas", type=int, default=1)
@click.option("--max-concurrent-queries", type=int, required=False)
def main(num_replicas: int, num_queries: Optional[int],
         max_concurrent_queries: Optional[int], blocking: bool):
    client = serve.start()

    def noop(_):
        return "hello world"

    config = {
        "num_replicas": num_replicas,
        "max_concurrent_queries": max_concurrent_queries
    }
    print("Using config", config)
    client.create_backend("noop", noop, config=config)
    client.create_endpoint("noop", backend="noop", route="/noop")

    url = "{}/noop".format(DEFAULT_HTTP_ADDRESS)
    block_until_ready(url)

    if num_queries:
        run_http_benchmark(url, num_queries)
    if blocking:
        print("Endpoint {} is ready.".format(url))
        while True:
            time.sleep(5)


if __name__ == "__main__":
    main()
