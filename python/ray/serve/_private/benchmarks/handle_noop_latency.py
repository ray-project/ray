import logging
import time

import click
import pandas as pd

from ray import serve
from ray.serve._private.benchmarks.common import run_latency_benchmark
from ray.serve.handle import DeploymentHandle, RayServeHandle


@serve.deployment
class Noop:
    def __init__(self):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    def __call__(self):
        return b""


@serve.deployment
class Caller:
    def __init__(self, noop_handle: RayServeHandle):
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        self._noop_handle: DeploymentHandle = noop_handle.options(
            use_new_handle_api=True
        )

    async def do_single_request(self):
        return await self._noop_handle.remote()

    async def run_latency_benchmark(self, num_requests: int) -> pd.Series:
        return await run_latency_benchmark(
            self.do_single_request,
            num_requests=num_requests,
        )


@click.command(help="Benchmark no-op DeploymentHandle latency.")
@click.option("--num-replicas", type=int, default=1)
@click.option("--num-requests", type=int, default=100)
def main(num_replicas: int, num_requests: int):
    h: DeploymentHandle = serve.run(
        Caller.bind(Noop.options(num_replicas=num_replicas).bind())
    ).options(use_new_handle_api=True)

    latencies = h.run_latency_benchmark.remote(
        num_requests,
    ).result()

    # Let the logs flush to avoid interwoven output.
    time.sleep(1)

    print(
        "Latency (ms) for noop DeploymentHandle requests "
        f"(num_replicas={num_replicas},num_requests={num_requests}):"
    )
    print(latencies.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))


if __name__ == "__main__":
    main()
