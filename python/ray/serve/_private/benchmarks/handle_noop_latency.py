import time

import click

from ray import serve
from ray.serve._private.benchmarks.common import Benchmarker, Noop
from ray.serve.handle import DeploymentHandle


@click.command(help="Benchmark no-op DeploymentHandle latency.")
@click.option("--num-replicas", type=int, default=1)
@click.option("--num-requests", type=int, default=100)
@click.option(
    "--mode",
    type=click.Choice(["remote", "choose_dispatch"]),
    default="remote",
    help="Which call pattern to benchmark.",
)
def main(num_replicas: int, num_requests: int, mode: str):
    h: DeploymentHandle = serve.run(
        Benchmarker.bind(Noop.options(num_replicas=num_replicas).bind())
    )

    latencies = h.run_latency_benchmark.remote(
        num_requests=num_requests, mode=mode
    ).result()

    # Let the logs flush to avoid interwoven output.
    time.sleep(1)

    print(
        f"Latency (ms) for noop DeploymentHandle requests via {mode!r} "
        f"(num_replicas={num_replicas},num_requests={num_requests}):"
    )
    print(latencies.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))


if __name__ == "__main__":
    main()
