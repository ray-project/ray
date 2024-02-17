import click

import ray
from ray.serve._private.benchmarks.streaming.common import Caller, Endpoint, IOMode


# @ray.remote(runtime_env=GRPC_DEBUG_RUNTIME_ENV)
@ray.remote
class EndpointActor(Endpoint):
    pass


# @ray.remote(runtime_env=GRPC_DEBUG_RUNTIME_ENV)
@ray.remote
class CallerActor(Caller):
    async def _consume_single_stream(self):
        method = self._get_remote_method()
        async for ref in method.options(num_returns="streaming").remote():
            r = ray.get(ref)

            # self.sink(str(r, 'utf-8'))
            self.sink(r)


@click.command(help="Benchmark streaming deployment handle throughput.")
@click.option(
    "--tokens-per-request",
    type=int,
    default=1000,
    help="Number of tokens (per request) to stream from downstream deployment",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="Number of requests to send to downstream deployment in each batch.",
)
@click.option(
    "--num-replicas",
    type=int,
    default=1,
    help="Number of replicas in the downstream deployment.",
)
@click.option(
    "--num-trials",
    type=int,
    default=5,
    help="Number of trials of the benchmark to run.",
)
@click.option(
    "--trial-runtime",
    type=int,
    default=5,
    help="Duration to run each trial of the benchmark for (seconds).",
)
@click.option(
    "--io-mode",
    type=str,
    default="async",
    help="Controls mode of the streaming generation (either 'sync' or 'async')",
)
def main(
    tokens_per_request: int,
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
    io_mode: str,
):
    h = CallerActor.remote(
        EndpointActor.remote(
            tokens_per_request=tokens_per_request,
        ),
        mode=IOMode(io_mode.upper()),
        tokens_per_request=tokens_per_request,
        batch_size=batch_size,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    )

    mean, stddev = ray.get(h.run_benchmark.remote())
    print(
        "Core Actors streaming throughput ({}) {}: {} +- {} tokens/s".format(
            io_mode.upper(),
            f"(num_replicas={num_replicas}, "
            f"tokens_per_request={tokens_per_request}, "
            f"batch_size={batch_size})",
            mean,
            stddev,
        )
    )


if __name__ == "__main__":
    main()
