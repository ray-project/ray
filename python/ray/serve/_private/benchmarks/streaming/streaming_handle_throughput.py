import click

from ray import serve
from ray.serve._private.benchmarks.streaming.common import Endpoint, Caller, IOMode


@serve.deployment(ray_actor_options={"num_cpus": 0})
class EndpointDeployment(Endpoint):
    pass


@serve.deployment
class CallerDeployment(Caller):

    async def _consume_single_stream(self):
        async for r in self._h.options(
            use_new_handle_api=True,
            stream=True,
        ).stream.remote():
            # Blackhole the response
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
    app = CallerDeployment.bind(
        EndpointDeployment.options(num_replicas=num_replicas).bind(tokens_per_request),
        mode=IOMode(io_mode.upper()),
        tokens_per_request=tokens_per_request,
        batch_size=batch_size,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    )
    h = serve.run(app).options(
        use_new_handle_api=True,
    )

    mean, stddev = h.run_benchmark.remote().result()
    print(
        "DeploymentHandle streaming throughput ({}) {}: {} +- {} tokens/s".format(
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
