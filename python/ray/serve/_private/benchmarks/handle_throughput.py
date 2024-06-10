import click

from ray import serve
from ray.serve._private.benchmarks.common import Benchmarker, Hello
from ray.serve.handle import DeploymentHandle


@click.command(help="Benchmark deployment handle throughput.")
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of requests to send to downstream deployment in each trial.",
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
    default=1,
    help="Duration to run each trial of the benchmark for (seconds).",
)
def main(
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
):
    app = Benchmarker.bind(
        Hello.options(
            num_replicas=num_replicas, ray_actor_options={"num_cpus": 0}
        ).bind(),
    )
    h: DeploymentHandle = serve.run(app)

    mean, stddev = h.run_throughput_benchmark.remote(
        batch_size=batch_size,
        num_trials=num_trials,
        trial_runtime=trial_runtime,
    ).result()

    print(
        "DeploymentHandle throughput {}: {} +- {} requests/s".format(
            f"(num_replicas={num_replicas}, batch_size={batch_size})",
            mean,
            stddev,
        )
    )


if __name__ == "__main__":
    main()
