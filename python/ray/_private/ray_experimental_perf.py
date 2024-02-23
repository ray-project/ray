"""This is the script for `ray microbenchmark`."""

import logging
from ray._private.ray_microbenchmark_helpers import timeit
import multiprocessing
import ray

import ray.experimental.channel as ray_channel
from ray.dag import InputNode, MultiOutputNode

logger = logging.getLogger(__name__)


@ray.remote
class DAGActor:
    def echo(self, x):
        return x


def check_optimized_build():
    if not ray._raylet.OPTIMIZED:
        msg = (
            "WARNING: Unoptimized build! "
            "To benchmark an optimized build, try:\n"
            "\tbazel build -c opt //:ray_pkg\n"
            "You can also make this permanent by adding\n"
            "\tbuild --compilation_mode=opt\n"
            "to your user-wide ~/.bazelrc file. "
            "(Do not add this to the project-level .bazelrc file.)"
        )
        logger.warning(msg)


def main(results=None):
    results = results or []

    check_optimized_build()

    print("Tip: set TESTS_TO_RUN='pattern' to run a subset of benchmarks")

    #################################################
    # Perf tests for channels, used in compiled DAGs.
    #################################################
    ray.init()

    def put_channel_small(chans, do_get=False, do_release=False):
        for chan in chans:
            chan.write(b"0")
            if do_get:
                chan.begin_read()
            if do_release:
                chan.end_read()

    @ray.remote
    class ChannelReader:
        def ready(self):
            return

        def read(self, chans):
            while True:
                for chan in chans:
                    chan.begin_read()
                    chan.end_read()

    chans = [ray_channel.Channel(1000)]
    results += timeit(
        "[unstable] local put, single channel calls",
        lambda: put_channel_small(chans, do_release=True),
    )
    results += timeit(
        "[unstable] local put:local get, single channel calls",
        lambda: put_channel_small(chans, do_get=True, do_release=True),
    )

    chans = [ray_channel.Channel(1000)]
    reader = ChannelReader.remote()
    ray.get(reader.ready.remote())
    reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:1 remote get, single channel calls",
        lambda: put_channel_small(chans),
    )
    ray.kill(reader)

    n_cpu = multiprocessing.cpu_count() // 2
    print(f"Testing multiple readers/channels, n={n_cpu}")

    chans = [ray_channel.Channel(1000, num_readers=n_cpu)]
    readers = [ChannelReader.remote() for _ in range(n_cpu)]
    ray.get([reader.ready.remote() for reader in readers])
    for reader in readers:
        reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:n remote get, single channel calls",
        lambda: put_channel_small(chans),
    )
    for reader in readers:
        ray.kill(reader)

    chans = [ray_channel.Channel(1000) for _ in range(n_cpu)]
    reader = ChannelReader.remote()
    ray.get(reader.ready.remote())
    reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:1 remote get, n channels calls",
        lambda: put_channel_small(chans),
    )
    ray.kill(reader)

    chans = [ray_channel.Channel(1000) for _ in range(n_cpu)]
    readers = [ChannelReader.remote() for _ in range(n_cpu)]
    ray.get([reader.ready.remote() for reader in readers])
    for chan, reader in zip(chans, readers):
        reader.read.remote([chan])
    results += timeit(
        "[unstable] local put:n remote get, n channels calls",
        lambda: put_channel_small(chans),
    )
    for reader in readers:
        ray.kill(reader)

    # Tests for compiled DAGs.

    def _exec(dag):
        output_channel = dag.execute(b"x")
        output_channel.begin_read()
        output_channel.end_read()

    def _exec_multi_output(dag):
        output_channels = dag.execute(b"x")
        for output_channel in output_channels:
            output_channel.begin_read()
        for output_channel in output_channels:
            output_channel.end_read()

    a = DAGActor.remote()
    with InputNode() as inp:
        dag = a.echo.bind(inp)

    results += timeit(
        "[unstable] single-actor DAG calls", lambda: ray.get(dag.execute(b"x"))
    )
    dag = dag.experimental_compile()
    results += timeit("[unstable] compiled single-actor DAG calls", lambda: _exec(dag))

    del a
    n_cpu = multiprocessing.cpu_count() // 2
    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = MultiOutputNode([a.echo.bind(inp) for a in actors])
    results += timeit(
        "[unstable] scatter-gather DAG calls, n={n_cpu} actors",
        lambda: ray.get(dag.execute(b"x")),
    )
    dag = dag.experimental_compile()
    results += timeit(
        f"[unstable] compiled scatter-gather DAG calls, n={n_cpu} actors",
        lambda: _exec_multi_output(dag),
    )

    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = inp
        for a in actors:
            dag = a.echo.bind(dag)
    results += timeit(
        f"[unstable] chain DAG calls, n={n_cpu} actors",
        lambda: ray.get(dag.execute(b"x")),
    )
    dag = dag.experimental_compile()
    results += timeit(
        f"[unstable] compiled chain DAG calls, n={n_cpu} actors", lambda: _exec(dag)
    )

    ray.shutdown()

    ############################
    # End of channel perf tests.
    ############################

    return results


if __name__ == "__main__":
    main()
