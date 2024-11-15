"""This is the script for `ray microbenchmark`."""

import asyncio
import logging
from ray._private.ray_microbenchmark_helpers import timeit, asyncio_timeit
import multiprocessing
import ray
from ray.dag.compiled_dag_node import CompiledDAG
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import ray.experimental.channel as ray_channel
from ray.dag import InputNode, MultiOutputNode
from ray._private.utils import (
    get_or_create_event_loop,
)
from ray._private.test_utils import get_actor_node_id

logger = logging.getLogger(__name__)


@ray.remote
class DAGActor:
    def echo(self, x):
        return x

    def echo_multiple(self, *x):
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


def create_driver_actor():
    return CompiledDAG.DAGDriverProxyActor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
    ).remote()


def main(results=None):
    results = results or []
    loop = get_or_create_event_loop()

    check_optimized_build()

    print("Tip: set TESTS_TO_RUN='pattern' to run a subset of benchmarks")

    #################################################
    # Perf tests for channels, used in compiled DAGs.
    #################################################
    ray.init()

    def put_channel_small(chans, do_get=False):
        for chan in chans:
            chan.write(b"0")
            if do_get:
                chan.read()

    @ray.remote
    class ChannelReader:
        def ready(self):
            return

        def read(self, chans):
            while True:
                for chan in chans:
                    chan.read()

    driver_actor = create_driver_actor()
    driver_node = get_actor_node_id(driver_actor)
    chans = [ray_channel.Channel(None, [(driver_actor, driver_node)], 1000)]
    results += timeit(
        "[unstable] local put:local get, single channel calls",
        lambda: put_channel_small(chans, do_get=True),
    )

    reader = ChannelReader.remote()
    reader_node = get_actor_node_id(reader)
    chans = [ray_channel.Channel(None, [(reader, reader_node)], 1000)]
    ray.get(reader.ready.remote())
    reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:1 remote get, single channel calls",
        lambda: put_channel_small(chans),
    )
    ray.kill(reader)

    n_cpu = multiprocessing.cpu_count() // 2
    print(f"Testing multiple readers/channels, n={n_cpu}")

    reader_and_node_list = []
    for _ in range(n_cpu):
        reader = ChannelReader.remote()
        reader_node = get_actor_node_id(reader)
        reader_and_node_list.append((reader, reader_node))
    chans = [ray_channel.Channel(None, reader_and_node_list, 1000)]
    ray.get([reader.ready.remote() for reader, _ in reader_and_node_list])
    for reader, _ in reader_and_node_list:
        reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:n remote get, single channel calls",
        lambda: put_channel_small(chans),
    )
    for reader, _ in reader_and_node_list:
        ray.kill(reader)

    reader = ChannelReader.remote()
    reader_node = get_actor_node_id(reader)
    chans = [
        ray_channel.Channel(None, [(reader, reader_node)], 1000) for _ in range(n_cpu)
    ]
    ray.get(reader.ready.remote())
    reader.read.remote(chans)
    results += timeit(
        "[unstable] local put:1 remote get, n channels calls",
        lambda: put_channel_small(chans),
    )
    ray.kill(reader)

    reader_and_node_list = []
    for _ in range(n_cpu):
        reader = ChannelReader.remote()
        reader_node = get_actor_node_id(reader)
        reader_and_node_list.append((reader, reader_node))
    chans = [
        ray_channel.Channel(None, [reader_and_node_list[i]], 1000) for i in range(n_cpu)
    ]
    ray.get([reader.ready.remote() for reader, _ in reader_and_node_list])
    for chan, reader_node_tuple in zip(chans, reader_and_node_list):
        reader = reader_node_tuple[0]
        reader.read.remote([chan])
    results += timeit(
        "[unstable] local put:n remote get, n channels calls",
        lambda: put_channel_small(chans),
    )
    for reader, _ in reader_and_node_list:
        ray.kill(reader)

    # Tests for compiled DAGs.

    def _exec(dag, num_args=1, payload_size=1):
        output_ref = dag.execute(*[b"x" * payload_size for _ in range(num_args)])
        ray.get(output_ref)

    async def exec_async(tag):
        async def _exec_async():
            fut = await compiled_dag.execute_async(b"x")
            if not isinstance(fut, list):
                await fut
            else:
                await asyncio.gather(*fut)

        return await asyncio_timeit(
            tag,
            _exec_async,
        )

    # Single-actor DAG calls

    a = DAGActor.remote()
    with InputNode() as inp:
        dag = a.echo.bind(inp)

    results += timeit(
        "[unstable] single-actor DAG calls", lambda: ray.get(dag.execute(b"x"))
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        "[unstable] compiled single-actor DAG calls", lambda: _exec(compiled_dag)
    )
    del a

    # Single-actor asyncio DAG calls

    a = DAGActor.remote()
    with InputNode() as inp:
        dag = a.echo.bind(inp)
    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(
            "[unstable] compiled single-actor asyncio DAG calls",
        )
    )
    del a

    # Scatter-gather DAG calls

    n_cpu = multiprocessing.cpu_count() // 2
    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = MultiOutputNode([a.echo.bind(inp) for a in actors])
    results += timeit(
        f"[unstable] scatter-gather DAG calls, n={n_cpu} actors",
        lambda: ray.get(dag.execute(b"x")),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        f"[unstable] compiled scatter-gather DAG calls, n={n_cpu} actors",
        lambda: _exec(compiled_dag),
    )

    # Scatter-gather asyncio DAG calls

    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = MultiOutputNode([a.echo.bind(inp) for a in actors])
    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(
            f"[unstable] compiled scatter-gather asyncio DAG calls, n={n_cpu} actors",
        )
    )

    # Chain DAG calls

    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = inp
        for a in actors:
            dag = a.echo.bind(dag)
    results += timeit(
        f"[unstable] chain DAG calls, n={n_cpu} actors",
        lambda: ray.get(dag.execute(b"x")),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        f"[unstable] compiled chain DAG calls, n={n_cpu} actors",
        lambda: _exec(compiled_dag),
    )

    # Chain asyncio DAG calls

    actors = [DAGActor.remote() for _ in range(n_cpu)]
    with InputNode() as inp:
        dag = inp
        for a in actors:
            dag = a.echo.bind(dag)
    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(f"[unstable] compiled chain asyncio DAG calls, n={n_cpu} actors")
    )

    # Multiple args with small payloads

    n_actors = 8
    assert (
        n_cpu > n_actors
    ), f"n_cpu ({n_cpu}) must be greater than n_actors ({n_actors})"

    actors = [DAGActor.remote() for _ in range(n_actors)]
    with InputNode() as inp:
        dag = MultiOutputNode([actors[i].echo.bind(inp[i]) for i in range(n_actors)])
    payload_size = 1
    results += timeit(
        f"[unstable] multiple args with small payloads DAG calls, n={n_actors} actors",
        lambda: ray.get(dag.execute(*[b"x" * payload_size for _ in range(n_actors)])),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        f"[unstable] compiled multiple args with small payloads DAG calls, "
        f"n={n_actors} actors",
        lambda: _exec(compiled_dag, num_args=n_actors, payload_size=payload_size),
    )

    # Multiple args with medium payloads

    actors = [DAGActor.remote() for _ in range(n_actors)]
    with InputNode() as inp:
        dag = MultiOutputNode([actors[i].echo.bind(inp[i]) for i in range(n_actors)])
    payload_size = 1024 * 1024
    results += timeit(
        f"[unstable] multiple args with medium payloads DAG calls, n={n_actors} actors",
        lambda: ray.get(dag.execute(*[b"x" * payload_size for _ in range(n_actors)])),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        "[unstable] compiled multiple args with medium payloads DAG calls, "
        f"n={n_actors} actors",
        lambda: _exec(compiled_dag, num_args=n_actors, payload_size=payload_size),
    )

    # Multiple args with large payloads

    actors = [DAGActor.remote() for _ in range(n_actors)]
    with InputNode() as inp:
        dag = MultiOutputNode([actors[i].echo.bind(inp[i]) for i in range(n_actors)])
    payload_size = 10 * 1024 * 1024
    results += timeit(
        f"[unstable] multiple args with large payloads DAG calls, n={n_actors} actors",
        lambda: ray.get(dag.execute(*[b"x" * payload_size for _ in range(n_actors)])),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        "[unstable] compiled multiple args with large payloads DAG calls, "
        f"n={n_actors} actors",
        lambda: _exec(compiled_dag, num_args=n_actors, payload_size=payload_size),
    )

    # Worst case for multiple arguments: a single actor takes all the arguments
    # with small payloads.

    actor = DAGActor.remote()
    n_args = 8
    with InputNode() as inp:
        dag = actor.echo_multiple.bind(*[inp[i] for i in range(n_args)])
    payload_size = 1
    results += timeit(
        "[unstable] single-actor with all args with small payloads DAG calls, "
        "n=1 actors",
        lambda: ray.get(dag.execute(*[b"x" * payload_size for _ in range(n_args)])),
    )
    compiled_dag = dag.experimental_compile()
    results += timeit(
        "[unstable] single-actor with all args with small payloads DAG calls, "
        "n=1 actors",
        lambda: _exec(compiled_dag, num_args=n_args, payload_size=payload_size),
    )

    ray.shutdown()

    return results


if __name__ == "__main__":
    main()
