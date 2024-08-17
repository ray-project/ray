"""This is the script for `ray microbenchmark`."""

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

    def _exec(dag):
        output_ref = dag.execute(b"x")
        ray.get(output_ref)

    async def exec_async(tag):
        async def _exec_async():
            fut = await compiled_dag.execute_async(b"x")
            await fut

        return await asyncio_timeit(
            tag,
            _exec_async,
        )

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
    compiled_dag.teardown()

    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(
            "[unstable] compiled single-actor asyncio DAG calls",
        )
    )
    # TODO: Need to explicitly tear down DAGs with enable_asyncio=True because
    # these DAGs create a background thread that can segfault if the CoreWorker
    # is torn down first.
    compiled_dag.teardown()

    del a
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
    compiled_dag.teardown()

    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(
            f"[unstable] compiled scatter-gather asyncio DAG calls, n={n_cpu} actors",
        )
    )
    # TODO: Need to explicitly tear down DAGs with enable_asyncio=True because
    # these DAGs create a background thread that can segfault if the CoreWorker
    # is torn down first.
    compiled_dag.teardown()

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
    compiled_dag.teardown()

    compiled_dag = dag.experimental_compile(enable_asyncio=True)
    results += loop.run_until_complete(
        exec_async(f"[unstable] compiled chain asyncio DAG calls, n={n_cpu} actors")
    )
    # TODO: Need to explicitly tear down DAGs with enable_asyncio=True because
    # these DAGs create a background thread that can segfault if the CoreWorker
    # is torn down first.
    compiled_dag.teardown()

    ray.shutdown()

    return results


if __name__ == "__main__":
    main()
