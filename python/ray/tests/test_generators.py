import pytest
import numpy as np
import sys

import ray


def test_generator_oom(ray_start_regular):
    @ray.remote(max_retries=0)
    def large_values(num_returns):
        return [
            np.random.randint(
                np.iinfo(np.int8).max, size=(100_000_000, 1), dtype=np.int8
            )
            for _ in range(num_returns)
        ]

    @ray.remote(max_retries=0)
    def large_values_generator(num_returns):
        for _ in range(num_returns):
            yield np.random.randint(
                np.iinfo(np.int8).max, size=(100_000_000, 1), dtype=np.int8
            )

    num_returns = 100
    try:
        # Worker may OOM using normal returns.
        ray.get(large_values.options(num_returns=num_returns).remote(num_returns)[0])
    except ray.exceptions.WorkerCrashedError:
        pass

    # Using a generator will allow the worker to finish.
    ray.get(
        large_values_generator.options(num_returns=num_returns).remote(num_returns)[0]
    )


@pytest.mark.parametrize("use_actors", [False, True])
def test_generator_returns(ray_start_regular, use_actors):
    remote_generator_fn = None
    if use_actors:

        @ray.remote
        class Generator:
            def __init__(self):
                pass

            def generator(self, num_returns):
                for i in range(num_returns):
                    yield i

        g = Generator.remote()
        remote_generator_fn = g.generator
    else:

        @ray.remote(max_retries=0)
        def generator(num_returns):
            for i in range(num_returns):
                yield i

        remote_generator_fn = generator

    # Check cases when num_returns does not match the number of values returned
    # by the generator.
    num_returns = 3

    try:
        ray.get(
            remote_generator_fn.options(num_returns=num_returns).remote(num_returns - 1)
        )
        assert False
    except ray.exceptions.RayTaskError as e:
        assert isinstance(e.as_instanceof_cause(), ValueError)

    try:
        ray.get(
            remote_generator_fn.options(num_returns=num_returns).remote(num_returns + 1)
        )
        assert False
    except ray.exceptions.RayTaskError as e:
        assert isinstance(e.as_instanceof_cause(), ValueError)

    # Check return values.
    ray.get(
        remote_generator_fn.options(num_returns=num_returns).remote(num_returns)
    ) == list(range(num_returns))


def test_dynamic_generator(ray_start_regular):
    @ray.remote
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            print("YIELD", i)
            yield np.random.randint(
                np.iinfo(np.int8).max, size=(100_000_000, 1), dtype=np.int8
            )

    gen = ray.get(dynamic_generator.remote(10))
    print(gen, gen.refs)
    for i, ref in enumerate(gen):
        print(ref, ray.get(ref))


def test_dynamic_generator_reconstruction(ray_start_cluster):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
        "fetch_warn_timeout_milliseconds": 1000,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0, _system_config=config, enable_object_reconstruction=True
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    cluster.wait_for_nodes()

    @ray.remote
    def dynamic_generator(num_returns):
        for _ in range(num_returns):
            yield np.random.randint(
                np.iinfo(np.int8).max, size=(1_000_000, 1), dtype=np.int8
            )

    @ray.remote
    def fetch(x):
        return

    # Test recovery of all dynamic objects through re-execution.
    gen = ray.get(dynamic_generator.remote(10))
    print(gen)
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    refs = list(gen)
    for ref in refs:
        ray.get(fetch.remote(ref))

    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10 ** 8)

    # Fetch one of the ObjectRefs to another node. We should try to reuse this
    # copy during recovery.
    ray.get(fetch.options(resources={"node2": 1}).remote(refs[-1]))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    for ref in refs:
        ray.get(ref)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
