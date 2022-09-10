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
    @ray.remote(num_returns="dynamic")
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            yield np.ones(100_000_000, dtype=np.int8) * i

    gen = ray.get(dynamic_generator.remote(10))
    for i, ref in enumerate(gen):
        assert ray.get(ref)[0] == i


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

    @ray.remote(num_returns="dynamic")
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return x[0]

    # Test recovery of all dynamic objects through re-execution.
    gen = ray.get(dynamic_generator.remote(10))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    refs = list(gen)
    for i, ref in enumerate(refs):
        assert ray.get(fetch.remote(ref)) == i

    cluster.add_node(num_cpus=1, resources={"node2": 1}, object_store_memory=10 ** 8)

    # Fetch one of the ObjectRefs to another node. We should try to reuse this
    # copy during recovery.
    ray.get(fetch.options(resources={"node2": 1}).remote(refs[-1]))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    for i, ref in enumerate(refs):
        assert ray.get(fetch.remote(ref)) == i


@pytest.mark.parametrize("too_many_returns", [False, True])
def test_dynamic_generator_reconstruction_nondeterministic(
    ray_start_cluster, too_many_returns
):
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
        num_cpus=0,
        _system_config=config,
        enable_object_reconstruction=True,
        resources={"head": 1},
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=0, resources={"head": 1})
    class ExecutionCounter:
        def __init__(self):
            self.count = 0

        def inc(self):
            self.count += 1
            return self.count

    @ray.remote(num_returns="dynamic")
    def dynamic_generator(exec_counter):
        num_returns = 10
        if ray.get(exec_counter.inc.remote()) > 1:
            if too_many_returns:
                num_returns += 1
            else:
                num_returns -= 1
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return

    exec_counter = ExecutionCounter.remote()
    gen = ray.get(dynamic_generator.remote(exec_counter))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    refs = list(gen)
    if too_many_returns:
        for ref in refs:
            ray.get(ref)
    else:
        with pytest.raises(ray.exceptions.RayTaskError):
            for ref in refs:
                ray.get(ref)
    # TODO(swang): If the re-executed task returns a different number of
    # objects, we should throw an error for every return value.
    # for ref in refs:
    #     with pytest.raises(ray.exceptions.RayTaskError):
    #         ray.get(ref)


# TODO: Test cases:
# - test exception when num_returns="dynamic" but not a generator, and vice versa
# - generator errors before yield
# - generator calls ray.put, reconstruction
# - ref counting, check for leaks
# - passing ObjRefGenerator, passing generated ObjRefs

if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
