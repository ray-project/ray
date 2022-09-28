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
@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_generator_returns(ray_start_regular, use_actors, store_in_plasma):
    remote_generator_fn = None
    if use_actors:

        @ray.remote
        class Generator:
            def __init__(self):
                pass

            def generator(self, num_returns, store_in_plasma):
                for i in range(num_returns):
                    if store_in_plasma:
                        yield np.ones(1_000_000, dtype=np.int8) * i
                    else:
                        yield [i]

        g = Generator.remote()
        remote_generator_fn = g.generator
    else:

        @ray.remote(max_retries=0)
        def generator(num_returns, store_in_plasma):
            for i in range(num_returns):
                if store_in_plasma:
                    yield np.ones(1_000_000, dtype=np.int8) * i
                else:
                    yield [i]

        remote_generator_fn = generator

    # Check cases when num_returns does not match the number of values returned
    # by the generator.
    num_returns = 3

    try:
        ray.get(
            remote_generator_fn.options(num_returns=num_returns).remote(
                num_returns - 1, store_in_plasma
            )
        )
        assert False
    except ray.exceptions.RayTaskError as e:
        assert isinstance(e.as_instanceof_cause(), ValueError)

    if store_in_plasma:
        # TODO(swang): Currently there is a bug when a generator errors after
        # already storing some values in plasma. The already stored values can
        # be accessed and the error is lost.  Propagate the error correctly by
        # replacing all successfully stored return values with the same error.
        ray.get(
            remote_generator_fn.options(num_returns=num_returns).remote(
                num_returns + 1, store_in_plasma
            )
        )
    else:
        try:
            ray.get(
                remote_generator_fn.options(num_returns=num_returns).remote(
                    num_returns + 1, store_in_plasma
                )
            )
            assert False
        except ray.exceptions.RayTaskError as e:
            assert isinstance(e.as_instanceof_cause(), ValueError)

    # Check return values.
    [
        x[0]
        for x in ray.get(
            remote_generator_fn.options(num_returns=num_returns).remote(
                num_returns, store_in_plasma
            )
        )
    ] == list(range(num_returns))
    # Works for num_returns=1 if generator returns a single value.
    assert (
        ray.get(remote_generator_fn.options(num_returns=1).remote(1, store_in_plasma))[
            0
        ]
        == 0
    )


@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_generator_errors(ray_start_regular, store_in_plasma):
    @ray.remote(max_retries=0)
    def generator(num_returns, store_in_plasma):
        for i in range(num_returns - 2):
            if store_in_plasma:
                yield np.ones(1_000_000, dtype=np.int8) * i
            else:
                yield [i]
        raise Exception("error")

    ref1, ref2, ref3 = generator.options(num_returns=3).remote(3, store_in_plasma)
    # TODO(swang): Currently there is a bug when a generator errors after
    # already storing some values in plasma. The already stored values can
    # be accessed and the error is lost.  Propagate the error correctly by
    # replacing all successfully stored return values with the same error.
    if not store_in_plasma:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref1)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref2)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref3)

    dynamic_ref = generator.options(num_returns="dynamic").remote(3, store_in_plasma)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(dynamic_ref)


@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_dynamic_generator(ray_start_regular, store_in_plasma):
    @ray.remote(num_returns="dynamic")
    def dynamic_generator(num_returns, store_in_plasma):
        for i in range(num_returns):
            if store_in_plasma:
                yield np.ones(1_000_000, dtype=np.int8) * i
            else:
                yield [i]

    @ray.remote
    def read(gen):
        for i, ref in enumerate(gen):
            if ray.get(ref)[0] != i:
                return False
        return True

    gen = ray.get(dynamic_generator.remote(10, store_in_plasma))
    for i, ref in enumerate(gen):
        assert ray.get(ref)[0] == i

    # Test empty generator.
    gen = ray.get(dynamic_generator.remote(0, store_in_plasma))
    assert len(gen) == 0

    # Check that passing as task arg.
    gen = dynamic_generator.remote(10, store_in_plasma)
    assert ray.get(read.remote(gen))
    assert ray.get(read.remote(ray.get(gen)))

    # Generator remote functions with num_returns=1 error if they return more
    # than 1 value.
    # TODO(swang): Currently there is a bug when a generator errors after
    # already storing some values in plasma. The already stored values can
    # be accessed and the error is lost. Propagate the error correctly by
    # replacing all successfully stored return values with the same error.
    if not store_in_plasma:
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(dynamic_generator.options(num_returns=1).remote(3, store_in_plasma))

    # Normal remote functions don't work with num_returns="dynamic".
    @ray.remote(num_returns="dynamic")
    def static(num_returns):
        return list(range(num_returns))

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(static.remote(3))


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
            # Random ray.put to make sure it's okay to interleave these with
            # the dynamic returns.
            if np.random.randint(2) == 1:
                ray.put(np.ones(1_000_000, dtype=np.int8) * np.random.randint(100))
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


def test_dynamic_generator_reconstruction_fails(ray_start_cluster):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
        "fetch_warn_timeout_milliseconds": 1000,
    }
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=1,
        _system_config=config,
        enable_object_reconstruction=True,
        resources={"head": 1},
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"head": 1})
    class FailureSignal:
        def __init__(self):
            return

        def ping(self):
            return

    @ray.remote(num_returns="dynamic")
    def dynamic_generator(failure_signal):
        num_returns = 10
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i
            if i == num_returns // 2:
                # If this is the re-execution, fail the worker after partial yield.
                try:
                    ray.get(failure_signal.ping.remote())
                except ray.exceptions.RayActorError:
                    sys.exit(-1)

    @ray.remote
    def fetch(*refs):
        pass

    failure_signal = FailureSignal.remote()
    gen = ray.get(dynamic_generator.remote(failure_signal))
    refs = list(gen)
    ray.get(fetch.remote(*refs))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    done = fetch.remote(*refs)

    ray.kill(failure_signal)
    # Make sure we can get the error.
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        for ref in refs:
            ray.get(ref)
    # Make sure other tasks can also get the error.
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(done)


def test_dynamic_empty_generator_reconstruction_nondeterministic(ray_start_cluster):
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

        def get_count(self):
            return self.count

    @ray.remote(num_returns="dynamic")
    def maybe_empty_generator(exec_counter):
        if ray.get(exec_counter.inc.remote()) > 1:
            for i in range(3):
                yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def check(empty_generator):
        return len(empty_generator) == 0

    exec_counter = ExecutionCounter.remote()
    gen = maybe_empty_generator.remote(exec_counter)
    assert ray.get(check.remote(gen))
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10 ** 8)
    assert ray.get(check.remote(gen))

    # We should never reconstruct an empty generator.
    assert ray.get(exec_counter.get_count.remote()) == 1


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
