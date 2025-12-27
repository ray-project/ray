import gc
import sys
import time
from unittest.mock import Mock

import numpy as np
import pytest

import ray
from ray._common.test_utils import (
    wait_for_condition,
)
from ray._private.client_mode_hook import enable_client_mode
from ray.tests.conftest import call_ray_start_context
from ray.util.client.ray_client_helpers import (
    ray_start_client_server_for_address,
)


def assert_no_leak():
    def check():
        gc.collect()
        core_worker = ray._private.worker.global_worker.core_worker
        ref_counts = core_worker.get_all_reference_counts()
        for k, rc in ref_counts.items():
            if rc["local"] != 0:
                return False
            if rc["submitted"] != 0:
                return False
        return True

    wait_for_condition(check)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="This test requires Linux.",
)
def test_generator_oom(ray_start_regular_shared):
    num_returns = 100

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
def test_generator_returns(ray_start_regular_shared, use_actors, store_in_plasma):
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

    # TODO(swang): When generators return more values than expected, we log an
    # error but the exception is not thrown to the application.
    # https://github.com/ray-project/ray/issues/28689.
    ray.get(
        remote_generator_fn.options(num_returns=num_returns).remote(
            num_returns + 1, store_in_plasma
        )
    )

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


@pytest.mark.parametrize("use_actors", [False, True])
@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_generator_errors(
    ray_start_regular_shared, use_actors, store_in_plasma
):
    remote_generator_fn = None
    if use_actors:

        @ray.remote
        class Generator:
            def __init__(self):
                pass

            def generator(self, num_returns, store_in_plasma):
                for i in range(num_returns - 2):
                    if store_in_plasma:
                        yield np.ones(1_000_000, dtype=np.int8) * i
                    else:
                        yield [i]
                raise Exception("error")

        g = Generator.remote()
        remote_generator_fn = g.generator
    else:

        @ray.remote(max_retries=0)
        def generator(num_returns, store_in_plasma):
            for i in range(num_returns - 2):
                if store_in_plasma:
                    yield np.ones(1_000_000, dtype=np.int8) * i
                else:
                    yield [i]
            raise Exception("error")

        remote_generator_fn = generator

    ref1, ref2, ref3 = remote_generator_fn.options(num_returns=3).remote(
        3, store_in_plasma
    )
    ray.get(ref1)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref2)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref3)


def test_yield_exception(ray_start_cluster):
    @ray.remote
    def f():
        yield 1
        yield 2
        yield Exception("value")
        yield 3
        raise Exception("raise")
        yield 5

    gen = f.remote()
    assert ray.get(next(gen)) == 1
    assert ray.get(next(gen)) == 2
    yield_exc = ray.get(next(gen))
    assert isinstance(yield_exc, Exception)
    assert str(yield_exc) == "value"
    assert ray.get(next(gen)) == 3
    with pytest.raises(Exception, match="raise"):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))


def test_actor_yield_exception(ray_start_cluster):
    @ray.remote
    class A:
        def f(self):
            yield 1
            yield 2
            yield Exception("value")
            yield 3
            raise Exception("raise")
            yield 5

    a = A.remote()
    gen = a.f.remote()
    assert ray.get(next(gen)) == 1
    assert ray.get(next(gen)) == 2
    yield_exc = ray.get(next(gen))
    assert isinstance(yield_exc, Exception)
    assert str(yield_exc) == "value"
    assert ray.get(next(gen)) == 3
    with pytest.raises(Exception, match="raise"):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))


def test_async_actor_yield_exception(ray_start_cluster):
    @ray.remote
    class A:
        async def f(self):
            yield 1
            yield 2
            yield Exception("value")
            yield 3
            raise Exception("raise")
            yield 5

    a = A.remote()
    gen = a.f.remote()
    assert ray.get(next(gen)) == 1
    assert ray.get(next(gen)) == 2
    yield_exc = ray.get(next(gen))
    assert isinstance(yield_exc, Exception)
    assert str(yield_exc) == "value"
    assert ray.get(next(gen)) == 3
    with pytest.raises(Exception, match="raise"):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))


# Client server port of the shared Ray instance
SHARED_CLIENT_SERVER_PORT = 25555


@pytest.fixture(scope="module")
def call_ray_start_shared(request):
    request = Mock()
    request.param = (
        "ray start --head --min-worker-port=0 --max-worker-port=0 --port 0 "
        f"--ray-client-server-port={SHARED_CLIENT_SERVER_PORT}"
    )
    with call_ray_start_context(request) as address:
        yield address


@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_ray_client(call_ray_start_shared, store_in_plasma):
    with ray_start_client_server_for_address(call_ray_start_shared):
        enable_client_mode()

        @ray.remote(max_retries=0)
        def generator(num_returns, store_in_plasma):
            for i in range(num_returns):
                if store_in_plasma:
                    yield np.ones(1_000_000, dtype=np.int8) * i
                else:
                    yield [i]

        # TODO(swang): When generators return more values than expected, we log an
        # error but the exception is not thrown to the application.
        # https://github.com/ray-project/ray/issues/28689.
        num_returns = 3
        ray.get(
            generator.options(num_returns=num_returns).remote(
                num_returns + 1, store_in_plasma
            )
        )

        # Check return values.
        [
            x[0]
            for x in ray.get(
                generator.options(num_returns=num_returns).remote(
                    num_returns, store_in_plasma
                )
            )
        ] == list(range(num_returns))
        # Works for num_returns=1 if generator returns a single value.
        assert (
            ray.get(generator.options(num_returns=1).remote(1, store_in_plasma))[0] == 0
        )


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
