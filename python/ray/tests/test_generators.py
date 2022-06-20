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

    # Check num_returns=1 case, should receive TypeError because generator
    # cannot be pickled.
    try:
        ray.get(remote_generator_fn.remote(num_returns))
        assert False
    except ray.exceptions.RayTaskError as e:
        assert isinstance(e.as_instanceof_cause(), TypeError)

    # Check return values.
    ray.get(
        remote_generator_fn.options(num_returns=num_returns).remote(num_returns)
    ) == list(range(num_returns))


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
