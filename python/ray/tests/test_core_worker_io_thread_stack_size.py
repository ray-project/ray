import os
import sys
import pytest
import ray


@pytest.mark.asyncio
async def test_core_worker_io_thread_stack_size(shutdown_only):
    # Test to make sure core worker io thread
    # has big enough stack size to run python code.
    # See https://github.com/ray-project/ray/issues/41094 for more details.
    ray.init()

    @ray.remote(num_cpus=0)
    def task():
        import numpy

        return numpy.zeros(1)

    assert (await task.remote()).tolist() == [0]


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
