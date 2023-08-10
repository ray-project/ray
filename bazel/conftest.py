import pytest

import ray


@pytest.fixture(autouse=True, scope="module")
def shutdown_ray():
    ray.shutdown()
    yield


@pytest.fixture(autouse=True)
def preserve_block_order():
    ray.data.context.DataContext.get_current().execution_options.preserve_order = True
    yield
