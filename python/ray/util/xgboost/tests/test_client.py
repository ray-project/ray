import pytest
import sys

import ray
from ray.util.client.ray_client_helpers import ray_start_client_server


@pytest.fixture
def start_client_server():
    with ray_start_client_server() as client:
        yield client


def test_simple_example(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.util.xgboost.simple_example import main

    main()


def test_simple_tune(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.util.xgboost.simple_tune import main

    main()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
