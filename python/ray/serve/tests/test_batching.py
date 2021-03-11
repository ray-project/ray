import pytest

import ray
from ray import serve
from ray.serve.config import BackendConfig


def test_batching(serve_instance):
    client = serve_instance

    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            self.count += 1
            batch_size = len(requests)
            return [self.count] * batch_size

    # set the max batch size
    config = BackendConfig(max_batch_size=5, batch_wait_timeout=1)
    client.create_backend("counter:v11", BatchingExample, config=config)
    client.create_endpoint(
        "counter1", backend="counter:v11", route="/increment2")

    future_list = []
    handle = client.get_handle("counter1")
    for _ in range(20):
        f = handle.remote(temp=1)
        future_list.append(f)

    counter_result = ray.get(future_list)
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max(counter_result) < 20


def test_batching_exception(serve_instance):
    client = serve_instance

    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            return len(requests)

    # Set the max batch size.
    config = BackendConfig(max_batch_size=5)
    client.create_backend("exception:v1", NoListReturned, config=config)
    client.create_endpoint("exception-test", backend="exception:v1")

    handle = client.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
