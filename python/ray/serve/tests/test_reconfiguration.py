import pytest
import asyncio
from ray import serve
import ray


def test_reconfiguration():
    @serve.deployment(max_concurrent_queries=10)
    class A:
        def __init__(self):
            self.state = None

        def reconfigure(self, config):
            self.state = config

        async def __call__(self):
            # change sleep to SignalActor for testing
            await asyncio.sleep(3)
            return self.state["a"]

    serve.start()
    A.options(version="1", user_config={"a": 1}).deploy()
    handle = A.get_handle()
    refs = []
    for _ in range(20):
        refs.append(handle.remote())
    A.options(version="1", user_config={"a": 2}).deploy()
    for ref in refs:
        assert ray.get(ref) == 1
    ref = handle.remote()
    assert ray.get(ref) == 2


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
