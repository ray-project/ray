import pytest
import asyncio
from ray import serve
import ray

def test_reconfiguration():
    @serve.deployment
    class A:
        def __init__(self):
            self.state = None

        def reconfigure(self, config):
            self.state = config

        async def __call__(self):
            print("before", self.state)
            # change sleep to SignalActor for testing
            await asyncio.sleep(5)
            print("after", self.state)
            return self.state["a"]

    serve.start()
    A.options(version="1", user_config={"a": 1}).deploy()
    handle1 = A.get_handle()
    handle2 = A.get_handle()
    ref1 = handle1.remote()
    ref2 = handle2.remote()
    A.options(version="1", user_config={"a": 2}).deploy()
    ref3 = handle1.remote()
    assert ray.get(ref1) == 1
    assert ray.get(ref2) == 1
    assert ray.get(ref3) == 2

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
