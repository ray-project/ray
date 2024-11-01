import sys

import pytest

from ray import serve
from ray.serve.handle import DeploymentHandle

# TODO:
# - add test for to_object_ref error.
# - add test for exception raised in constructor.
# - run test_handle.py too.
# - support local http client.


def test_ingress_handle():
    @serve.deployment
    class D:
        def __init__(self, my_name: str):
            self._my_name = my_name

        def __call__(self, name: str):
            return f"Hello {name} from {self._my_name}!"

    h = serve.run(D.bind("Theodore"), _local_testing_mode=True)
    assert isinstance(h, DeploymentHandle)
    assert h.remote("Edith").result() == "Hello Edith from Theodore!"


def test_ingress_handle_streaming():
    @serve.deployment
    class Stream:
        def __init__(self, my_name: str):
            self._my_name = my_name

        def __call__(self, name: str, *, n: int):
            for i in range(n):
                yield f"Hello {name} from {self._my_name} ({i})!"

    h = serve.run(Stream.bind("Theodore"), _local_testing_mode=True)
    assert isinstance(h, DeploymentHandle)

    num_results = 0
    for i, result in enumerate(h.options(stream=True).remote("Edith", n=10)):
        num_results += 1
        assert result == f"Hello Edith from Theodore ({i})!"

    assert num_results == 10


def test_composed_deployment_handle():
    @serve.deployment
    class Inner:
        def __init__(self, my_name: str):
            self._my_name = my_name

        def __call__(self):
            return self._my_name

    @serve.deployment
    class Outer:
        def __init__(self, my_name: str, inner_handle: DeploymentHandle):
            assert isinstance(inner_handle, DeploymentHandle)

            self._my_name = my_name
            self._inner_handle = inner_handle

        async def __call__(self, name: str):
            inner_name = await self._inner_handle.remote()
            return f"Hello {name} from {self._my_name} and {inner_name}!"

    h = serve.run(Outer.bind("Theodore", Inner.bind("Kevin")), _local_testing_mode=True)
    assert isinstance(h, DeploymentHandle)
    assert h.remote("Edith").result() == "Hello Edith from Theodore and Kevin!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
