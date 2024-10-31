from ray import serve
from ray.serve.handle import DeploymentHandle

# TODO:
# - support local http client.
# - support composing DeploymentResponse objects.

def test_ingress_handle():
    @serve.deployment
    class D:
        def __init__(self, my_name: str):
            self._my_name = my_name

        def __call__(self, name: str):
            return f"Hello {name} from {self._my_name}!"

    h = serve.run(D.bind("Theodore"), local_testing_mode=True)
    assert isinstance(h, DeploymentHandle)
    assert h.remote("Edith").result() == "Hello Edith from Theodore!"


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

    h = serve.run(Outer.bind("Theodore", Inner.bind("Kevin")), local_testing_mode=True)
    assert isinstance(h, DeploymentHandle)
    assert h.remote("Edith").result() == "Hello Edith from Theodore and Kevin!"
