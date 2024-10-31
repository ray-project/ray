from ray import serve
from ray.serve.testing import LocalDeploymentHandle

# TODO:
# - assert that serve is *not* initialized?? or at least warn.
# - convert to use UserCallableWrapper.
# - implement better response types.
# - inherit from regular types (or shared ABC).
# - support http.
# - make LocalDeploymentHandle a public import?


def test_basic():
    @serve.deployment
    class D:
        def __init__(self, my_name: str):
            self._my_name = my_name

        def __call__(self, name: str):
            return f"Hello {name} from {self._my_name}!"

    h = serve.run(D.bind("Theodore"), local_testing_mode=True)
    assert isinstance(h, LocalDeploymentHandle)
    assert h.remote("Edith").result() == "Hello Edith from Theodore!"
