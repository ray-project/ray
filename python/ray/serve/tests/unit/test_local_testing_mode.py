import sys

import pytest

from ray import serve
from ray.serve.handle import DeploymentHandle


def test_basic_composition():
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


@pytest.mark.parametrize("deployment", ["Inner", "Outer"])
def test_exception_raised_in_constructor(deployment: str):
    @serve.deployment
    class Inner:
        def __init__(self, should_raise: bool):
            if should_raise:
                raise RuntimeError("Exception in Inner constructor.")

    @serve.deployment
    class Outer:
        def __init__(self, h: DeploymentHandle, should_raise: bool):
            if should_raise:
                raise RuntimeError("Exception in Outer constructor.")

    with pytest.raises(RuntimeError, match=f"Exception in {deployment} constructor."):
        serve.run(
            Outer.bind(Inner.bind(deployment == "Inner"), deployment == "Outer"),
            _local_testing_mode=True,
        )


def test_to_object_ref_error_message():
    @serve.deployment
    class Inner:
        pass

    @serve.deployment
    class Outer:
        def __init__(self, h: DeploymentHandle):
            self._h = h

        async def __call__(self):
            with pytest.raises(
                RuntimeError,
                match=(
                    "Converting DeploymentResponses to ObjectRefs "
                    "is not supported in local testing mode."
                ),
            ):
                await self._h.remote()._to_object_ref()

    h = serve.run(Outer.bind(Inner.bind()), _local_testing_mode=True)
    with pytest.raises(
        RuntimeError,
        match=(
            "Converting DeploymentResponses to ObjectRefs "
            "is not supported in local testing mode."
        ),
    ):
        h.remote()._to_object_ref_sync()

    # Test the inner handle case (this would raise if it failed).
    h.remote().result()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
