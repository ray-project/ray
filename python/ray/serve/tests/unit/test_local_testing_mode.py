import os
import sys

import pytest
import logging
from ray.serve._private.constants import SERVE_LOGGER_NAME
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


def test_dictionary_logging_config_with_local_mode():
    """Test that the logging config can be passed as a dictionary.

    See: https://github.com/ray-project/ray/issues/50052
    """

    @serve.deployment
    class MyApp:
        def __call__(self):
            logger = logging.getLogger(SERVE_LOGGER_NAME)
            return logger.level

    app = MyApp.bind()
    logging_config = {"log_level": "WARNING"}

    # This should not raise exception.
    h = serve.run(app, logging_config=logging_config, _local_testing_mode=True)

    # The logger should be setup with WARNING level.
    assert h.remote().result() == logging.WARNING


def test_deploy_multiple_apps_batched() -> None:
    @serve.deployment
    class A:
        def __call__(self):
            return "a"

    @serve.deployment
    class B:
        def __call__(self):
            return "b"

    a, b = serve.run_many(
        [
            serve.RunTarget(A.bind(), name="a", route_prefix="/a"),
            serve.RunTarget(B.bind(), name="b", route_prefix="/b"),
        ],
        _local_testing_mode=True,
    )

    assert a.remote().result() == "a"
    assert b.remote().result() == "b"


def test_redeploy_multiple_apps_batched() -> None:
    @serve.deployment
    class A:
        def __call__(self):
            return "a", os.getpid()

    @serve.deployment
    class V1:
        def __call__(self):
            return "version 1", os.getpid()

    @serve.deployment
    class V2:
        def __call__(self):
            return "version 2", os.getpid()

    a_handle, v1_handle = serve.run_many(
        [
            serve.RunTarget(A.bind(), name="a", route_prefix="/a"),
            serve.RunTarget(V1.bind(), name="v", route_prefix="/v"),
        ],
        _local_testing_mode=True,
    )

    a1, pida1 = a_handle.remote().result()

    assert a1 == "a"

    v1, pid1 = v1_handle.remote().result()

    assert v1 == "version 1"

    (v2_handle,) = serve.run_many(
        [
            serve.RunTarget(V2.bind(), name="v", route_prefix="/v"),
        ],
        _local_testing_mode=True,
    )

    v2, pid2 = v2_handle.remote().result()

    assert v2 == "version 2"
    assert pid1 == pid2  # because local testing mode, so it's all in-process

    # Redeploying "v" should not have affected "a"
    a2, pida2 = a_handle.remote().result()

    assert a1 == a2
    assert pida1 == pida2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
