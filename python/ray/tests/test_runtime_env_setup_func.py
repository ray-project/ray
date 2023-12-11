import threading
import os
import sys
import logging

import pytest

import ray


def _hook():
    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize("is_module", [False, True])
def test_setup_func_basic(shutdown_only, is_module):
    def configure_logging(level: int):
        logger = logging.getLogger("")
        logger.setLevel(level)

    if is_module:
        runtime_env = {
            "worker_process_setup_hook": "ray.tests.test_runtime_env_setup_func._hook",  # noqa
            "env_vars": {"ABC": "123"},
        }
    else:
        runtime_env = {
            "worker_process_setup_hook": lambda: configure_logging(logging.DEBUG),
            "env_vars": {"ABC": "123"},
        }

    ray.init(num_cpus=1, runtime_env=runtime_env)

    @ray.remote
    def f(level):
        logger = logging.getLogger("")
        assert logging.getLevelName(logger.getEffectiveLevel()) == level
        return True

    @ray.remote
    class Actor:
        def __init__(self, level):
            logger = logging.getLogger("")
            assert logging.getLevelName(logger.getEffectiveLevel()) == level

        def ready(self):
            return True

        def get_env_var(self, key):
            return os.getenv(key)

    # Test basic.
    for _ in range(10):
        assert ray.get(f.remote("DEBUG"))
    a = Actor.remote("DEBUG")
    assert ray.get(a.__ray_ready__.remote())

    # Make sure env var is not overwritten.
    assert ray.get(a.get_env_var.remote("ABC")) == "123"

    # Test override.
    # TODO(sang)
    # ray.get(
    #     f.options(
    #         runtime_env={
    #             "worker_process_setup_hook": lambda: configure_logging(logging.INFO)}
    #     ).remote("INFO"))
    # a = Actor.optinos(
    #     runtime_env={
    #       "worker_process_setup_hook": lambda: configure_logging(logging.INFO)
    #     }
    # ).remote("INFO")
    # assert ray.get(a.__ray_ready__.remote())


def test_setup_func_failure(shutdown_only):
    """
    Verify when deserilization failed, it raises an exception.
    """

    class CustomClass:
        """
        Custom class that can serialize but canont deserialize.
        It is used to test deserialization failure.
        """

        def __getstate__(self):
            # This method is called during serialization
            return self.__dict__

        def __setstate__(self, state):
            # This method is called during deserialization
            raise RuntimeError("Deserialization not allowed")

    c = CustomClass()

    def setup():
        print(c)

    ray.init(
        num_cpus=1,
        runtime_env={
            "worker_process_setup_hook": setup,
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    # TODO(sang): Maybe we should raise RuntimeEnvSetupError?
    # It is pretty difficult now. See
    # https://github.com/ray-project/ray/pull/34738#discussion_r1189553716
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Deserialization not allowed" in str(e.value)

    """
    Verify when the serialization fails, ray.init fails.
    """
    ray.shutdown()
    lock = threading.Lock()

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as e:
        ray.init(
            num_cpus=0,
            runtime_env={
                "worker_process_setup_hook": lambda: print(lock),
            },
        )
    assert "Failed to export the setup function." in str(e.value)

    """
    Verify when the setup hook failed, it raises an exception.
    """
    ray.shutdown()

    def setup_func():
        raise ValueError("Setup Failed")

    ray.init(
        num_cpus=1,
        runtime_env={
            "worker_process_setup_hook": setup_func,
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Setup Failed" in str(e.value)
    assert "Failed to execute the setup hook method." in str(e.value)


def test_setup_hook_module_failure(shutdown_only):
    # Use a module that cannot be found.
    ray.init(
        runtime_env={
            "worker_process_setup_hook": (
                "ray.tests.test_runtime_env_setup_func._hooks"
            )
        },
    )

    @ray.remote
    class A:
        pass

    a = A.remote()
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.__ray_ready__.remote())
    assert "Failed to execute the setup hook method" in str(e.value)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
