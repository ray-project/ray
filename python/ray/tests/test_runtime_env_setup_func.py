import os
import sys
import logging

import pytest

import ray


def test_setup_func_basic(shutdown_only):
    def configure_logging(level: int):
        logger = logging.getLogger("")
        logger.setLevel(level)

    ray.init(
        num_cpus=1,
        runtime_env={
            "worker_setup_func": lambda: configure_logging(logging.DEBUG),
            "env_vars": {"ABC": "123"},
        },
    )

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

    # Test basic.
    for _ in range(10):
        assert ray.get(f.remote("DEBUG"))
    a = Actor.remote("DEBUG")
    assert ray.get(a.__ray_ready__.remote())

    # Test override.
    # SANG-TODO
    # ray.get(
    #     f.options(
    #         runtime_env={
    #             "worker_setup_func": lambda: configure_logging(logging.INFO)}
    #     ).remote("INFO"))
    # a = Actor.optinos(
    #     runtime_env={"worker_setup_func": lambda: configure_logging(logging.INFO)}
    # ).remote("INFO")
    # assert ray.get(a.__ray_ready__.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
