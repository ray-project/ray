# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray._private
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)


pytestmark = [
    pytest.mark.skipif(
        sys.platform != "linux" and sys.platform != "darwin",
        reason="Requires Linux or MacOS",
    ),
    pytest.mark.timeout(500),
]


def test_unhandled_error(shutdown_only, caplog):
    """Make sure unhandled error is printed.

    It should not be printed in the following scenario.
    - env var RAY_IGNORE_UNHANDLED_ERRORS is set.
    - The dag is already teardown.
    """
    caplog.set_level(logging.INFO, logger="ray.experimental.compiled_dag_ref.logger")
    ray.init()

    @ray.remote
    class A:
        def f(self, inp):
            raise RuntimeError

    a = A.remote()
    b = A.remote()
    with InputNode() as inp:
        x = b.f.bind(inp)
        y = a.f.bind(inp)
        dag = MultiOutputNode([x, y])
    adag = dag.experimental_compile()
    x, y = adag.execute(1)
    
    # Should print unhandled error.
    del x
    del y
    print(caplog)
    breakpoint()
    print(caplog.records)
    for record in caplog.records:
        print(record)


def test_unhandled_error_dag_already_teardown(shutdown_only):
    """
    Verify when DAG is already teardown, it doesn't print an error.
    """
    ray.init()

    @ray.remote
    class A:
        def f(self, inp):
            raise RuntimeError

    a = A.remote()
    b = A.remote()

    with InputNode() as inp:
        x = b.f.bind(inp)
        y = a.f.bind(inp)
        dag = MultiOutputNode([x, y])

    adag = dag.experimental_compile()
    x, y = adag.execute(1)
    adag.teardown()

    # Should not print unhandled error.
    del x
    del y


def test_unhandled_error_suppressed_env_var(shutdown_only):
    """
    Verify the error is not raised when env var is set.
    """
    ray.init()

    @ray.remote
    class A:
        def f(self, inp):
            raise RuntimeError

    a = A.remote()
    b = A.remote()

    @ray.remote
    def run():
        with InputNode() as inp:
            x = b.f.bind(inp)
            y = a.f.bind(inp)
            dag = MultiOutputNode([x, y])
        adag = dag.experimental_compile()
        x, y = adag.execute(1)

        with pytest.raises(RuntimeError):
            ray.get(x)

    ray.get(run.options(runtime_env={
        "env_vars": {"RAY_IGNORE_UNHANDLED_ERRORS" : "1"}
    }).remote())



if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
