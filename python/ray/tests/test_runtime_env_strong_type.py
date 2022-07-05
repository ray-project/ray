import os
import sys
import pytest
import ray

from typing import List
from ray.runtime_env import RuntimeEnv
from ray.runtime_env.types.pip import Pip
from dataclasses import dataclass


@dataclass
class ValueType:
    nfeild1: List[str]
    nfeild2: bool


@dataclass
class TestPlugin:
    feild1: List[ValueType]
    feild2: str


def test_convert_from_and_to_dataclass():
    runtime_env = RuntimeEnv()
    test_plugin = TestPlugin(
        feild1=[
            ValueType(nfeild1=["a", "b", "c"], nfeild2=False),
            ValueType(nfeild1=["d", "e"], nfeild2=True),
        ],
        feild2="abc",
    )
    runtime_env.set("test_plugin", test_plugin)
    serialized_runtime_env = runtime_env.serialize()
    assert "test_plugin" in serialized_runtime_env
    runtime_env_2 = RuntimeEnv.deserialize(serialized_runtime_env)
    test_plugin_2 = runtime_env_2.get("test_plugin", data_class=TestPlugin)
    assert len(test_plugin_2.feild1) == 2
    assert test_plugin_2.feild1[0].nfeild1 == ["a", "b", "c"]
    assert test_plugin_2.feild1[0].nfeild2 is False
    assert test_plugin_2.feild1[1].nfeild1 == ["d", "e"]
    assert test_plugin_2.feild1[1].nfeild2 is True
    assert test_plugin_2.feild2 == "abc"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Pip option not supported on Windows.",
)
def test_pip(start_cluster):
    cluster, address = start_cluster
    ray.init(address)

    runtime_env = RuntimeEnv()
    pip = Pip(packages=["pip-install-test==0.5"])
    runtime_env.set("pip", pip)

    @ray.remote
    class Actor:
        def foo(self):
            import pip_install_test  # noqa

            return "hello"

    a = Actor.options(runtime_env=runtime_env).remote()
    assert ray.get(a.foo.remote()) == "hello"


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
