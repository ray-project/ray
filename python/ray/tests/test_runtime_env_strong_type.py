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
    nfield1: List[str]
    nfield2: bool


@dataclass
class TestPlugin:
    field1: List[ValueType]
    field2: str


def test_convert_from_and_to_dataclass():
    runtime_env = RuntimeEnv()
    test_plugin = TestPlugin(
        field1=[
            ValueType(nfield1=["a", "b", "c"], nfield2=False),
            ValueType(nfield1=["d", "e"], nfield2=True),
        ],
        field2="abc",
    )
    runtime_env.set("test_plugin", test_plugin)
    serialized_runtime_env = runtime_env.serialize()
    assert "test_plugin" in serialized_runtime_env
    runtime_env_2 = RuntimeEnv.deserialize(serialized_runtime_env)
    test_plugin_2 = runtime_env_2.get("test_plugin", data_class=TestPlugin)
    assert len(test_plugin_2.field1) == 2
    assert test_plugin_2.field1[0].nfield1 == ["a", "b", "c"]
    assert test_plugin_2.field1[0].nfield2 is False
    assert test_plugin_2.field1[1].nfield1 == ["d", "e"]
    assert test_plugin_2.field1[1].nfield2 is True
    assert test_plugin_2.field2 == "abc"


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

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
