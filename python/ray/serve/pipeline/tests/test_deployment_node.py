import pytest

import ray
from ray import serve
from ray.serve.dag import InputNode
from ray.serve.pipeline.deployment_node import (
    DeploymentNode,
)
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY


@serve.deployment
class ServeActor:
    def __init__(self, init_value=0):
        self.i = init_value

    def inc(self):
        self.i += 1

    def get(self):
        return self.i


class SyncActor:
    def __init__(self, init_value=0):
        self.i = init_value

    def inc(self):
        self.i += 1

    def get(self):
        return self.i


class Actor:
    def __init__(self, init_value=0):
        self.i = init_value

    async def inc(self):
        self.i += 1

    async def get(self):
        return self.i


@pytest.mark.asyncio
async def test_simple_deployment_async(serve_instance):
    """Internal testing only for simple creation and execution.

    User should NOT directly create instances of Deployment or DeploymentNode.
    """
    node = DeploymentNode(
        Actor,
        "test",
        (10,),
        {},
        {},
        other_args_to_resolve={USE_SYNC_HANDLE_KEY: False},
    )
    node._deployment.deploy()
    handle = node._deployment_handle

    assert ray.get(await node.get.execute()) == 10
    ray.get(await node.inc.execute())
    assert ray.get(await node.get.execute()) == 11
    assert ray.get(await node.get.execute()) == ray.get(await handle.get.remote())


def test_simple_deployment_sync(serve_instance):
    """Internal testing only for simple creation and execution.

    User should NOT directly create instances of Deployment or DeploymentNode.
    """
    node = DeploymentNode(
        Actor,
        "test",
        (10,),
        {},
        {},
        other_args_to_resolve={USE_SYNC_HANDLE_KEY: True},
    )
    node._deployment.deploy()
    handle = node._deployment_handle

    assert ray.get(node.get.execute()) == 10
    ray.get(node.inc.execute())
    assert ray.get(node.get.execute()) == 11
    assert ray.get(node.get.execute()) == ray.get(handle.get.remote())


def test_no_input_node_as_init_args():
    """
    User should NOT directly create instances of Deployment or DeploymentNode.
    """
    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = DeploymentNode(
            Actor,
            "test",
            (InputNode()),
            {},
            {},
            other_args_to_resolve={USE_SYNC_HANDLE_KEY: True},
        )
    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = DeploymentNode(
            Actor,
            "test",
            (),
            {"a": InputNode()},
            {},
            other_args_to_resolve={USE_SYNC_HANDLE_KEY: True},
        )

    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = DeploymentNode(
            Actor,
            "test",
            (),
            {},
            {},
            other_args_to_resolve={"arg": {"options_a": InputNode()}},
        )


def test_invalid_use_sync_handle():
    with pytest.raises(
        ValueError,
        match=f"{USE_SYNC_HANDLE_KEY} should only be set with a boolean value",
    ):
        _ = DeploymentNode(
            Actor,
            "test",
            [],
            {},
            {},
            other_args_to_resolve={USE_SYNC_HANDLE_KEY: {"options_a": "hii"}},
        )


def test_mix_sync_async_handle(serve_instance):
    # TODO: (jiaodong) Add complex multi-deployment tests from ray DAG.
    pass


def test_deployment_node_as_init_args(serve_instance):
    # TODO: (jiaodong) Add complex multi-deployment tests from ray DAG.
    pass


def test_multi_deployment_nodes(serve_instance):
    # TODO: (jiaodong) Add complex multi-deployment tests from ray DAG.
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
