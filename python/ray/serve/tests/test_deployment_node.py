import pytest

import ray
from ray import serve
from ray.experimental.dag.input_node import InputNode
from ray.serve.api import Deployment, DeploymentNode, DeploymentMethodNode
from ray.serve.config import DeploymentConfig
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY


@serve.deployment
class SyncActor:
    def __init__(self, init_value=0):
        self.i = init_value

    def inc(self):
        self.i += 1

    def get(self):
        return self.i


@serve.deployment
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
    dag = Actor.options(name="test").bind(
        10, other_args_to_resolve={USE_SYNC_HANDLE_KEY: False}
    )
    dag.get_deployment().deploy()
    handle = dag.get_deployment_handle()

    assert ray.get(await dag.get.bind().execute()) == 10
    ray.get(await dag.inc.bind().execute())
    assert ray.get(await dag.get.bind().execute()) == 11
    assert ray.get(await dag.get.bind().execute()) == ray.get(
        await handle.get.remote()
    )


def test_simple_deployment_sync(serve_instance):
    """Internal testing only for simple creation and execution.

    User should NOT directly create instances of Deployment or DeploymentNode.
    """
    dag = SyncActor.options(name="test").bind(
        10, other_args_to_resolve={USE_SYNC_HANDLE_KEY: True}
    )
    dag.get_deployment().deploy()
    handle = dag.get_deployment_handle()

    assert ray.get(dag.get.bind().execute()) == 10
    ray.get(dag.inc.bind().execute())
    assert ray.get(dag.get.bind().execute()) == 11
    assert ray.get(dag.get.bind().execute()) == ray.get(handle.get.remote())


def test_no_input_node_as_init_args():
    """
    User should NOT directly create instances of Deployment or DeploymentNode.
    """
    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = Actor.options(name="test").bind(InputNode())

    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = Actor.options(name="test").bind(a=InputNode())

    with pytest.raises(
        ValueError,
        match="cannot be used as args, kwargs, or other_args_to_resolve",
    ):
        _ = Actor.options(name="test").bind(
            other_args_to_resolve={"arg": {"options_a": InputNode()}}
        )


def test_invalid_use_sync_handle():
    with pytest.raises(
        ValueError,
        match=f"{USE_SYNC_HANDLE_KEY} should only be set with a boolean value",
    ):
        _ = Actor.options(name="test").bind(other_args_to_resolve={USE_SYNC_HANDLE_KEY: {"options_a": "hii"}})

    with pytest.raises(
        ValueError,
        match=f"{USE_SYNC_HANDLE_KEY} should only be set with a boolean value",
    ):
        _ = Actor.options(name="test").bind(other_args_to_resolve={USE_SYNC_HANDLE_KEY: None})


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
