import pytest

import ray
from ray import serve
from ray.serve.api import Deployment
from ray.serve.config import DeploymentConfig
from ray.serve.pipeline.deployment_node import DeploymentNode


@serve.deployment
class ServeActor:
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


def test_disallow_binding_deployments(serve_instance):
    with pytest.raises(
        AttributeError,
        match="DAG building API should only be used for @ray.remote decorated",
    ):
        a1 = ServeActor._bind(10)


@pytest.mark.asyncio
async def test_simple_deployment_async(serve_instance):
    """Internal testing only for simple creation and execution.

    User should not directly create instances of Deployment or DeploymentNode.
    """
    deployment = Deployment(
        Actor,
        "test",
        DeploymentConfig(),
        init_args=(10,),
        route_prefix="/test",
        _internal=True,
    )
    node = DeploymentNode(
        deployment,
        [],
        {},
        {},
        other_args_to_resolve={"sync_handle": False},
    )
    deployment.deploy()
    handle = deployment.get_handle(sync=False)

    assert ray.get(await node.get.execute()) == 10
    ray.get(await node.inc.execute())
    assert ray.get(await node.get.execute()) == 11
    assert ray.get(await node.get.execute()) == ray.get(
        await handle.get.remote()
    )


def test_simple_deployment_sync(serve_instance):
    deployment = Deployment(
        Actor,
        "test",
        DeploymentConfig(),
        init_args=(10,),
        route_prefix="/test",
        _internal=True,
    )
    node = DeploymentNode(
        deployment,
        [],
        {},
        {},
        other_args_to_resolve={"sync_handle": True},
    )
    deployment.deploy()
    handle = deployment.get_handle(sync=True)

    assert ray.get(node.get.execute()) == 10
    ray.get(node.inc.execute())
    assert ray.get(node.get.execute()) == 11
    assert ray.get(node.get.execute()) == ray.get(handle.get.remote())


def test_no_input_node_as_init_args(serve_instance):
    pass


def test_deployment_node_as_init_args(serve_instance):
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
