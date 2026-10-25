import sys
from typing import List

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.handle import DeploymentHandle


@serve.deployment
class DownstreamA:
    def __call__(self, x: int) -> int:
        return x * 2


@serve.deployment
class DownstreamB:
    def process(self, x: int) -> int:
        return x + 10


@serve.deployment
class UpstreamWithStoredHandles:
    def __init__(self, handle_a: DeploymentHandle, handle_b: DeploymentHandle):
        self.handle_a = handle_a
        self.handle_b = handle_b

    async def __call__(self, x: int) -> int:
        result_a = await self.handle_a.remote(x)
        result_b = await self.handle_b.process.remote(x)
        return result_a + result_b


@serve.deployment
class UpstreamWithNestedHandles:
    def __init__(self, handles_dict: dict, handles_list: list):
        self.handles = handles_dict  # {"a": handle_a, "b": handle_b}
        self.handle_list = handles_list  # [handle_a, handle_b]

    async def __call__(self, x: int) -> int:
        result_a = await self.handles["a"].remote(x)
        result_b = await self.handles["b"].process.remote(x)
        return result_a + result_b


@serve.deployment
class DynamicDeployment:
    async def __call__(self, x: int, app_name1: str, app_name2: str) -> int:
        handle_a = serve.get_deployment_handle("DownstreamA", app_name=app_name1)
        handle_b = serve.get_deployment_handle("DownstreamB", app_name=app_name2)
        result_a = await handle_a.remote(x)
        result_b = await handle_b.process.remote(x)
        return result_a + result_b


def get_replica_actor_handle(deployment_name: str, app_name: str):
    actors = ray.util.list_named_actors(all_namespaces=True)
    replica_actor_name = None
    for actor in actors:
        # Match pattern: SERVE_REPLICA::{app_name}#{deployment_name}#
        if actor["name"].startswith(f"SERVE_REPLICA::{app_name}#{deployment_name}#"):
            replica_actor_name = actor["name"]
            break

    if replica_actor_name is None:
        # Debug: print all actor names to help diagnose
        all_actors = [a["name"] for a in actors if "SERVE" in a["name"]]
        raise RuntimeError(
            f"Could not find replica actor for {deployment_name} in app {app_name}. "
            f"Available serve actors: {all_actors}"
        )

    return ray.get_actor(replica_actor_name, namespace=SERVE_NAMESPACE)


@pytest.mark.asyncio
class TestListOutboundDeployments:
    """Test suite for list_outbound_deployments() method."""

    async def test_stored_handles_in_init(self, serve_instance):
        """Test listing handles that are passed to __init__ and stored as attributes."""
        app_name = "test_stored_handles"

        # Build and deploy the app
        handle_a = DownstreamA.bind()
        handle_b = DownstreamB.bind()
        app = UpstreamWithStoredHandles.bind(handle_a, handle_b)

        serve.run(app, name=app_name)

        # Get the replica actor for the upstream deployment
        replica_actor = get_replica_actor_handle("UpstreamWithStoredHandles", app_name)

        # Call list_outbound_deployments
        outbound_deployments: List[DeploymentID] = ray.get(
            replica_actor.list_outbound_deployments.remote()
        )

        # Verify results
        deployment_names = {dep_id.name for dep_id in outbound_deployments}
        assert "DownstreamA" in deployment_names
        assert "DownstreamB" in deployment_names
        assert len(outbound_deployments) == 2

        # Verify app names match
        for dep_id in outbound_deployments:
            assert dep_id.app_name == app_name

    async def test_nested_handles_in_dict_and_list(self, serve_instance):
        """Test listing handles stored in nested data structures (dict, list)."""
        app_name = "test_nested_handles"

        # Build and deploy the app
        handle_a = DownstreamA.bind()
        handle_b = DownstreamB.bind()
        handles_dict = {"a": handle_a, "b": handle_b}
        handles_list = [handle_a, handle_b]
        app = UpstreamWithNestedHandles.bind(handles_dict, handles_list)

        serve.run(app, name=app_name)

        # Get the replica actor
        replica_actor = get_replica_actor_handle("UpstreamWithNestedHandles", app_name)

        # Call list_outbound_deployments
        outbound_deployments: List[DeploymentID] = ray.get(
            replica_actor.list_outbound_deployments.remote()
        )

        # Verify results (should find handles despite being in nested structures)
        deployment_names = {dep_id.name for dep_id in outbound_deployments}
        assert "DownstreamA" in deployment_names
        assert "DownstreamB" in deployment_names

        # Verify no duplicates (handle_a and handle_b appear in both dict and list)
        assert len(outbound_deployments) == 2

    async def test_no_handles(self, serve_instance):
        """Test deployment with no outbound handles."""
        app_name = "test_no_handles"

        # Deploy a simple deployment with no handles
        app = DownstreamA.bind()
        serve.run(app, name=app_name)

        # Get the replica actor
        replica_actor = get_replica_actor_handle("DownstreamA", app_name)

        # Call list_outbound_deployments
        outbound_deployments: List[DeploymentID] = ray.get(
            replica_actor.list_outbound_deployments.remote()
        )

        # Should be empty
        assert len(outbound_deployments) == 0

    async def test_dynamic_handles(self, serve_instance):
        app1 = DownstreamA.bind()
        app2 = DownstreamB.bind()
        app3 = DynamicDeployment.bind()

        serve.run(app1, name="app1", route_prefix="/app1")
        serve.run(app2, name="app2", route_prefix="/app2")
        handle = serve.run(app3, name="app3", route_prefix="/app3")

        # Make requests to trigger dynamic handle creation
        # x=1: DownstreamA returns 1*2=2, DownstreamB returns 1+10=11, total=2+11=13
        results = [await handle.remote(1, "app1", "app2") for _ in range(10)]
        for result in results:
            assert result == 13

        # Get the replica actor
        replica_actor = get_replica_actor_handle("DynamicDeployment", "app3")

        # Call list_outbound_deployments
        outbound_deployments: List[DeploymentID] = ray.get(
            replica_actor.list_outbound_deployments.remote()
        )

        # Verify results - should include dynamically created handles
        deployment_names = {dep_id.name for dep_id in outbound_deployments}
        assert "DownstreamA" in deployment_names
        assert "DownstreamB" in deployment_names
        assert len(outbound_deployments) == 2

        # Verify the app names are correct
        app_names = {dep_id.app_name for dep_id in outbound_deployments}
        assert "app1" in app_names
        assert "app2" in app_names


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
