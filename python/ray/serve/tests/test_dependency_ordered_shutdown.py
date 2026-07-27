import asyncio
import sys
from typing import List, Set, Tuple

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.test_utils import Accumulator
from ray.serve.api import get_deployment_handle
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle


@pytest.fixture
def serve_shutdown_instance(request, monkeypatch):
    """A Serve instance the test itself is allowed to shut down."""
    for name, value in getattr(request, "param", {}).items():
        monkeypatch.setenv(name, value)

    ray.init(num_cpus=16, namespace="test_dependency_ordered_shutdown")
    yield
    serve.shutdown()
    ray.shutdown()


class _RecordsShutdown:
    """Record a deployment name when its replica is torn down."""

    def _record_shutdown_as(self, name: str, recorder: ray.actor.ActorHandle):
        self._shutdown_name = name
        self._shutdown_recorder = recorder

    async def __del__(self):
        await self._shutdown_recorder.add.remote(self._shutdown_name)


@serve.deployment
class Node(_RecordsShutdown):
    """A deployment that calls its downstream handles and records its teardown."""

    def __init__(
        self, name: str, recorder: ray.actor.ActorHandle, *downstream: DeploymentHandle
    ):
        self._record_shutdown_as(name, recorder)
        self._downstream = downstream

    async def __call__(self) -> str:
        results = [await handle.remote() for handle in self._downstream]
        return "/".join([self._shutdown_name, *results])


@serve.deployment
class LinkedNode(_RecordsShutdown):
    """Builds its downstream handle in the constructor.

    Used for edges a bind graph cannot express, such as an edge into another
    app or an edge that closes a cycle.
    """

    def __init__(
        self,
        name: str,
        recorder: ray.actor.ActorHandle,
        target_name: str,
        target_app: str,
    ):
        self._record_shutdown_as(name, recorder)
        self._downstream = get_deployment_handle(target_name, target_app)

    async def __call__(self) -> str:
        return f"{self._shutdown_name}/{await self._downstream.remote()}"


@serve.deployment
class LazyNode(_RecordsShutdown):
    """Builds its downstream handle per request, after it reported as ready."""

    def __init__(
        self,
        name: str,
        recorder: ray.actor.ActorHandle,
        target_name: str,
        target_app: str,
    ):
        self._record_shutdown_as(name, recorder)
        self._target = (target_name, target_app)

    async def __call__(self) -> str:
        handle = get_deployment_handle(*self._target)
        return f"{self._shutdown_name}/{await handle.remote()}"


@serve.deployment
class PlainNode:
    """Same shape as Node but does not record its teardown."""

    def __init__(self, *downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self) -> str:
        for handle in self._downstream:
            await handle.remote()
        return "plain"


@serve.deployment(graceful_shutdown_timeout_s=1000)
class WedgedNode:
    """A deployment whose replica never finishes shutting down."""

    def __init__(self, *downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self) -> str:
        for handle in self._downstream:
            await handle.remote()
        return "wedged"

    async def __del__(self):
        await asyncio.sleep(1000)


def _node(name: str, recorder: ray.actor.ActorHandle, *downstream):
    return Node.options(name=name).bind(name, recorder, *downstream)


def _linked(
    name: str, recorder: ray.actor.ActorHandle, target_name: str, target_app: str
):
    return LinkedNode.options(name=name).bind(name, recorder, target_name, target_app)


def _lazy(
    name: str, recorder: ray.actor.ActorHandle, target_name: str, target_app: str
):
    return LazyNode.options(name=name).bind(name, recorder, target_name, target_app)


def _plain(name: str, *downstream):
    return PlainNode.options(name=name).bind(*downstream)


def _outbound(app_name: str, deployment_name: str) -> Set[Tuple[str, str]]:
    """The controller's view of what a deployment calls, as (app, name) pairs."""
    details = _get_global_client().get_serve_details()
    topology = details["applications"][app_name]["deployment_topology"]
    return {
        (dep["app_name"], dep["name"])
        for dep in topology["nodes"][deployment_name]["outbound_deployments"]
    }


def _shutdown_order(recorder: ray.actor.ActorHandle) -> List[str]:
    return ray.get(recorder.get.remote())


def _replica_actor(deployment_name: str, app_name: str) -> ray.actor.ActorHandle:
    """Handle to a live replica actor, raising if the replica is gone."""
    prefix = f"SERVE_REPLICA::{app_name}#{deployment_name}#"
    for actor in ray.util.list_named_actors(all_namespaces=True):
        if actor["name"].startswith(prefix):
            return ray.get_actor(actor["name"], namespace=SERVE_NAMESPACE)

    raise RuntimeError(f"No live replica for {deployment_name} in app {app_name}.")


class TestDependencyOrderedShutdown:
    """Teardown order for topologies the controller fully knows."""

    def test_linear_chain(self, serve_shutdown_instance):
        recorder = Accumulator.remote()

        handle = serve.run(
            _node(
                "Ingress", recorder, _node("Middle", recorder, _node("Leaf", recorder))
            ),
            name="chain",
        )
        assert handle.remote().result() == "Ingress/Middle/Leaf"

        wait_for_condition(
            lambda: _outbound("chain", "Ingress") == {("chain", "Middle")}
            and _outbound("chain", "Middle") == {("chain", "Leaf")}
            and _outbound("chain", "Leaf") == set()
        )

        serve.shutdown()

        assert _shutdown_order(recorder) == ["Ingress", "Middle", "Leaf"]

    def test_diamond_shared_leaf_last(self, serve_shutdown_instance):
        recorder = Accumulator.remote()

        leaf = _node("Leaf", recorder)
        handle = serve.run(
            _node(
                "Ingress",
                recorder,
                _node("M1", recorder, leaf),
                _node("M2", recorder, leaf),
            ),
            name="diamond",
        )
        assert handle.remote().result() == "Ingress/M1/Leaf/M2/Leaf"

        wait_for_condition(
            lambda: _outbound("diamond", "Ingress")
            == {("diamond", "M1"), ("diamond", "M2")}
            and _outbound("diamond", "M1") == {("diamond", "Leaf")}
            and _outbound("diamond", "M2") == {("diamond", "Leaf")}
        )

        serve.shutdown()

        order = _shutdown_order(recorder)
        assert order[0] == "Ingress"
        assert order[-1] == "Leaf"
        assert set(order[1:3]) == {"M1", "M2"}

    def test_independent_apps(self, serve_shutdown_instance):
        recorder = Accumulator.remote()

        serve.run(
            _node("Ingress1", recorder, _node("Backend1", recorder)),
            name="app1",
            route_prefix="/app1",
        )
        serve.run(
            _node("Ingress2", recorder, _node("Backend2", recorder)),
            name="app2",
            route_prefix="/app2",
        )

        wait_for_condition(
            lambda: _outbound("app1", "Ingress1") == {("app1", "Backend1")}
            and _outbound("app2", "Ingress2") == {("app2", "Backend2")}
        )

        serve.shutdown()

        order = _shutdown_order(recorder)
        assert set(order) == {"Ingress1", "Backend1", "Ingress2", "Backend2"}
        assert max(order.index("Ingress1"), order.index("Ingress2")) < min(
            order.index("Backend1"), order.index("Backend2")
        )

    def test_cross_app_chain(self, serve_shutdown_instance):
        """A caller in one app is torn down before its callee in another."""
        recorder = Accumulator.remote()

        serve.run(
            _node("Middle", recorder, _node("Leaf", recorder)),
            name="backend",
            route_prefix="/backend",
        )
        handle = serve.run(
            _linked("Caller", recorder, "Middle", "backend"),
            name="frontend",
            route_prefix="/frontend",
        )
        assert handle.remote().result() == "Caller/Middle/Leaf"

        wait_for_condition(
            lambda: _outbound("frontend", "Caller") == {("backend", "Middle")}
            and _outbound("backend", "Middle") == {("backend", "Leaf")}
        )

        serve.shutdown()

        assert _shutdown_order(recorder) == ["Caller", "Middle", "Leaf"]


class TestBestEffortTopologyShutdown:
    """Shutdown when the topology is incomplete, cyclic, or cannot drain."""

    def test_incomplete_topology(self, serve_shutdown_instance):
        """Handles created at request time are missing from the topology."""
        recorder = Accumulator.remote()

        serve.run(_node("Leaf", recorder), name="leaf_app", route_prefix="/leaf")
        handle = serve.run(
            _node("Ingress", recorder, _lazy("Middle", recorder, "Leaf", "leaf_app")),
            name="main",
            route_prefix="/main",
        )

        assert handle.remote().result() == "Ingress/Middle/Leaf"

        wait_for_condition(lambda: _outbound("main", "Ingress") == {("main", "Middle")})
        assert _outbound("main", "Middle") == set()

        serve.shutdown()

        order = _shutdown_order(recorder)
        assert set(order) == {"Ingress", "Middle", "Leaf"}

        # The known edge is still respected.
        assert order.index("Ingress") < order.index("Middle")

    def test_cycle(self, serve_shutdown_instance):
        """A cycle has no caller first order, so it is torn down as a group."""
        recorder = Accumulator.remote()

        # Redeploy A after B is up to build the cycle.
        serve.run(_plain("A"), name="app_a", route_prefix="/a")
        serve.run(_linked("B", recorder, "A", "app_a"), name="app_b", route_prefix="/b")
        serve.run(_linked("A", recorder, "B", "app_b"), name="app_a", route_prefix="/a")

        wait_for_condition(
            lambda: _outbound("app_a", "A") == {("app_b", "B")}
            and _outbound("app_b", "B") == {("app_a", "A")},
            timeout=20,
        )

        serve.shutdown()

        assert sorted(_shutdown_order(recorder)) == ["A", "B"]

    def test_ingress_into_cycle(self, serve_shutdown_instance):
        """An ingress feeding a cycle drains before the cyclic remainder."""
        recorder = Accumulator.remote()

        serve.run(_plain("Ingress", _plain("A")), name="app_a", route_prefix="/a")
        serve.run(_linked("B", recorder, "A", "app_a"), name="app_b", route_prefix="/b")
        serve.run(
            _node("Ingress", recorder, _linked("A", recorder, "B", "app_b")),
            name="app_a",
            route_prefix="/a",
        )

        wait_for_condition(
            lambda: _outbound("app_a", "Ingress") == {("app_a", "A")}
            and _outbound("app_a", "A") == {("app_b", "B")}
            and _outbound("app_b", "B") == {("app_a", "A")},
            timeout=20,
        )

        serve.shutdown()

        order = _shutdown_order(recorder)
        assert order[0] == "Ingress"
        assert sorted(order[1:]) == ["A", "B"]

    @pytest.mark.parametrize(
        "serve_shutdown_instance",
        [{"RAY_SERVE_SHUTDOWN_TIER_TIMEOUT_S": "2"}],
        indirect=True,
    )
    def test_tier_that_never_drains(self, serve_shutdown_instance):
        """A replica that refuses to stop does not block the tiers behind it."""
        recorder = Accumulator.remote()

        handle = serve.run(
            _node(
                "Ingress",
                recorder,
                WedgedNode.options(name="Middle").bind(_node("Leaf", recorder)),
            ),
            name="chain",
        )
        assert handle.remote().result() == "Ingress/wedged"

        # Start the shutdown without blocking the driver on the stuck replica.
        client = _get_global_client()
        ray.get(client._controller.graceful_shutdown.remote(False))

        wait_for_condition(lambda: "Leaf" in _shutdown_order(recorder), timeout=60)
        assert _shutdown_order(recorder) == ["Ingress", "Leaf"]

        # Leaf was torn down while Middle was still stopping, which is only
        # possible if shutdown advanced past the tier that never drained.
        middle_replica = _replica_actor("Middle", "chain")
        ray.kill(middle_replica)

    def test_no_applications(self, serve_shutdown_instance):
        """Shutting down an instance with nothing deployed completes."""
        serve.start()

        serve.shutdown()

        assert not [
            actor
            for actor in ray.util.list_named_actors(all_namespaces=True)
            if actor["name"].startswith("SERVE")
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
