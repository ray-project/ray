import asyncio
import sys
from typing import Dict, List, Set, Tuple

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
def shutdown_test_cluster(request, monkeypatch):
    """A Ray cluster whose Serve instance the test itself shuts down."""
    for name, value in getattr(request, "param", {}).items():
        monkeypatch.setenv(name, value)

    ray.init(num_cpus=16, namespace="test_dependency_ordered_shutdown")
    yield
    serve.shutdown()
    ray.shutdown()


# A healthy replica cannot drain faster than graceful_shutdown_wait_loop_s, whose
# 2s default exceeds the tier timeout below, so every tier would time out.
_SHORT_DRAIN = {"graceful_shutdown_wait_loop_s": 0.1}


class _RecordsShutdown:
    """Record a deployment name when its replica is torn down."""

    def _record_shutdown_as(self, name: str, recorder: ray.actor.ActorHandle):
        self._shutdown_name = name
        self._shutdown_recorder = recorder

    async def __del__(self):
        await self._shutdown_recorder.add.remote(self._shutdown_name)


@serve.deployment(**_SHORT_DRAIN)
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


@serve.deployment(**_SHORT_DRAIN)
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


@serve.deployment(**_SHORT_DRAIN)
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


@serve.deployment(**_SHORT_DRAIN)
class PlainNode:
    """A placeholder for a deployment a LinkedNode needs to already exist."""

    def __init__(self, *downstream: DeploymentHandle):
        self._downstream = downstream

    def __call__(self) -> str:
        return "plain"


@serve.deployment(graceful_shutdown_timeout_s=1000, **_SHORT_DRAIN)
class WedgedNode:
    """A deployment whose replica never finishes shutting down."""

    def __init__(self, *downstream: DeploymentHandle):
        self._downstream = downstream

    def __call__(self) -> str:
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


def _wait_for_topology(expected: Dict[Tuple[str, str], Set[Tuple[str, str]]]):
    """Wait until the controller sees exactly these caller to callee edges."""
    seen = {}

    def _matches() -> bool:
        seen.update({node: _outbound(*node) for node in expected})
        return seen == expected

    try:
        wait_for_condition(_matches, timeout=20)
    except RuntimeError:
        raise AssertionError(f"Expected topology {expected}, controller sees {seen}.")


def _shutdown_order(recorder: ray.actor.ActorHandle, expected: Set[str]) -> List[str]:
    """Wait for every expected replica to record its teardown, then return the order.

    `serve.shutdown()` gives up after 30s and returns with teardown still in
    flight, so reading the recorder as soon as it returns can truncate.
    """

    def recorded() -> List[str]:
        return ray.get(recorder.get.remote())

    try:
        wait_for_condition(lambda: set(recorded()) == expected, timeout=60)
    except RuntimeError:
        raise AssertionError(
            f"Expected {sorted(expected)} to record a teardown, got {recorded()}."
        )

    return recorded()


def _replica_actor(deployment_name: str, app_name: str) -> ray.actor.ActorHandle:
    """Handle to a live replica actor, raising if the replica is gone."""
    prefix = f"SERVE_REPLICA::{app_name}#{deployment_name}#"
    for actor in ray.util.list_named_actors(all_namespaces=True):
        if actor["name"].startswith(prefix):
            return ray.get_actor(actor["name"], namespace=SERVE_NAMESPACE)

    raise RuntimeError(f"No live replica for {deployment_name} in app {app_name}.")


class TestKnownTopologyShutdown:
    """Teardown order for topologies the controller fully knows."""

    def test_linear_chain(self, shutdown_test_cluster):
        recorder = Accumulator.remote()

        handle = serve.run(
            _node(
                "Ingress", recorder, _node("Middle", recorder, _node("Leaf", recorder))
            ),
            name="chain",
        )
        assert handle.remote().result() == "Ingress/Middle/Leaf"

        _wait_for_topology(
            {
                ("chain", "Ingress"): {("chain", "Middle")},
                ("chain", "Middle"): {("chain", "Leaf")},
                ("chain", "Leaf"): set(),
            }
        )

        serve.shutdown()

        expected = ["Ingress", "Middle", "Leaf"]
        assert _shutdown_order(recorder, set(expected)) == expected

    def test_cross_app_chain(self, shutdown_test_cluster):
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

        _wait_for_topology(
            {
                ("frontend", "Caller"): {("backend", "Middle")},
                ("backend", "Middle"): {("backend", "Leaf")},
            }
        )

        serve.shutdown()

        expected = ["Caller", "Middle", "Leaf"]
        assert _shutdown_order(recorder, set(expected)) == expected


class TestBestEffortTopologyShutdown:
    """Shutdown when the topology is incomplete, cyclic, or cannot drain."""

    def test_incomplete_topology(self, shutdown_test_cluster):
        """Handles created at request time are missing from the topology."""
        recorder = Accumulator.remote()

        serve.run(_node("Leaf", recorder), name="leaf_app", route_prefix="/leaf")
        handle = serve.run(
            _node("Ingress", recorder, _lazy("Middle", recorder, "Leaf", "leaf_app")),
            name="main",
            route_prefix="/main",
        )

        assert handle.remote().result() == "Ingress/Middle/Leaf"

        _wait_for_topology(
            {
                ("main", "Ingress"): {("main", "Middle")},
                ("main", "Middle"): set(),
            }
        )

        serve.shutdown()

        order = _shutdown_order(recorder, {"Ingress", "Middle", "Leaf"})

        # The known edge is still respected.
        assert order.index("Ingress") < order.index("Middle")

    def test_ingress_into_cycle(self, shutdown_test_cluster):
        """An ingress feeding a cycle drains before the cyclic remainder."""
        recorder = Accumulator.remote()

        serve.run(_plain("Ingress", _plain("A")), name="app_a", route_prefix="/a")
        serve.run(_linked("B", recorder, "A", "app_a"), name="app_b", route_prefix="/b")
        serve.run(
            _node("Ingress", recorder, _linked("A", recorder, "B", "app_b")),
            name="app_a",
            route_prefix="/a",
        )

        _wait_for_topology(
            {
                ("app_a", "Ingress"): {("app_a", "A")},
                ("app_a", "A"): {("app_b", "B")},
                ("app_b", "B"): {("app_a", "A")},
            }
        )

        serve.shutdown()

        order = _shutdown_order(recorder, {"Ingress", "A", "B"})
        assert order[0] == "Ingress"
        assert sorted(order[1:]) == ["A", "B"]

    @pytest.mark.parametrize(
        "shutdown_test_cluster",
        [{"RAY_SERVE_SHUTDOWN_TIER_TIMEOUT_S": "5"}],
        indirect=True,
    )
    def test_tier_that_never_drains(self, shutdown_test_cluster):
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

        _wait_for_topology(
            {
                ("chain", "Ingress"): {("chain", "Middle")},
                ("chain", "Middle"): {("chain", "Leaf")},
            }
        )

        # Start the shutdown without blocking the driver on the stuck replica.
        client = _get_global_client()
        ray.get(client._controller.graceful_shutdown.remote(False))

        # Ingress drains well inside the tier timeout, so Leaf is only reached by
        # timing out on Middle, the one tier that never drains.
        assert _shutdown_order(recorder, {"Ingress", "Leaf"}) == ["Ingress", "Leaf"]
        middle_replica = _replica_actor("Middle", "chain")

        # Cleanup Middle so it doesn't sit in __del__ for its full 1000s grace period
        ray.kill(middle_replica)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
