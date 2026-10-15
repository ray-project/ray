import sys

import pytest

import ray
import ray.experimental.sandbox as sandbox_module
from ray.cluster_utils import Cluster
from ray.experimental.sandbox import placement
from ray.experimental.sandbox.placement import image_scheduling_strategy


def _node(node_id, alive=True, **resources):
    return {"NodeID": node_id, "Alive": alive, "Resources": resources}


@pytest.fixture
def fake_nodes(monkeypatch):
    """Serve ``ray.nodes()`` from the returned list, with Ray "initialized"."""
    nodes = []
    monkeypatch.setattr(placement.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(placement.ray, "nodes", lambda: list(nodes))
    monkeypatch.setattr(placement, "get_current_placement_group", lambda: None)
    monkeypatch.setattr(placement, "_nodes_fetched_at", float("-inf"))
    monkeypatch.setattr(placement, "_NODES_TTL_SECONDS", 0)
    return nodes


def test_an_image_maps_to_one_fitting_node_however_spelled(fake_nodes):
    fake_nodes.extend(
        [
            _node("aa" * 28, CPU=8.0, gvisor=1.0),
            _node("bb" * 28, CPU=2.0, gvisor=1.0),
            _node("cc" * 28, CPU=8.0),
            _node("dd" * 28, alive=False, CPU=8.0, gvisor=1.0),
            {"NodeID": "ee" * 28, "Alive": True, "Resources": None},
        ]
    )
    strategies = [
        image_scheduling_strategy(image, num_cpus=4, resources={"gvisor": 1})
        for image in (
            "python:3.12-slim",
            "library/python:3.12-slim",
            "docker.io/library/python:3.12-slim",
        )
    ]
    # Only the live node with gVisor and four CPUs fits.
    assert {s.node_id for s in strategies} == {"aa" * 28}
    assert all(s.soft and s._spill_on_unavailable for s in strategies)
    assert image_scheduling_strategy("python:3.12-slim", num_cpus=16) is None


def test_images_spread_over_nodes_and_few_move_when_a_node_joins(fake_nodes):
    fake_nodes.extend(_node(f"{i:02x}" * 28, CPU=8.0) for i in range(4))
    images = [f"swebench/task-{i}:latest" for i in range(200)]
    before = {image: image_scheduling_strategy(image).node_id for image in images}
    assert min(list(before.values()).count(n["NodeID"]) for n in fake_nodes) > 25

    fake_nodes.append(_node("ee" * 28, CPU=8.0))
    after = {image: image_scheduling_strategy(image).node_id for image in images}
    moved = [image for image in images if after[image] != before[image]]
    # Rendezvous hashing: only the images the new node wins move, to it.
    assert 0 < len(moved) < 80
    assert {after[image] for image in moved} == {"ee" * 28}


def test_no_preference_without_ray_for_archives_or_in_placement_groups(
    fake_nodes, monkeypatch
):
    fake_nodes.append(_node("aa" * 28, CPU=8.0))
    assert image_scheduling_strategy("busybox:latest") is not None
    # A local archive is on one node's filesystem, so it stays unplaced.
    for archive in ("/data/images/app.tar", "./app.tar", "app.tar"):
        assert image_scheduling_strategy(archive) is None
    monkeypatch.setattr(placement, "get_current_placement_group", lambda: object())
    assert image_scheduling_strategy("busybox:latest") is None
    monkeypatch.setattr(placement.ray, "is_initialized", lambda: False)
    assert image_scheduling_strategy("busybox:latest") is None


def test_a_failed_node_listing_keeps_the_last_one(fake_nodes, monkeypatch):
    fake_nodes.append(_node("aa" * 28, CPU=8.0))
    assert image_scheduling_strategy("busybox:latest").node_id == "aa" * 28

    calls = []

    def unavailable():
        calls.append(1)
        raise ConnectionError("GCS unavailable")

    monkeypatch.setattr(placement.ray, "nodes", unavailable)
    assert image_scheduling_strategy("busybox:latest").node_id == "aa" * 28
    # The failure starts a fresh TTL, so a burst of creates tries once.
    monkeypatch.setattr(placement, "_NODES_TTL_SECONDS", 60)
    for _ in range(5):
        assert image_scheduling_strategy("busybox:latest").node_id == "aa" * 28
    assert len(calls) == 1


def test_create_prefers_the_node_the_image_maps_to(monkeypatch):
    options = {}

    class _Actor:
        def remote(self, **kwargs):
            return "handle"

    def fake_options(**actor_options):
        options.update(actor_options)
        return _Actor()

    monkeypatch.setattr(sandbox_module.Sandbox, "options", fake_options)
    monkeypatch.setattr(
        sandbox_module,
        "image_scheduling_strategy",
        lambda image, **demand: ("strategy", image, demand),
    )
    sandbox_module.create(
        "python:3.12-slim", cpu=2, memory="1Gi", resources={"gvisor": 1}
    )
    assert options["scheduling_strategy"] == (
        "strategy",
        "python:3.12-slim",
        {"num_cpus": 2, "memory": 1 << 30, "resources": {"gvisor": 1}},
    )


@pytest.fixture
def two_sandbox_nodes():
    """A head that hosts no sandboxes and two nodes that host two each."""
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    nodes = [cluster.add_node(num_cpus=0, resources={"sandbox": 2}) for _ in range(2)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()
    yield [node.node_id for node in nodes]
    ray.shutdown()
    cluster.shutdown()


def test_actors_of_one_image_share_its_node_until_it_is_full(two_sandbox_nodes):
    @ray.remote(num_cpus=0, resources={"sandbox": 1})
    class Host:
        def node_id(self):
            return ray.get_runtime_context().get_node_id()

    strategy = image_scheduling_strategy(
        "python:3.12-slim", num_cpus=0, resources={"sandbox": 1}
    )
    assert strategy.node_id in two_sandbox_nodes
    hosts = [Host.options(scheduling_strategy=strategy).remote() for _ in range(3)]
    nodes = ray.get([host.node_id.remote() for host in hosts], timeout=60)
    # Two hosts fit on the image's node. The third goes to the other node
    # rather than waiting for room. Which host is third depends on the order
    # their creations reach the node, so only the counts are checked.
    assert nodes.count(strategy.node_id) == 2
    assert set(nodes) == set(two_sandbox_nodes)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
