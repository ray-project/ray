import sys

import pytest

import ray
from ray.data._internal.execution.autoscaling_requester import AutoscalingRequester
from ray.data._internal.execution.node_trackers.actor_location import (
    ActorLocationTracker,
)
from ray.data._internal.singleton_actor import (
    SINGLETON_ACTOR_NAMESPACE,
    get_or_create_singleton_actor,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_returns_same_actor_across_calls(ray_start_regular_shared):
    first = get_or_create_singleton_actor(ActorLocationTracker)
    second = get_or_create_singleton_actor(ActorLocationTracker)

    # A second call must reuse the actor rather than reset its state.
    ray.get(first.update_actor_location.remote("op1", "node1"))
    assert ray.get(second.get_actor_locations.remote(["op1"])) == {"op1": "node1"}


def test_actor_is_named_after_its_class(ray_start_regular_shared):
    get_or_create_singleton_actor(ActorLocationTracker)

    # Callers holding only the name (e.g. another process) must find the actor.
    actor = ray.get_actor("ActorLocationTracker", namespace=SINGLETON_ACTOR_NAMESPACE)
    assert ray.get(actor.get_actor_locations.remote([])) == {}


def test_distinct_classes_get_distinct_actors(ray_start_regular_shared):
    tracker = get_or_create_singleton_actor(ActorLocationTracker)
    requester = get_or_create_singleton_actor(AutoscalingRequester)

    assert tracker._actor_id != requester._actor_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
