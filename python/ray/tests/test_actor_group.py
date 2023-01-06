import time
import warnings

import pytest

import ray
from ray.util.actor_group import ActorGroup


class DummyActor:
    def return_arg(self, arg):
        return arg

    def get_actor_metadata(self):
        return "metadata"


def test_actor_creation(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    with warnings.catch_warnings(record=True) as w:
        ag = ActorGroup(actor_cls=DummyActor, num_actors=2)
        assert any(
            "use ray.util.multiprocessing" in str(warning.message) for warning in w
        )
    assert len(ag) == 2
    time.sleep(1)
    # Make sure both CPUs are being used by the actors.
    assert "CPU" not in ray.available_resources()
    ag.shutdown()


def test_actor_creation_num_cpus(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    ag = ActorGroup(actor_cls=DummyActor, num_cpus_per_actor=2)
    time.sleep(1)
    assert len(ag) == 1
    # Make sure both CPUs are being used by the actor.
    assert "CPU" not in ray.available_resources()
    ag.shutdown()


def test_actor_shutdown(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    ag = ActorGroup(actor_cls=DummyActor, num_actors=2)
    time.sleep(1)
    assert "CPU" not in ray.available_resources()
    assert len(ray._private.state.actors()) == 2
    ag.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2

    with pytest.raises(RuntimeError):
        ag.return_arg.remote(1)


def test_actor_restart(ray_start_2_cpus):
    ag = ActorGroup(actor_cls=DummyActor, num_actors=2)
    with pytest.raises(RuntimeError):
        ag.start()
    # Avoid race condition.
    time.sleep(1)
    ag.shutdown(0)
    ag.start()
    ray.get(ag.return_arg.remote(1))


def test_actor_method(ray_start_2_cpus):
    ag = ActorGroup(actor_cls=DummyActor, num_actors=2)
    assert ray.get(ag.return_arg.remote(1)) == [1, 1]


def test_actor_metadata(ray_start_2_cpus):
    ag = ActorGroup(actor_cls=DummyActor, num_actors=2)
    assert ag.actor_metadata == ["metadata", "metadata"]


def test_actor_method_fail(ray_start_2_cpus):
    ag = ActorGroup(actor_cls=DummyActor, num_actors=2)

    with pytest.raises(TypeError):
        ag.return_arg(1)

    with pytest.raises(AttributeError):
        ag.non_existent_method.remote()


def test_bad_resources(ray_start_2_cpus):
    with pytest.raises(ValueError):
        ActorGroup(actor_cls=DummyActor, num_actors=-1)

    with pytest.raises(ValueError):
        ActorGroup(actor_cls=DummyActor, num_actors=-1)

    with pytest.raises(ValueError):
        ActorGroup(actor_cls=DummyActor, num_actors=-1)


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
