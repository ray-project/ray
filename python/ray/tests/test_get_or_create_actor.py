import os
import sys

import pytest

import ray
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_simple(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Actor:
        def ping(self):
            return "ok"

        def pid(self):
            return os.getpid()

    for ns in [None, "test"]:
        a = Actor.options(name="x", namespace=ns, get_if_exists=True).remote()
        b = Actor.options(name="x", namespace=ns, get_if_exists=True).remote()
        assert ray.get(a.ping.remote()) == "ok"
        assert ray.get(b.ping.remote()) == "ok"
        assert ray.get(b.pid.remote()) == ray.get(a.pid.remote())

    with pytest.raises(TypeError):
        Actor.options(name=object(), get_if_exists=True).remote()

    with pytest.raises(TypeError):
        Actor.options(name="x", namespace=object(), get_if_exists=True).remote()

    with pytest.raises(ValueError):
        Actor.options(num_cpus=1, get_if_exists=True).remote()


def test_shared_actor(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote(name="x", namespace="test", get_if_exists=True)
    class SharedActor:
        def ping(self):
            return "ok"

        def pid(self):
            return os.getpid()

    a = SharedActor.remote()
    b = SharedActor.remote()
    assert ray.get(a.ping.remote()) == "ok"
    assert ray.get(b.ping.remote()) == "ok"
    assert ray.get(b.pid.remote()) == ray.get(a.pid.remote())


def test_no_verbose_output():
    script = """
import ray

@ray.remote
class Actor:
    def ping(self):
        return "ok"


@ray.remote
def getter(name):
    actor = Actor.options(
        name="foo", lifetime="detached", namespace="n", get_if_exists=True).remote()
    ray.get(actor.ping.remote())


def do_run(name):
    name = "actor_" + str(name)
    tasks = [getter.remote(name) for i in range(4)]
    ray.get(tasks)
    try:
        ray.kill(ray.get_actor(name, namespace="n"))  # Cleanup
    except:
        pass


for i in range(100):
    do_run(i)

print("DONE")
"""

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    # Check there's no excessively verbose raylet error messages due to
    # actor creation races.
    out = []
    for line in out_str.split("\n"):
        if "local Ray instance" not in line and "The object store" not in line:
            out.append(line)
    valid = "".join(out)
    assert "DONE" in valid, out_str


def test_get_or_create_named_actor(shutdown_only):
    """
    This test aggressively gets or creates a named actor and makes the actor
    go out of scope immediately. Additionally, `max_restarts=-1` is set to make
    the actor restartable and make the test more aggressive.
    """

    @ray.remote
    class Actor:
        pass

    for _ in range(1000):
        Actor.options(
            name="test-get-or-create-named-actor",
            get_if_exists=True,
            max_restarts=-1,
        ).remote()


def test_get_or_create_actor_with_placement_group_validation(shutdown_only):
    """
    Test that get_or_create with placement group properly validates
    resource requirements and provides clear error messages.
    """
    ray.init(num_cpus=1)

    # Create a placement group with limited resources
    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())

    @ray.remote(num_cpus=1, num_gpus=8)
    class Actor:
        def __init__(self):
            pass

    # This should raise a ValueError with a clear message about resource mismatch
    with pytest.raises(
        ValueError,
        match=r"Cannot schedule test_get_or_create_actor_with_placement_group_validation\.<locals>\.Actor with the placement group because the resource request \{'CPU': 1, 'GPU': 8\} cannot fit into any bundles for the placement group, \[\{'CPU': 1\.0\}\]\.",
    ):
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
            name="actor",
            get_if_exists=True,
        ).remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
