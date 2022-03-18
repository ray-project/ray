import sys
import pytest

from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
)


def test_get_or_create_actor():
    script = """
import ray

@ray.remote
class Actor:
    def ping(self):
        return "ok"


@ray.remote
def getter(name):
    actor = Actor.options(
        name="foo", lifetime="detached", namespace="test").get_or_create()
    ray.get(actor.ping.remote())


def do_run(name):
    name = "actor_" + str(name)
    tasks = [getter.remote(name) for i in range(4)]
    ray.get(tasks)
    try:
        ray.kill(ray.get_actor(name, namespace="test"))  # Cleanup
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
    valid = "".join(x for x in out_str.split("\n") if "Ray dashboard" not in x)
    assert valid.strip() == "DONE", out_str


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
