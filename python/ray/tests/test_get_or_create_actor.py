import sys
import os
import pytest

import ray
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
)


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
        if "Ray dashboard" not in line and "The object store" not in line:
            out.append(line)
    valid = "".join(out)
    assert valid.strip() == "DONE", out_str


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
