import subprocess
import sys
import pytest

import ray

from ray._private.test_utils import run_string_as_driver_nonblocking


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_worker_stdout():
    script = """
import ray
import sys

ray.init(num_cpus=2)

@ray.remote
def foo(out_str, err_str):
    print(out_str)
    print(err_str, file=sys.stderr)

ray.get(foo.remote("abc", "def"))
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    assert out_str.endswith("abc\n"), out_str
    assert "(foo pid=" in out_str, out_str
    assert err_str.split("\n")[-2].endswith("def")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_actor_stdout():
    script = """
import ray

ray.init(num_cpus=2)

@ray.remote
class Actor1:
    def f(self):
        print("hi")

@ray.remote
class Actor2:
    def f(self):
        print("bye")
    def __repr__(self):
        return "ActorX"

a = Actor1.remote()
ray.get(a.f.remote())
b = Actor2.remote()
ray.get(b.f.remote())
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")

    assert "hi" in out_str, out_str
    assert "(Actor1 pid=" in out_str, out_str
    assert "bye" in out_str, out_str
    assert "(Actor2 pid=" not in out_str, out_str
    assert "(ActorX pid=" in out_str, out_str


def test_output():
    # Use subprocess to execute the __main__ below.
    outputs = subprocess.check_output(
        [sys.executable, __file__, "_ray_instance"],
        stderr=subprocess.STDOUT).decode()
    lines = outputs.split("\n")
    for line in lines:
        print(line)
    assert len(lines) == 2, lines


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_ray_instance":
        # Set object store memory very low so that it won't complain
        # about low shm memory in Linux environment.
        # The test failures currently complain it only has 2 GB memory,
        # so let's set it much lower than that.
        MB = 1000**2
        ray.init(num_cpus=1, object_store_memory=(100 * MB))
        ray.shutdown()
    else:
        sys.exit(pytest.main(["-v", __file__]))
