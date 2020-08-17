import re
import subprocess
import sys
import pytest

import ray

from ray.test_utils import run_string_as_driver_nonblocking


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

    assert out_str.endswith("abc\n")
    assert err_str.split("\n")[-2].endswith("def")


def test_output():
    # Use subprocess to execute the __main__ below.
    outputs = subprocess.check_output(
        [sys.executable, __file__, "_ray_instance"],
        stderr=subprocess.STDOUT).decode()
    lines = outputs.split("\n")
    for line in lines:
        print(line)
    assert len(lines) == 3, lines
    logging_header = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\sINFO\s"
    assert re.match(
        logging_header + r"resource_spec.py:\d+ -- Starting Ray with [0-9\.]+ "
        r"GiB memory available for workers and up to [0-9\.]+ GiB "
        r"for objects. You can adjust these settings with .*?.", lines[0])
    assert re.match(
        logging_header +
        r"services.py:\d+ -- View the Ray dashboard at .*?localhost:\d+?.*",
        lines[1])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_ray_instance":
        # Since this code will run inside subprocess, we should
        # manually mock the method. This will fix the issue
        # https://github.com/ray-project/ray/pull/10162
        original_method = ray.utils.get_shared_memory_bytes
        ray.utils.get_shared_memory_bytes = lambda: 1000**3

        ray.init(num_cpus=1)
        ray.shutdown()

        ray.utils.get_shared_memory_bytes = original_method
    else:
        sys.exit(pytest.main(["-v", __file__]))
