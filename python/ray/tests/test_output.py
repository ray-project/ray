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


exempted_logs = [
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\sWARNING\s"
    r"services.py:\d{4} -- WARNING: The object store is using "
    r".* instead of "
    r"/dev/shm because /dev/shm has only \d+ bytes available. "
    r"This may slow down performance! You may be able to free "
    r"up space by deleting files in /dev/shm or terminating "
    r"any running plasma_store_server processes. If you are "
    r"inside a Docker container, you may need to pass an "
    r"argument with the flag '--shm-size' to 'docker run'."  # from services.py
]


def test_output():
    # Use subprocess to execute the __main__ below.
    outputs = subprocess.check_output(
        [sys.executable, __file__, "_ray_instance"],
        stderr=subprocess.STDOUT).decode()
    original_lines = outputs.split("\n")
    lines = []
    for line in original_lines:
        for exempted_log in exempted_logs:
            if not re.match(exempted_log, line):
                lines.append(line)
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
        ray.init(num_cpus=1)
        ray.shutdown()
    else:
        sys.exit(pytest.main(["-v", __file__]))
