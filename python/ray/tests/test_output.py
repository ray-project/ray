import re
import subprocess
import sys
import pytest
import ray


def test_output():
    outputs = subprocess.check_output(
        [sys.executable, __file__, "_ray_instance"],
        stderr=subprocess.STDOUT).decode()
    lines = outputs.split("\n")
    assert len(lines) == 3
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
        ray.init()
        ray.shutdown()
    else:
        sys.exit(pytest.main(["-v", __file__]))
