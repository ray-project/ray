import sys
import re

import pytest

import ray.scripts.scripts as scripts
from click.testing import CliRunner

def test_ray_start():
    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--head", "--log-new-style"])
    assert runner.invoke(scripts.stop).exit_code == 0

    expected_lines = [
        r"Local node IP: .+",
        r"Available RAM",
        r"  Workers: .+ GiB",
        r"  Objects: .+ GiB",
        r"",
        r"  To adjust these values, use",
        r"    ray\.init\(memory=<bytes>, object_store_memory=<bytes>\)",
        r"Dashboard URL: .+",
        r"",
        r"--------------------",
        r"Ray runtime started.",
        r"--------------------",
        r"",
        r"Next steps",
        r"  To connect to this Ray runtime from another node, run",
        r"    ray start --address='.+' --redis-password='.+'",
        r"",
        r"  Alternatively, use the following Python code:",
        r"    import ray",
        r"    ray\.init\(address='auto', redis_password='.+'\)",
        r"",
        r"  If connection fails, check your firewall settings other network"
        r" configuration.",
        r"",
        r"  To terminate the Ray runtime, run",
        r"    ray stop",
        r""
    ]

    # for debugging
    # output_lines = result.output.split("\n")
    # for exp, out in zip(expected_lines, output_lines):
    #     print(exp, out, re.fullmatch(exp + r" *", out) is not None)
    # assert False

    expected = r" *\n".join(expected_lines)
    assert re.fullmatch(expected, result.output) is not None
    assert result.exit_code == 0

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
