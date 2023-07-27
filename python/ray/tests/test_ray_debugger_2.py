import pexpect
import ray
import platform
import pytest
import os
import sys
from ray._private.test_utils import run_string_as_driver, wait_for_condition
from ray._private import ray_constants


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_ray_debugger_commands(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        """We support unicode too: 🐛"""
        ray.util.pdb.set_trace()

    result1 = f.remote()
    result2 = f.remote()

    wait_for_condition(
        lambda: len(
            ray.experimental.internal_kv._internal_kv_list(
                "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
            )
        )
        > 0
    )

    # Make sure that calling "continue" in the debugger
    # gives back control to the debugger loop:
    p = pexpect.spawn("ray debug")
    p.expect("Enter breakpoint index or press enter to refresh: ")
    p.sendline("0")
    p.expect("-> ray.util.pdb.set_trace()")
    p.sendline("ll")
    # Cannot use the 🐛 symbol here because pexpect doesn't support
    # unicode, but this test also does nicely:
    p.expect("unicode")
    p.sendline("c")
    p.expect("Enter breakpoint index or press enter to refresh: ")
    p.sendline("0")
    p.expect("-> ray.util.pdb.set_trace()")
    p.sendline("c")

    ray.get([result1, result2])
    p.close()


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_ray_debugger_stepping(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def g():
        return None

    @ray.remote
    def f():
        ray.util.pdb.set_trace()
        x = g.remote()
        return ray.get(x)

    result = f.remote()

    p = pexpect.spawn("ray debug")
    p.expect("Enter breakpoint index or press enter to refresh: ")
    p.sendline("0")
    p.expect("-> x = g.remote()")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("get")
    p.expect("(Pdb)")
    p.sendline("continue")

    # This should succeed now!
    ray.get(result)
    p.close()


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_ray_debugger_recursive(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def fact(n):
        if n < 1:
            return n
        ray.util.pdb.set_trace()
        n_id = fact.remote(n - 1)
        return n * ray.get(n_id)

    result = fact.remote(5)

    p = pexpect.spawn("ray debug")
    p.expect("Enter breakpoint index or press enter to refresh: ")
    p.sendline("0")
    p.expect("(Pdb)")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("remote")
    p.expect("(Pdb)")
    p.sendline("remote")

    ray.get(result)
    p.close()


@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_job_exit_cleanup(ray_start_regular):
    address = ray_start_regular["address"]

    driver_script = """
import time

import ray
ray.init(address="{}")

@ray.remote
def f():
    ray.util.rpdb.set_trace()

f.remote()
# Give the remote function long enough to actually run.
time.sleep(5)
""".format(
        address
    )

    assert not len(
        ray.experimental.internal_kv._internal_kv_list(
            "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
        )
    )

    run_string_as_driver(driver_script)

    def one_active_session():
        return len(
            ray.experimental.internal_kv._internal_kv_list(
                "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
            )
        )

    wait_for_condition(one_active_session)

    # Start the debugger. This should clean up any existing sessions that
    # belong to dead jobs.
    p = pexpect.spawn("ray debug")  # noqa:F841

    def no_active_sessions():
        return not len(
            ray.experimental.internal_kv._internal_kv_list(
                "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
            )
        )

    wait_for_condition(no_active_sessions)
    p.close()


if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
