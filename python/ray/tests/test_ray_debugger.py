import json
import os
import platform
import sys
from telnetlib import Telnet

import pexpect
import pytest
import ray


def test_ray_debugger_breakpoint(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        ray.util.pdb.set_trace()
        return 1

    result = f.remote()

    # Wait until the breakpoint is hit:
    while True:
        active_sessions = ray.experimental.internal_kv._internal_kv_list(
            "RAY_PDB_")
        if len(active_sessions) > 0:
            break

    # Now continue execution:
    session = json.loads(
        ray.experimental.internal_kv._internal_kv_get(active_sessions[0]))
    host, port = session["pdb_address"].split(":")
    tn = Telnet(host, int(port))
    tn.write(b"c\n")

    # This should succeed now!
    ray.get(result)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_ray_debugger_commands(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        """We support unicode too: 🐛"""
        ray.util.pdb.set_trace()

    result1 = f.remote()
    result2 = f.remote()

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


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
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


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
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


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
