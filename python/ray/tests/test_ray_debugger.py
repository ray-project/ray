import json
import os
import sys
from telnetlib import Telnet

import ray


def test_ray_debugger_breakpoint(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        ray.pdb.set_trace()
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


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
