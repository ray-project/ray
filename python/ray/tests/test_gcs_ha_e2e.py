import sys
from time import sleep

import pytest

from ray._common.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_nodes_liveness(docker_cluster):
    get_nodes_script = """
import ray
ray.init("auto")
print("Num Alive Nodes: ", sum([1 if n["Alive"] else 0 for n in ray.nodes()]))
"""
    head, worker = docker_cluster

    def check_alive(n):
        output = worker.exec_run(cmd=f"python -c '{get_nodes_script}'")
        output_msg = output.output.decode().strip().split("\n")
        print("Output: ", output_msg)
        assert output.exit_code == 0
        for msg in output_msg:
            if "Num Alive Nodes" in msg:
                return n == int(msg.split()[-1])
        return False

    # Make sure two nodes are alive
    wait_for_condition(check_alive, n=2)
    print("head killed")
    head.kill()

    sleep(2)

    head.restart()
    # When GCS restarts, a new raylet is added
    # GCS mark the old raylet as dead so the alive nodes will be 2
    num_retries = 5
    while num_retries > 0:
        num_retries -= 1
        assert check_alive(2)
        sleep(0.1)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
