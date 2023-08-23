import pytest
import sys
from time import sleep
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_nodes_liveness(docker_cluster):
    get_nodes_script = """
import ray
ray.init("auto")
print(sum([1 if n["Alive"] else 0 for n in ray.nodes()]))
"""
    head, worker = docker_cluster

    def check_alive(n):
        output = worker.exec_run(cmd=f"python -c '{get_nodes_script}'")
        text = output.output.decode().strip().split("\n")[-1]
        print("Alive nodes: ", text)
        assert output.exit_code == 0
        return n == int(text)

    # Make sure two nodes are alive
    wait_for_condition(check_alive, n=2)
    print("head killed")
    head.kill()

    sleep(2)

    head.restart()
    # When GCS restarts, a new raylet is added
    # and the old dead raylet is going to take a while to be marked dead.
    # So there should be 3 alive nodes
    wait_for_condition(check_alive, timeout=10, n=3)
    # Later, GCS detect the old raylet dead and the alive nodes will be 2
    wait_for_condition(check_alive, timeout=30, n=2)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
