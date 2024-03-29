import pytest
import sys
from time import sleep
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa


# TODO(sang): Also check temp dir
@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_session_name_preserved(docker_cluster):
    get_nodes_script = """
import ray
ray.init("auto")
print(ray._private.worker._global_node.session_name)
"""
    head, worker = docker_cluster

    def get_session_name(to_head=True):
        if to_head:
            output = head.exec_run(cmd=f"python -c '{get_nodes_script}'")
        else:
            output = worker.exec_run(cmd=f"python -c '{get_nodes_script}'")
        session_name = output.output.decode().strip().split("\n")[-1]
        print("Output: ", output.output.decode().strip().split("\n"))
        assert output.exit_code == 0
        return session_name

    # Make sure two nodes are alive
    wait_for_condition(get_session_name, to_head=True)
    session_name_head = get_session_name(to_head=True)
    wait_for_condition(get_session_name, to_head=False)
    session_name_worker = get_session_name(to_head=False)
    assert session_name_head == session_name_worker
    print("head killed")
    head.kill()

    sleep(2)

    head.restart()

    wait_for_condition(get_session_name, timeout=30, to_head=True)
    session_name_head_after_restart = get_session_name(to_head=True)
    wait_for_condition(get_session_name, to_head=False)
    session_name_worker_after_restart = get_session_name(to_head=False)
    assert session_name_worker_after_restart == session_name_head_after_restart
    assert session_name_head == session_name_head_after_restart
    assert session_name_worker_after_restart == session_name_worker


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
