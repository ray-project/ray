import os
import pytest
import sys
import threading
from time import sleep

from ray.tests.conftest_docker import *  # noqa
from ray._private.test_utils import wait_for_condition
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME

scripts = """
import json
import os

import ray

from ray import serve

@serve.deployment
class GetPID:
    def __call__(self, *args):
        return {{"pid": os.getpid()}}

serve.run(GetPID.options(num_replicas={num_replicas}).bind())
"""

check_script = """
import ray
import requests

@ray.remote
def get_pid():
    return requests.get("http://127.0.0.1:8000/").json()["pid"]

pids = {{
    requests.get("http://127.0.0.1:8000/").json()["pid"]
    for _ in range(20)
}}
print(pids)
assert len(pids) == {num_replicas}
"""

check_ray_nodes_script = """
import ray

ray.init(address="auto")
print(ray.nodes())
"""


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_serve_basic(docker_cluster):
    # This test covers the basic cases for serve ha
    # - It starts the serve on worker nodes.
    # - Check the deployment is OK
    # - Stop headnode
    # - Check the serve app is running healthy
    # - Start a reconfig (2 replicas) and it'll hang
    # - Start head node. The script will continue once GCS is back
    # - Make sure two replicas are there

    # TODO(iycheng): Update serve to better integrate with GCS HA:
    #   - Make sure no task can run in the raylet where GCS is deployed.

    head, worker = docker_cluster
    output = worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=1)}'")
    assert output.exit_code == 0, output.output
    assert b"Adding 1 replica to deployment " in output.output

    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=1)}'")
    assert output.exit_code == 0, output.output

    # Kill the head node.
    head.kill()

    # Make sure the Serve application can still handle traffic.
    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=1)}'")
    assert output.exit_code == 0, output.output

    # Scale up the application in a background thread so we can concurrently kill the
    # head node.
    def reconfig():
        worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=2)}'")

    t = threading.Thread(target=reconfig)
    t.start()
    sleep(1)

    # Updating the application should continue once the head node is back online.
    head.restart()
    t.join()

    # Ensure head node is up before calling check_script on the worker again.
    def check_for_head_node_come_back_up():
        _output = head.exec_run(cmd=f"python -c '{check_ray_nodes_script}'")
        return (
            _output.exit_code == 0
            and bytes(HEAD_NODE_RESOURCE_NAME, "utf-8") in _output.output
        )

    wait_for_condition(check_for_head_node_come_back_up)

    # Check that the application has been updated.
    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=2)}'")
    assert output.exit_code == 0, output.output

    # Make sure the serve controller still runs on the head node after restart.
    check_controller_head_node_script = """
import ray
import requests
from ray.serve.schema import ServeInstanceDetails
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
ray.init(address="auto")
head_node_id = ray.get_runtime_context().get_node_id()
serve_details = ServeInstanceDetails(
    **requests.get("http://localhost:52365/api/serve/applications/").json())
assert serve_details.controller_info.node_id == head_node_id
"""
    output = head.exec_run(cmd=f"python -c '{check_controller_head_node_script}'")
    assert output.exit_code == 0, output.output


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
