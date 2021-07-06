import os
import subprocess
import sys
import time

import ray
from ray.experimental import workflow


def test_workflow_lifetime():
    subprocess.run(["ray start --head"], shell=True)
    time.sleep(1)
    script = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "workflows_to_fail.py")
    proc = subprocess.Popen([sys.executable, script])
    time.sleep(10)
    proc.kill()
    time.sleep(1)
    # connect to the cluster
    ray.init(address="auto", namespace="workflow")
    output = workflow.get_output("cluster_failure")
    assert ray.get(output) == 20
    ray.shutdown()
    subprocess.run(["ray stop"], shell=True)
    time.sleep(1)
