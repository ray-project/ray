import os
import subprocess
import sys
import time

import pytest
import ray
from ray.experimental import workflow


@pytest.mark.skip(reason="Blocked by issue #16951, where the exiting of "
                  "driver kills the task launched by a detached "
                  "named actor.")
def test_workflow_lifetime():
    subprocess.run(["ray start --head"], shell=True)
    time.sleep(1)
    script = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "driver_terminated.py")
    sleep_duration = 5
    proc = subprocess.Popen([sys.executable, script, str(sleep_duration)])
    # TODO(suquark): also test killing the driver after fixing
    # https://github.com/ray-project/ray/issues/16951
    # now we only let the driver exit normally
    proc.wait()
    time.sleep(1)
    # connect to the cluster
    ray.init(address="auto", namespace="workflow")
    output = workflow.get_output("driver_terminated")
    assert ray.get(output) == 20
    ray.shutdown()
    subprocess.run(["ray stop"], shell=True)
    time.sleep(1)
