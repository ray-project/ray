from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import signal

import pytest

import ray


@pytest.fixture
def ray_start_raylet_valgrind():
    # TODO NEED TO MAKE SURE STDOUT/STDERR from VALGRID ARE CAPTURE!!!!!!!!!!!!!!!!!!!

    # Start the Ray processes.
    ray.worker._init(start_ray_local=True, num_cpus=1, raylet_valgrind=True)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def kill_raylet_and_check_valgrind():
    if len(ray.services.all_processes[ray.services.PROCESS_TYPE_RAYLET]) == 0:
        raise Exception("No raylets were started by this process, so the "
                        "check is not meaningful.")
    for p in ray.services.all_processes[ray.services.PROCESS_TYPE_RAYLET]:
        p.send_signal(signal.SIGTERM)
        p.wait()
        if p.returncode != 0:
            raise Exception("Valgrind detected some errors.")


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_basic_task_api(ray_start_raylet_valgrind):
    kill_raylet_and_check_valgrind()
