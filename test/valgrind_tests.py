from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

import ray


@pytest.fixture
def ray_start_raylet_valgrind():
    # Start the Ray processes.
    ray.worker._init(num_cpus=1, raylet_valgrind=True)
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
            os._exit(-1)
            # WHY NOT RAISE EXCEPTION INSTEAD?????????????????????????????????????????


def test_basic_task_api(ray_start_raylet_valgrind):
    kill_raylet_and_check_valgrind()
