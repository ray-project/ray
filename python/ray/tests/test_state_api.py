import sys
import pytest

import ray
from ray.experimental.state.api import (
    list_actors,
    list_placement_groups,
    list_nodes,
    list_jobs,
    list_workers,
    list_tasks,
    list_objects,
)
from ray._private.test_utils import wait_for_condition
from ray.job_submission import JobSubmissionClient


def is_hex(val):
    try:
        int_val = int(val, 16)
    except ValueError:
        return False
    return f"0x{val}" == hex(int_val)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_actors(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        pass

    a = A.remote()  # noqa

    def verify():
        actor_data = list(list_actors().values())[0]
        correct_state = actor_data["state"] == "ALIVE"
        is_id_hex = is_hex(actor_data["actor_id"])
        correct_id = a._actor_id.hex() == actor_data["actor_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_actors())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_pgs(shutdown_only):
    ray.init()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify():
        pg_data = list(list_placement_groups().values())[0]
        correct_state = pg_data["state"] == "CREATED"
        is_id_hex = is_hex(pg_data["placement_group_id"])
        correct_id = pg.id.hex() == pg_data["placement_group_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_placement_groups())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_nodes(shutdown_only):
    ray.init()

    def verify():
        node_data = list(list_nodes().values())[0]
        correct_state = node_data["state"] == "ALIVE"
        is_id_hex = is_hex(node_data["node_id"])
        correct_id = ray.nodes()[0]["NodeID"] == node_data["node_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_nodes())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_jobs(shutdown_only):
    ray.init()
    client = JobSubmissionClient(
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )

    def verify():
        job_data = list(list_jobs().values())[0]
        job_id_from_api = list(list_jobs().keys())[0]
        correct_state = job_data["status"] == "SUCCEEDED"
        correct_id = job_id == job_id_from_api
        return correct_state and correct_id

    wait_for_condition(verify)
    print(list_jobs())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_workers(shutdown_only):
    ray.init()

    def verify():
        # +1 to take into account of drivers.
        worker_data = list(list_workers().values())[0]
        is_id_hex = is_hex(worker_data["worker_id"])
        print(is_id_hex)
        correct_num_workers = len(list_workers()) == ray.cluster_resources()["CPU"] + 1
        return is_id_hex and correct_num_workers

    wait_for_condition(verify)
    print(list_workers())


def test_list_tasks(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    def g(dep):
        import time

        time.sleep(30)

    out = [f.remote() for _ in range(2)]  # noqa
    g_out = g.remote(f.remote())  # noqa

    def verify():
        tasks = list(list_tasks().values())
        correct_num_tasks = len(tasks) == 4
        scheduled = len(
            list(filter(lambda task: task["scheduling_state"] == "SCHEDULED", tasks))
        )
        waiting_for_dep = len(
            list(
                filter(
                    lambda task: task["scheduling_state"] == "WAITING_FOR_DEPENDENCIES",
                    tasks,
                )
            )
        )

        return correct_num_tasks and scheduled == 3 and waiting_for_dep == 1

    wait_for_condition(verify)
    print(list_tasks())


def test_list_objects(shutdown_only):
    ray.init()
    import numpy as np

    plasma_obj = ray.put(np.ones(50 * 1024 * 1024, dtype=np.uint8))  # noqa

    @ray.remote
    def f(obj):
        print(obj)

    ray.get(f.remote(plasma_obj))

    def verify():
        print(list_objects())
        obj = list(list_objects().values())[0]
        print(plasma_obj.hex())
        return obj["object_ref"] == plasma_obj.hex()

    wait_for_condition(verify)
    print(list_objects())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
