import pytest

import ray
from ray.experimental.state.api import (
    list_actors,
    list_placement_groups,
    list_nodes,
    list_jobs,
    list_workers,
)
from ray._private.test_utils import wait_for_condition
from ray.job_submission import JobSubmissionClient


def test_list_actors(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        pass

    a = A.remote()  # noqa

    def f():
        state = list(list_actors().values())[0]["state"]
        return state == "ALIVE"

    wait_for_condition(lambda: f())
    print(list_actors())


def test_list_pgs(shutdown_only):
    ray.init()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def f():
        state = list(list_placement_groups().values())[0]["state"]
        return state == "CREATED"

    wait_for_condition(lambda: f())
    print(list_placement_groups())


def test_list_nodes(shutdown_only):
    ray.init()
    print(list_nodes())

    def f():
        state = list(list_nodes().values())[0]["state"]
        return state == "ALIVE"

    wait_for_condition(lambda: f())
    print(list_nodes())


def test_list_jobs(shutdown_only):
    ray.init()
    client = JobSubmissionClient(
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )

    def f():
        state = list(list_jobs().values())[0]["status"]
        return state == "SUCCEEDED"

    wait_for_condition(f)
    print(list_jobs())


def test_list_workers(shutdown_only):
    ray.init()
    print(list_workers())

    def f():
        # +1 to take into account of drivers.
        return len(list_workers()) == ray.cluster_resources()["CPU"] + 1

    wait_for_condition(f)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
