import sys
import pytest

from typing import List

import ray

from click.testing import CliRunner
from ray.cluster_utils import cluster_not_supported
from ray.experimental.state.api import (
    list_actors,
    list_placement_groups,
    list_nodes,
    list_jobs,
    list_workers,
)
from ray._private.test_utils import wait_for_condition
from ray.job_submission import JobSubmissionClient
from ray.experimental.state.state_cli import list_state_cli_group


def is_hex(val):
    try:
        int_val = int(val, 16)
    except ValueError:
        return False
    return f"0x{val}" == hex(int_val)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported on Windows")
def test_cli_apis_sanity_check(ray_start_cluster):
    """Test all of CLI APIs work as expected."""
    cluster = ray_start_cluster
    for _ in range(4):
        cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    runner = CliRunner()

    client = JobSubmissionClient(
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
    )

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    class Actor:
        pass

    obj = ray.put(3)  # noqa
    task = f.remote()  # noqa
    actor = Actor.remote()  # noqa
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify_output(resource_name, necessary_substrings: List[str]):
        result = runner.invoke(list_state_cli_group, [resource_name])
        exit_code_correct = result.exit_code == 0
        substring_matched = all(
            substr in result.output for substr in necessary_substrings
        )
        print(result.output)
        return exit_code_correct and substring_matched

    assert verify_output("actors", ["actor_id"])
    assert verify_output("workers", ["worker_id"])
    assert verify_output("nodes", ["node_id"])
    assert verify_output("placement-groups", ["placement_group_id"])
    assert verify_output("jobs", ["raysubmit"])


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
