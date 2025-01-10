import logging
import sys
import tempfile
from pathlib import Path

import pytest
import requests

import ray
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import format_web_url, wait_for_condition
from ray.dashboard.tests.conftest import *  # noqa
from ray.data.tests.conftest import *  # noqa
from ray.runtime_env.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


@ray.remote
class JobSignalActor:
    def __init__(self):
        self._ready = False
        self._data = None
        self._barrier = False

    def is_ready(self):
        return self._ready

    def ready(self):
        self._ready = True

    def unready(self):
        self._ready = False
        self._barrier = False

    def set_barrier(self):
        self._barrier = True

    def get_barrier(self):
        return self._barrier

    def data(self, data=None):
        if data is not None:
            self._data = data
        return self._data


def submit_job_to_virtual_cluster(
    job_client, tmp_dir, driver_script, virtual_cluster_id
):
    path = Path(tmp_dir)
    test_script_file = path / "test_script.py"
    with open(test_script_file, "w+") as file:
        file.write(driver_script)

    runtime_env = {"working_dir": tmp_dir}
    runtime_env = upload_working_dir_if_needed(runtime_env, tmp_dir, logger=logger)
    runtime_env = RuntimeEnv(**runtime_env).to_dict()

    job_id = job_client.submit_job(
        entrypoint="python test_script.py",
        entrypoint_memory=1,
        runtime_env=runtime_env,
        virtual_cluster_id=virtual_cluster_id,
    )
    return job_id


def get_virtual_cluster_nodes(webui_url, virtual_cluster_id):
    resp = requests.get(
        webui_url + "/virtual_clusters",
        timeout=10,
    )
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    assert len(result["data"]["virtualClusters"]) > 0
    virtual_clusters = result["data"]["virtualClusters"]
    for vc in virtual_clusters:
        if vc["virtualClusterId"] == virtual_cluster_id:
            return vc
    return None


def get_job_actors(webui_url, job_id=None):
    resp = requests.get(
        webui_url + "/logical/actors",
        timeout=10,
    )
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    actors = result["data"]["actors"]
    print(f"Fetched {job_id} actors: {actors}")
    if job_id is not None:
        final_actors = {}
        for actor_id, info in actors.items():
            if info["jobId"] == job_id:
                final_actors[actor_id] = info
        return final_actors
    return actors


def check_job_actor_in_virtual_cluster(webui_url, job_id, virtual_cluster_id):
    webui_url = format_web_url(webui_url)
    job_actors = get_job_actors(webui_url, job_id)
    target_virtual_cluster = get_virtual_cluster_nodes(webui_url, virtual_cluster_id)
    target_virtual_cluster_node_ids = set(
        target_virtual_cluster["nodeInstances"].keys()
    )
    for actor_id, actor_info in job_actors.items():
        assert actor_info["address"]["rayletId"] in target_virtual_cluster_node_ids


@pytest.mark.parametrize(
    "create_virtual_cluster",
    [
        {
            "node_instances": [("1c2g", 2), ("2c4g", 2), ("8c16g", 4)],
            "virtual_cluster": {
                "VIRTUAL_CLUSTER_0": {
                    "allocation_mode": "mixed",
                    "replica_sets": {
                        "1c2g": 2,
                    },
                },
                "VIRTUAL_CLUSTER_1": {
                    "allocation_mode": "mixed",
                    "replica_sets": {
                        "2c4g": 2,
                    },
                },
                "VIRTUAL_CLUSTER_2": {
                    "allocation_mode": "mixed",
                    "replica_sets": {
                        "8c16g": 4,
                    },
                },
            },
        }
    ],
    indirect=True,
)
def test_auto_parallelism(create_virtual_cluster):
    """Tests that the parallelism can be auto-deduced by
    data size and current virtual cluster's resources.
    """
    _, job_client = create_virtual_cluster
    MiB = 1024 * 1024
    # (data size, expected parallelism in each virtual cluster)
    TEST_CASES = [
        # Should all be avail_cpus * 2
        (1024, (4, 8, 64)),
        # Should be MAX_PARALLELISM, MAX_PARALLELISM, avail_cpus * 2
        # MAX_PARALLELISM = 10MB(data size) / 1MB(block size)
        (10 * MiB, (10, 10, 64)),
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        for i in range(3):  # 3 virtual clusters in total
            signal_actor_name = f"storage_actor_{i}"
            signal_actor = JobSignalActor.options(
                name=signal_actor_name, namespace="storage", num_cpus=0
            ).remote()
            # Just to make sure the actor is running, signal actor
            # should not be "ready" at this time until the submitted job
            # is running
            assert not ray.get(signal_actor.is_ready.remote())
            for data_size, expected_parallelism in TEST_CASES:
                driver_script = """
import ray
import time
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext


data_size = {data_size}
signal_actor_name = "{signal_actor_name}"
print("Job is running, data_size: ", data_size,
      "signal_actor_name: ", signal_actor_name)
ray.init(address="auto")
signal_actor = ray.get_actor(signal_actor_name, namespace="storage")
ray.get(signal_actor.ready.remote())
target_max_block_size = DataContext.get_current().target_max_block_size
class MockReader:
    def estimate_inmemory_data_size(self):
        return data_size


final_parallelism, _, _ = _autodetect_parallelism(
    parallelism=-1,
    target_max_block_size=target_max_block_size,
    ctx=DataContext.get_current(),
    datasource_or_legacy_reader=MockReader(),
)

while(True):
    barrier = ray.get(signal_actor.get_barrier.remote())
    if barrier:
        break
    time.sleep(0.1)

ray.get(signal_actor.data.remote(final_parallelism))
ray.get(signal_actor.unready.remote())
"""
                driver_script = driver_script.format(
                    signal_actor_name=signal_actor_name, data_size=data_size
                )
                submit_job_to_virtual_cluster(
                    job_client, tmp_dir, driver_script, f"VIRTUAL_CLUSTER_{i}"
                )
                # wait for job running
                wait_for_condition(
                    lambda: ray.get(signal_actor.is_ready.remote()), timeout=40
                )
                # Give finish permission to the job in case it's finished
                # before we catched the "ready" signal
                ray.get(signal_actor.set_barrier.remote())
                # wait for job finish
                wait_for_condition(
                    lambda: not ray.get(signal_actor.is_ready.remote()), timeout=30
                )
                # retrieve job data and check
                res = ray.get(signal_actor.data.remote())
                print(
                    f"Driver detected parallelism: {res}, "
                    f"expect[{i}]: {expected_parallelism[i]}"
                )
                assert res == expected_parallelism[i]


def test_job_in_virtual_cluster(create_virtual_cluster):
    """Tests that an e2e ray data job's actor can be restricted
    only in the target virtual cluster
    """
    cluster, job_client = create_virtual_cluster

    with tempfile.TemporaryDirectory() as tmp_dir:
        for i in range(3):  # 3 virtual clusters in total
            signal_actor_name = f"storage_actor_{i}"
            signal_actor = JobSignalActor.options(
                name=signal_actor_name, namespace="storage", num_cpus=0
            ).remote()
            # Just to make sure the actor is running, signal actor
            # should not be "ready" at this time until the submitted job
            # is running
            assert not ray.get(signal_actor.is_ready.remote())
            driver_script = """
import ray
import time

ray.init(address="auto")
signal_actor = ray.get_actor("{signal_actor_name}", namespace="storage")
ray.get(signal_actor.ready.remote())

def map_fn(row):
    row['species'] = row['variety'] + '!!!'
    return row

def flat_map_fn(row):
    new_row = row.copy()
    new_row['species'] = row['variety'] + '???'
    return [row, new_row]

ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
ds = ds.map(map_fn).flat_map(flat_map_fn)

while(True):
    barrier = ray.get(signal_actor.get_barrier.remote())
    if barrier:
        break
    time.sleep(0.1)

ray.get(signal_actor.unready.remote())
"""
            driver_script = driver_script.format(signal_actor_name=signal_actor_name)
            job_id = submit_job_to_virtual_cluster(
                job_client, tmp_dir, driver_script, f"VIRTUAL_CLUSTER_{i}"
            )
            # wait for job running
            wait_for_condition(
                lambda: ray.get(signal_actor.is_ready.remote()), timeout=40
            )
            # Give finish permission to the job in case it's finished
            # before we catched the "ready" signal
            ray.get(signal_actor.set_barrier.remote())
            # wait for job finish
            wait_for_condition(
                lambda: not ray.get(signal_actor.is_ready.remote()), timeout=40
            )
            check_job_actor_in_virtual_cluster(
                cluster.webui_url, job_id, f"VIRTUAL_CLUSTER_{i}"
            )


@pytest.mark.parametrize(
    "create_virtual_cluster",
    [
        {
            "node_instances": [("2c4g", 1), ("8c16g", 1)],
            "virtual_cluster": {
                "VIRTUAL_CLUSTER_0": {
                    "allocation_mode": "mixed",
                    "replica_sets": {
                        "2c4g": 1,
                    },
                },
                "VIRTUAL_CLUSTER_1": {
                    "allocation_mode": "mixed",
                    "replica_sets": {
                        "8c16g": 1,
                    },
                },
            },
        }
    ],
    indirect=True,
)
def test_start_actor_timeout(create_virtual_cluster):
    """Tests that when requesting resources that exceeding only
    the virtual cluster but the whole cluster, will raise an exception
    on timeout while waiting for actors."""

    cluster, job_client = create_virtual_cluster

    # achievable & unachievable cpu requirements
    TEST_CASES = [
        (1, 4),  # for virtual cluster 0
        (6, 36),  # for virtual cluster 1
    ]
    with tempfile.TemporaryDirectory() as tmp_dir:
        for i in range(2):  # 2 virtual clusters in total
            signal_actor_name = f"storage_actor_{i}"
            signal_actor = JobSignalActor.options(
                name=signal_actor_name, namespace="storage", num_cpus=0
            ).remote()
            # Just to make sure the actor is running, signal actor
            # should not be "ready" at this time until the submitted job
            # is running
            ray.get(signal_actor.is_ready.remote())
            driver_script = """
import ray
import time
from ray.exceptions import GetTimeoutError


class UDFClass:
    def __call__(self, x):
        return x

ray.init(address="auto")
signal_actor = ray.get_actor("{signal_actor_name}", namespace="storage")
ray.get(signal_actor.ready.remote())
ray.data.DataContext.get_current().wait_for_min_actors_s = 10
result = False
# Specify an unachievable resource requirement to ensure
# we timeout while waiting for actors.

achievable_cpus = {achievable_cpus}
unachievable_cpus = {unachievable_cpus}
print("Requesting achievable_cpus: ", achievable_cpus)
try:
    ray.data.range(10).map_batches(
        UDFClass,
        batch_size=1,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_cpus=achievable_cpus,
    ).take_all()
    result = True
except GetTimeoutError as e:
    print("This shouldn't happen")
    result = False

print("Requesting unachievable_cpus: ", unachievable_cpus)
try:
    ray.data.range(10).map_batches(
        UDFClass,
        batch_size=1,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_cpus=unachievable_cpus,
    ).take_all()
    result = False
except GetTimeoutError as e:
    result = result and True

print("Final result: ", result)

while(True):
    barrier = ray.get(signal_actor.get_barrier.remote())
    if barrier:
        break
    time.sleep(0.1)

ray.get(signal_actor.data.remote(result))
ray.get(signal_actor.unready.remote())
"""
            driver_script = driver_script.format(
                signal_actor_name=signal_actor_name,
                achievable_cpus=TEST_CASES[i][0],
                unachievable_cpus=TEST_CASES[i][1],
            )

            submit_job_to_virtual_cluster(
                job_client, tmp_dir, driver_script, f"VIRTUAL_CLUSTER_{i}"
            )
            # wait for job running
            wait_for_condition(
                lambda: ray.get(signal_actor.is_ready.remote()), timeout=30
            )
            # Give finish permission to the job in case it's finished
            # before we catched the "ready" signal
            ray.get(signal_actor.set_barrier.remote())
            # wait for job finish
            wait_for_condition(
                lambda: not ray.get(signal_actor.is_ready.remote()), timeout=600
            )
            assert ray.get(signal_actor.data.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
