import os
import sys

import pytest
import requests

import ray
from ray.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"

DATA_HEAD_URLS = {"GET": "http://localhost:8265/api/data/datasets/{job_id}"}

DATA_SCHEMA = [
    "state",
    "progress",
    "total",
    "total_rows",
    "ray_data_output_rows",
    "ray_data_spilled_bytes",
    "ray_data_current_bytes",
    "ray_data_cpu_usage_cores",
    "ray_data_gpu_usage_cores",
]

RESPONSE_SCHEMA = [
    "dataset",
    "job_id",
    "start_time",
    "end_time",
    "operators",
] + DATA_SCHEMA

OPERATOR_SCHEMA = [
    "name",
    "operator",
] + DATA_SCHEMA


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_unique_operator_id(ray_start_regular_shared):
    # This regression test addresses a bug caused by using a non-unique operator ID
    # format. Specifically, the third operator's name is limit11 with the ID limit112,
    # while the thirteenth operator's name is limit1 with the same ID limit112, leading
    # to a collision.
    ds = ray.data.range(100, override_num_blocks=20).limit(11)  # 3 operators
    for i in range(11):  # 11 more operators
        ds = ds.limit(1)
    ds._set_name("unique_operator_id_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("unique_operator_id_test")
    ]
    assert len(datasets) == 1
    dataset = datasets[0]

    operators = dataset["operators"]
    assert len(operators) == 14


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_datasets(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20).map_batches(lambda x: x)
    ds._set_name("data_head_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("data_head_test")
    ]

    assert len(datasets) == 1
    assert sorted(datasets[0].keys()) == sorted(RESPONSE_SCHEMA)

    dataset = datasets[0]
    assert dataset["dataset"].startswith("data_head_test")
    assert dataset["job_id"] == job_id
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None

    operators = dataset["operators"]
    assert len(operators) == 2
    op0 = operators[0]
    op1 = operators[1]
    assert sorted(op0.keys()) == sorted(OPERATOR_SCHEMA)
    assert sorted(op1.keys()) == sorted(OPERATOR_SCHEMA)
    assert {
        "operator": "Input_0",
        "name": "Input",
        "state": "FINISHED",
        "progress": 20,
        "total": 20,
    }.items() <= op0.items()
    assert {
        "operator": "ReadRange->MapBatches(<lambda>)_1",
        "name": "ReadRange->MapBatches(<lambda>)",
        "state": "FINISHED",
        "progress": 20,
        "total": 20,
    }.items() <= op1.items()

    ds._set_name("another_data_head_test")
    ds.map_batches(lambda x: x).materialize()
    data = requests.get(DATA_HEAD_URLS["GET"].format(job_id=job_id)).json()

    dataset = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("another_data_head_test")
    ][0]
    assert dataset["dataset"].startswith("another_data_head_test")
    assert dataset["job_id"] == job_id
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
