import sys

import pytest
import requests

import ray
from ray.tests.conftest import *  # noqa
from ray.dashboard.modules.anyscale.data.data_schema import (
    OperatorState,
    DatasetMetrics,
    DatasetResponse,
)

DATA_HEAD_URLS = "http://localhost:8265/api/anyscale/data"


def test_get_datasets(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20).map_batches(lambda x: x)
    ds.materialize()

    job_response = requests.get(f"{DATA_HEAD_URLS}/jobs").json()
    assert len(job_response) == 1, job_response
    job_id = job_response[0]["job_id"]

    dataset_response = DatasetResponse(
        **requests.get(f"{DATA_HEAD_URLS}/datasets").json()
    )
    assert len(dataset_response.datasets) == 1
    assert {
        "job_id": job_id,
        "state": OperatorState.FINISHED,
        "progress": 20,
        "total": 20,
    }.items() <= dataset_response.datasets[0].items()
    assert dataset_response.datasets[0].get("session_name")

    operators = DatasetMetrics(**dataset_response.datasets[0]).operator_metrics
    assert len(operators) == 2
    op0, op1 = operators[0], operators[1]
    assert {
        "name": "Input",
        "state": OperatorState.FINISHED,
        "progress": 20,
        "total": 20,
    }.items() <= op0.items()
    assert {
        "name": "ReadRange->MapBatches(<lambda>)",
        "state": "FINISHED",
        "progress": 20,
        "total": 20,
    }.items() <= op1.items()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
