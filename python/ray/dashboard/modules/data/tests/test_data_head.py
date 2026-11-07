import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ray
from ray._private.test_utils import request_with_auth_token
from ray.dashboard.modules.data.data_head import DataHead
from ray.data._internal.stats import _to_metric_tag_value
from ray.job_submission import JobSubmissionClient
from ray.tests.conftest import *  # noqa

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
    "ray_data_memory_usage_bytes",
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
    "queued_blocks",
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

    data = request_with_auth_token(
        "GET", DATA_HEAD_URLS["GET"].format(job_id=job_id)
    ).json()
    datasets = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("unique_operator_id_test")
    ]
    assert len(datasets) == 1
    dataset = datasets[0]

    operators = dataset["operators"]
    assert len(operators) == 3  # Should be 3 because of limiter operator fusion.


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_datasets(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20).map_batches(lambda x: x)
    ds.set_name("data_head_test")
    ds.materialize()

    client = JobSubmissionClient()
    jobs = client.list_jobs()
    assert len(jobs) == 1, jobs
    job_id = jobs[0].job_id

    data = request_with_auth_token(
        "GET", DATA_HEAD_URLS["GET"].format(job_id=job_id)
    ).json()
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
    data = request_with_auth_token(
        "GET", DATA_HEAD_URLS["GET"].format(job_id=job_id)
    ).json()

    dataset = [
        dataset
        for dataset in data["datasets"]
        if dataset["dataset"].startswith("another_data_head_test")
    ][0]
    assert dataset["dataset"].startswith("another_data_head_test")
    assert dataset["job_id"] == job_id
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None


# https://github.com/ray-project/ray/issues/67230
async def test_get_datasets_matches_escaped_operator_tags():
    """Prometheus operator labels are ASCII-escaped, while the stats actor reports
    the original operator IDs. Metrics must still be matched to the operator."""
    operator = "Filter(col('text') == '中文')_1"
    stats_actor = MagicMock()
    stats_actor.get_datasets.remote = AsyncMock(
        return_value={
            "dataset": {"start_time": 0, "operators": {"Input_0": {}, operator: {}}}
        }
    )

    async def query_prometheus(query):
        if "by (dataset, operator)" in query:
            labels = {"dataset": "dataset", "operator": _to_metric_tag_value(operator)}
        else:
            labels = {"dataset": "dataset"}
        return {"data": {"result": [{"metric": labels, "value": [0, "3"]}]}}

    data_head = SimpleNamespace(
        session_name="session", _query_prometheus=query_prometheus
    )
    request = SimpleNamespace(match_info={"job_id": "job_id"})
    with patch("ray.is_initialized", return_value=True), patch(
        "ray.data._internal.stats.get_or_create_stats_actor",
        return_value=stats_actor,
    ):
        response = await DataHead.get_datasets(data_head, request)

    (dataset,) = json.loads(response.text)["datasets"]
    operators = {op["operator"]: op for op in dataset["operators"]}
    assert operators[operator]["ray_data_output_rows"] == {"max": "3"}
    assert operators["Input_0"]["ray_data_output_rows"] == {"max": 0}


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
