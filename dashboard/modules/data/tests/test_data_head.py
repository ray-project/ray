import ray
import requests
import sys
import pytest

DATA_HEAD_URLS = {"GET": "http://localhost:8265/api/data/datasets"}

DATA_SCHEMA = [
    "state",
    "progress",
    "total",
    "ray_data_output_bytes",
    "ray_data_spilled_bytes",
    "ray_data_current_bytes",
]

RESPONSE_SCHEMA = [
    "dataset",
    "start_time",
    "end_time",
    "operators",
] + DATA_SCHEMA

OPERATOR_SCHEMA = [
    "operator",
] + DATA_SCHEMA


def test_get_datasets():
    ray.init()
    ds = ray.data.range(100, parallelism=20).map_batches(lambda x: x)
    ds._set_name("data_head_test")
    ds.materialize()

    data = requests.get(DATA_HEAD_URLS["GET"]).json()

    assert len(data["datasets"]) == 1
    assert sorted(data["datasets"][0].keys()) == sorted(RESPONSE_SCHEMA)

    dataset = data["datasets"][0]
    assert dataset["dataset"].startswith("data_head_test")
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None

    operators = dataset["operators"]
    assert len(operators) == 2
    op0 = operators[0]
    assert sorted(op0.keys()) == sorted(OPERATOR_SCHEMA)
    assert op0["operator"] == "Input0"
    assert op0["progress"] == 20
    assert op0["total"] == 20
    assert op0["state"] == "FINISHED"
    assert operators[1]["operator"] == "ReadRange->MapBatches(<lambda>)1"

    ds.map_batches(lambda x: x).materialize()
    data = requests.get(DATA_HEAD_URLS["GET"]).json()

    assert len(data["datasets"]) == 2
    dataset = data["datasets"][1]
    assert dataset["dataset"].startswith("data_head_test")
    assert dataset["state"] == "FINISHED"
    assert dataset["end_time"] is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
