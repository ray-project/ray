import pytest
import ray
import datasets
from ray.tests.conftest import *  # noqa


def test_huggingface(ray_start_regular_shared):
    data = datasets.load_dataset("tweet_eval", "emotion")

    assert isinstance(data, datasets.DatasetDict)

    ray_datasets = ray.data.from_huggingface(data)
    assert isinstance(ray_datasets, dict)

    assert ray.get(ray_datasets["train"].to_arrow_refs())[0].equals(
        data["train"].data.table
    )

    ray_dataset = ray.data.from_huggingface(data["train"])
    assert isinstance(ray_dataset, ray.data.Dataset)

    assert ray.get(ray_dataset.to_arrow_refs())[0].equals(data["train"].data.table)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
