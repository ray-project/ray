import datasets
import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_huggingface(ray_start_regular_shared):
    data = datasets.load_dataset("tweet_eval", "emotion")

    assert isinstance(data, datasets.DatasetDict)

    ray_datasets = ray.data.from_huggingface(data)
    assert isinstance(ray_datasets, dict)

    assert ray.get(ray_datasets["train"].to_arrow_refs())[0].equals(
        data["train"].data.table
    )
    assert ray_datasets["train"].count() == data["train"].num_rows
    assert ray_datasets["test"].count() == data["test"].num_rows

    ray_dataset = ray.data.from_huggingface(data["train"])
    assert isinstance(ray_dataset, ray.data.Dataset)

    assert ray.get(ray_dataset.to_arrow_refs())[0].equals(data["train"].data.table)

    # Test reading in a split Hugging Face dataset yields correct individual datasets
    base_hf_dataset = data["train"]
    hf_dataset_split = base_hf_dataset.train_test_split(test_size=0.2)
    ray_dataset_split = ray.data.from_huggingface(hf_dataset_split)
    assert ray_dataset_split["train"].count() == hf_dataset_split["train"].num_rows
    assert ray_dataset_split["test"].count() == hf_dataset_split["test"].num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
