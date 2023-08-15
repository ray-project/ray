import datasets
import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_huggingface(ray_start_regular_shared):
    data = datasets.load_dataset("tweet_eval", "emotion")

    # Check that DatasetDict is not directly supported.
    assert isinstance(data, datasets.DatasetDict)
    with pytest.raises(
        DeprecationWarning,
        match="You provided a Hugging Face DatasetDict",
    ):
        ray.data.from_huggingface(data)

    ray_datasets = {
        "train": ray.data.from_huggingface(data["train"]),
        "validation": ray.data.from_huggingface(data["validation"]),
        "test": ray.data.from_huggingface(data["test"]),
    }

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
    ray_dataset_split_train = ray.data.from_huggingface(hf_dataset_split["train"])
    ray_dataset_split_test = ray.data.from_huggingface(hf_dataset_split["test"])
    assert ray_dataset_split_train.count() == hf_dataset_split["train"].num_rows
    assert ray_dataset_split_test.count() == hf_dataset_split["test"].num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
