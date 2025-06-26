from unittest.mock import patch

import datasets
import pyarrow
import pytest
from packaging.version import Version

import ray
from ray.data.dataset import Dataset, MaterializedDataset
from ray.tests.conftest import *  # noqa


@pytest.fixture
def mock_hf_dataset():
    """Create a mock HuggingFace dataset for testing."""
    texts = [
        "Climate change is a serious threat to our planet",
        "We need to take action on global warming",
        "Renewable energy is the future",
        "Fossil fuels are destroying the environment",
        "Solar power is becoming more affordable",
        "Wind energy is growing rapidly",
        "Electric vehicles are the way forward",
        "Carbon emissions must be reduced",
        "Green technology is advancing quickly",
        "Sustainability is important for future generations",
        "Climate science is well established",
        "Ocean levels are rising due to warming",
        "Extreme weather events are increasing",
        "Biodiversity loss is accelerating",
        "Deforestation contributes to climate change",
        "Clean energy jobs are growing",
        "Energy efficiency saves money",
        "Public transportation reduces emissions",
        "Plant-based diets help the environment",
        "Recycling is essential for sustainability",
    ]

    # Create labels array with exactly the same length as texts
    labels = [i % 2 for i in range(len(texts))]  # Alternating 0s and 1s

    return datasets.Dataset.from_dict(
        {
            "text": texts,
            "label": labels,
        }
    )


@pytest.fixture
def mock_hf_dataset_dict(mock_hf_dataset):
    """Create a mock HuggingFace DatasetDict for testing."""
    return datasets.DatasetDict({"train": mock_hf_dataset})


@pytest.fixture
def mock_hf_iterable_dataset():
    """Create a mock HuggingFace IterableDataset for testing."""
    texts = [
        "Streaming climate tweet 1: The planet is warming",
        "Streaming climate tweet 2: Renewable energy is key",
        "Streaming climate tweet 3: We must act now",
        "Streaming climate tweet 4: Solar panels everywhere",
        "Streaming climate tweet 5: Wind turbines are beautiful",
        "Streaming climate tweet 6: Electric cars are the future",
        "Streaming climate tweet 7: Carbon neutral by 2050",
        "Streaming climate tweet 8: Green energy revolution",
        "Streaming climate tweet 9: Climate action needed",
        "Streaming climate tweet 10: Sustainable development",
        "Streaming climate tweet 11: Ocean conservation",
        "Streaming climate tweet 12: Forest protection",
        "Streaming climate tweet 13: Clean air matters",
        "Streaming climate tweet 14: Water conservation",
        "Streaming climate tweet 15: Biodiversity protection",
    ]

    labels = [1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1]

    dataset = datasets.Dataset.from_dict(
        {
            "text": texts,
            "label": labels,
        }
    )
    iterable_dataset = dataset.to_iterable_dataset()
    iterable_dataset.expected_count = len(texts)
    return iterable_dataset


def hfds_assert_equals(hfds: datasets.Dataset, ds: Dataset):
    hfds_table = hfds.data.table
    ds_table = pyarrow.concat_tables([ray.get(tbl) for tbl in ds.to_arrow_refs()])

    sorting = [(name, "descending") for name in hfds_table.column_names]
    hfds_table = hfds_table.sort_by(sorting)
    ds_table = ds_table.sort_by(sorting)

    assert hfds_table.equals(ds_table)


@pytest.mark.parametrize("num_par", [1, 4])
def test_from_huggingface(mock_hf_dataset_dict, ray_start_regular_shared, num_par):
    # Check that DatasetDict is not directly supported.
    assert isinstance(mock_hf_dataset_dict, datasets.DatasetDict)
    with pytest.raises(
        DeprecationWarning,
        match="You provided a Hugging Face DatasetDict",
    ):
        ray.data.from_huggingface(mock_hf_dataset_dict)

    ray_datasets = {
        "train": ray.data.from_huggingface(
            mock_hf_dataset_dict["train"], override_num_blocks=num_par
        ),
    }

    assert isinstance(ray_datasets["train"], ray.data.Dataset)
    hfds_assert_equals(mock_hf_dataset_dict["train"], ray_datasets["train"])

    # Test reading in a split Hugging Face dataset yields correct individual datasets
    base_hf_dataset = mock_hf_dataset_dict["train"]
    hf_dataset_split = base_hf_dataset.train_test_split(test_size=0.2)
    ray_dataset_split_train = ray.data.from_huggingface(hf_dataset_split["train"])
    assert ray_dataset_split_train.count() == hf_dataset_split["train"].num_rows


@pytest.mark.skipif(
    datasets.Version(datasets.__version__) < datasets.Version("2.8.0"),
    reason="IterableDataset.iter() added in 2.8.0",
)
@pytest.mark.skipif(
    Version(pyarrow.__version__) < Version("8.0.0"),
    reason="pyarrow.Table.to_reader() added in 8.0.0",
)
# Note, pandas is excluded here because IterableDatasets do not support pandas format.
@pytest.mark.parametrize(
    "batch_format",
    [None, "numpy", "arrow", "torch", "tensorflow", "jax"],
)
def test_from_huggingface_streaming(
    mock_hf_iterable_dataset, batch_format, ray_start_regular_shared
):
    hfds = mock_hf_iterable_dataset.with_format(batch_format)
    assert isinstance(hfds, datasets.IterableDataset)

    ds = ray.data.from_huggingface(hfds)
    expected_count = mock_hf_iterable_dataset.expected_count
    assert ds.count() == expected_count


@pytest.mark.skipif(
    datasets.Version(datasets.__version__) < datasets.Version("2.8.0"),
    reason="IterableDataset.iter() added in 2.8.0",
)
def test_from_huggingface_dynamic_generated(ray_start_regular_shared):
    # https://github.com/ray-project/ray/issues/49529
    # Mock the dynamic dataset loading
    mock_dataset = datasets.Dataset.from_dict(
        {
            "text": [
                "dynamic tweet 1",
                "dynamic tweet 2",
                "dynamic tweet 3",
                "dynamic tweet 4",
                "dynamic tweet 5",
            ],
            "label": [0, 1, 0, 1, 0],
        }
    )
    mock_iterable = mock_dataset.to_iterable_dataset()

    with patch("datasets.load_dataset", return_value=mock_iterable):
        hfds = datasets.load_dataset(
            "dataset-org/dream",
            split="test",
            streaming=True,
            trust_remote_code=True,
        )
        ds = ray.data.from_huggingface(hfds)
        ds.take(1)


@pytest.mark.parametrize("override_num_blocks", [1, 2, 4, 8])
def test_from_huggingface_override_num_blocks(
    mock_hf_dataset, ray_start_regular_shared, override_num_blocks
):
    """Test that override_num_blocks works correctly with HuggingFace datasets."""
    hf_train = mock_hf_dataset

    ds_subset = ray.data.from_huggingface(
        hf_train, override_num_blocks=override_num_blocks
    )

    assert isinstance(ds_subset, MaterializedDataset)

    # Verify number of blocks
    assert ds_subset.num_blocks() == override_num_blocks

    # Verify data integrity
    assert ds_subset.count() == hf_train.num_rows
    hfds_assert_equals(hf_train, ds_subset)

    # Test with a smaller subset to test edge cases
    small_size = max(override_num_blocks * 3, 10)
    hf_small = hf_train.select(range(min(small_size, hf_train.num_rows)))
    ds_small = ray.data.from_huggingface(
        hf_small, override_num_blocks=override_num_blocks
    )

    # Verify number of blocks
    assert ds_small.num_blocks() == override_num_blocks

    # Verify data integrity
    assert ds_small.count() == hf_small.num_rows
    hfds_assert_equals(hf_small, ds_small)


def test_from_huggingface_with_parquet_files(mock_hf_dataset, ray_start_regular_shared):
    """Test the distributed read path when parquet file URLs are available."""
    from ray.data._internal.datasource.huggingface_datasource import (
        HuggingFaceDatasource,
    )

    # Mock the list_parquet_urls_from_dataset method to return fake parquet URLs
    # This should trigger the distributed read via parquet files
    mock_parquet_urls = [
        "https://huggingface.co/datasets/test/parquet/train-00000-of-00001.parquet",
        "https://huggingface.co/datasets/test/parquet/train-00001-of-00001.parquet",
    ]

    with patch.object(
        HuggingFaceDatasource,
        "list_parquet_urls_from_dataset",
        return_value=mock_parquet_urls,
    ):
        # Mock the read_parquet function to return our mock dataset
        with patch("ray.data.read_api.read_parquet") as mock_read_parquet:
            # Create a mock dataset that matches our mock_hf_dataset
            mock_ds = ray.data.from_items(
                [
                    {"text": text, "label": label}
                    for text, label in zip(
                        mock_hf_dataset["text"], mock_hf_dataset["label"]
                    )
                ]
            )
            mock_read_parquet.return_value = mock_ds

            ds = ray.data.from_huggingface(mock_hf_dataset)

            # Verify that read_parquet was called with the expected parameters
            mock_read_parquet.assert_called_once()
            call_args = mock_read_parquet.call_args

            # Check that the parquet URLs were passed
            assert call_args[0][0] == mock_parquet_urls

            # Check that the filesystem is HTTPFileSystem
            assert "filesystem" in call_args[1]
            assert "HTTPFileSystem" in str(type(call_args[1]["filesystem"]))

            # Check that retry_exceptions includes FileNotFoundError and ClientResponseError
            assert "ray_remote_args" in call_args[1]
            assert (
                FileNotFoundError in call_args[1]["ray_remote_args"]["retry_exceptions"]
            )

            # Verify the dataset was created successfully
            assert isinstance(ds, MaterializedDataset)
            assert ds.count() == mock_hf_dataset.num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
