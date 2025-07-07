from unittest.mock import MagicMock, patch

import datasets
import pyarrow
import pytest
import requests
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


@pytest.fixture
def mock_parquet_urls():
    """Fixture providing mock parquet URLs for testing."""
    return [
        "https://huggingface.co/datasets/test/parquet/train-00000-of-00001.parquet",
        "https://huggingface.co/datasets/test/parquet/train-00001-of-00001.parquet",
    ]


@pytest.fixture
def mock_resolved_urls():
    """Fixture providing mock resolved URLs (after HTTP redirects) for testing."""
    return [
        "https://cdn-lfs.huggingface.co/datasets/test/parquet/train-00000-of-00001.parquet",
        "https://cdn-lfs.huggingface.co/datasets/test/parquet/train-00001-of-00001.parquet",
    ]


@pytest.fixture
def mock_ray_dataset(mock_hf_dataset):
    """Fixture providing a mock Ray dataset that matches the mock HuggingFace dataset."""
    return ray.data.from_items(
        [
            {"text": text, "label": label}
            for text, label in zip(mock_hf_dataset["text"], mock_hf_dataset["label"])
        ]
    )


@pytest.fixture
def mock_successful_http_responses(mock_parquet_urls):
    """Fixture providing mock successful HTTP responses for URL resolution."""
    mock_responses = []
    for url in mock_parquet_urls:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.url = url
        mock_responses.append(mock_response)
    return mock_responses


@pytest.fixture
def mock_redirected_http_responses(mock_parquet_urls, mock_resolved_urls):
    """Fixture providing mock HTTP responses that simulate redirects."""
    mock_responses = []
    for original_url, resolved_url in zip(mock_parquet_urls, mock_resolved_urls):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.url = resolved_url
        mock_responses.append(mock_response)
    return mock_responses


@pytest.fixture
def mock_huggingface_datasource():
    """Fixture providing the HuggingFaceDatasource class for mocking."""
    from ray.data._internal.datasource.huggingface_datasource import (
        HuggingFaceDatasource,
    )

    return HuggingFaceDatasource


def verify_http_requests(mock_requests_head, expected_urls):
    """Verify that HTTP requests were made correctly."""
    assert mock_requests_head.call_count == len(expected_urls)

    for i, url in enumerate(expected_urls):
        call_args = mock_requests_head.call_args_list[i]
        assert call_args[0][0] == url
        assert call_args[1]["allow_redirects"] is True
        assert call_args[1]["timeout"] == 5


def verify_read_parquet_call(mock_read_parquet, expected_urls):
    """Verify that read_parquet was called with correct parameters."""
    mock_read_parquet.assert_called_once()
    call_args = mock_read_parquet.call_args

    # Check that the parquet URLs were passed
    assert call_args[0][0] == expected_urls

    # Check that the filesystem is HTTPFileSystem
    assert "filesystem" in call_args[1]
    assert "HTTPFileSystem" in str(type(call_args[1]["filesystem"]))

    # Check that retry_exceptions includes FileNotFoundError and ClientResponseError
    assert "ray_remote_args" in call_args[1]
    assert FileNotFoundError in call_args[1]["ray_remote_args"]["retry_exceptions"]


def verify_dataset_creation(ds, mock_hf_dataset):
    """Verify that the dataset was created successfully."""
    assert isinstance(ds, MaterializedDataset)
    assert ds.count() == mock_hf_dataset.num_rows


def setup_parquet_mocks(
    mock_huggingface_datasource,
    mock_parquet_urls,
    mock_http_responses,
    mock_ray_dataset,
):
    """Setup common mocking pattern for parquet-based tests."""
    patches = []

    # Mock the list_parquet_urls_from_dataset method
    datasource_patch = patch.object(
        mock_huggingface_datasource,
        "list_parquet_urls_from_dataset",
        return_value=mock_parquet_urls,
    )
    patches.append(datasource_patch)

    # Mock the requests.head calls
    requests_patch = patch("requests.head")
    patches.append(requests_patch)

    # Mock the read_parquet function
    read_parquet_patch = patch("ray.data.read_api.read_parquet")
    patches.append(read_parquet_patch)

    # Start all patches
    datasource_mock = datasource_patch.start()
    requests_mock = requests_patch.start()
    read_parquet_mock = read_parquet_patch.start()

    # Configure mocks
    requests_mock.side_effect = mock_http_responses
    read_parquet_mock.return_value = mock_ray_dataset

    return datasource_mock, requests_mock, read_parquet_mock, patches


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


def test_from_huggingface_with_parquet_files(
    mock_hf_dataset,
    ray_start_regular_shared,
    mock_parquet_urls,
    mock_ray_dataset,
    mock_successful_http_responses,
    mock_huggingface_datasource,
):
    """Test the distributed read path when parquet file URLs are available."""
    datasource_mock, requests_mock, read_parquet_mock, patches = setup_parquet_mocks(
        mock_huggingface_datasource,
        mock_parquet_urls,
        mock_successful_http_responses,
        mock_ray_dataset,
    )

    try:
        ds = ray.data.from_huggingface(mock_hf_dataset)

        # Verify HTTP requests
        verify_http_requests(requests_mock, mock_parquet_urls)

        # Verify read_parquet call
        verify_read_parquet_call(read_parquet_mock, mock_parquet_urls)

        # Verify dataset creation
        verify_dataset_creation(ds, mock_hf_dataset)

    finally:
        # Stop all patches
        for patch_obj in patches:
            patch_obj.stop()


def test_from_huggingface_with_resolved_urls(
    mock_hf_dataset,
    ray_start_regular_shared,
    mock_parquet_urls,
    mock_resolved_urls,
    mock_ray_dataset,
    mock_redirected_http_responses,
    mock_huggingface_datasource,
):
    """Test the URL resolution logic when HTTP redirects are encountered."""
    datasource_mock, requests_mock, read_parquet_mock, patches = setup_parquet_mocks(
        mock_huggingface_datasource,
        mock_parquet_urls,
        mock_redirected_http_responses,
        mock_ray_dataset,
    )

    try:
        ds = ray.data.from_huggingface(mock_hf_dataset)

        # Verify HTTP requests
        verify_http_requests(requests_mock, mock_parquet_urls)

        # Verify read_parquet call with resolved URLs
        verify_read_parquet_call(read_parquet_mock, mock_resolved_urls)

        # Verify dataset creation
        verify_dataset_creation(ds, mock_hf_dataset)

    finally:
        # Stop all patches
        for patch_obj in patches:
            patch_obj.stop()


def test_from_huggingface_url_resolution_failures(
    mock_hf_dataset,
    ray_start_regular_shared,
    mock_parquet_urls,
    mock_ray_dataset,
    mock_huggingface_datasource,
):
    """Test URL resolution failures fall back to single node read."""
    # Convert the mock dataset to an IterableDataset so it uses the read_datasource fallback
    mock_iterable_dataset = mock_hf_dataset.to_iterable_dataset()

    with patch.object(
        mock_huggingface_datasource,
        "list_parquet_urls_from_dataset",
        return_value=mock_parquet_urls,
    ):
        # Mock the requests.head calls to simulate failures
        with patch("requests.head") as mock_requests_head:
            # Configure mock to raise an exception for all URLs
            mock_requests_head.side_effect = requests.RequestException(
                "Connection failed"
            )

            # Mock the fallback path
            with patch("ray.data.read_api.read_datasource") as mock_read_datasource:
                mock_read_datasource.return_value = mock_ray_dataset

                ds = ray.data.from_huggingface(mock_iterable_dataset)

                # Verify that requests.head was called for each URL
                assert mock_requests_head.call_count == len(mock_parquet_urls)

                # Verify that the fallback read_datasource was called
                mock_read_datasource.assert_called_once()

                # Verify the dataset was created successfully via fallback
                verify_dataset_creation(ds, mock_hf_dataset)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
