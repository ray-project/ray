import datasets
import pyarrow
import pytest
from packaging.version import Version

import ray
from ray.data.dataset import Dataset
from ray.tests.conftest import *  # noqa


@pytest.fixture(scope="session")
def hf_dataset():
    return datasets.load_dataset("tweet_eval", "stance_climate")


def _arrow_sort_values(table: pyarrow.lib.Table) -> pyarrow.lib.Table:
    """
    Sort an Arrow table by the values in the first column. Used for testing
    compatibility with pyarrow 6 where `sort_by` does not exist. Inspired by:
    https://stackoverflow.com/questions/70893521/how-to-sort-a-pyarrow-table
    """
    by = [table.schema.names[0]]  # grab first col_name
    table_sorted_indexes = pyarrow.compute.bottom_k_unstable(
        table, sort_keys=by, k=len(table)
    )
    table_sorted = table.take(table_sorted_indexes)
    return table_sorted


def hfds_assert_equals(hfds: datasets.Dataset, ds: Dataset):
    hfds_table = _arrow_sort_values(hfds.data.table)
    ds_table = _arrow_sort_values(
        pyarrow.concat_tables([ray.get(tbl) for tbl in ds.to_arrow_refs()])
    )
    assert hfds_table.equals(ds_table)


@pytest.mark.parametrize("num_par", [1, 4])
def test_from_huggingface(hf_dataset, ray_start_regular_shared, num_par):
    # Check that DatasetDict is not directly supported.
    assert isinstance(hf_dataset, datasets.DatasetDict)
    with pytest.raises(
        DeprecationWarning,
        match="You provided a Hugging Face DatasetDict",
    ):
        ray.data.from_huggingface(hf_dataset)

    ray_datasets = {
        "train": ray.data.from_huggingface(hf_dataset["train"], parallelism=num_par),
    }

    assert isinstance(ray_datasets["train"], ray.data.Dataset)
    hfds_assert_equals(hf_dataset["train"], ray_datasets["train"])

    # Test reading in a split Hugging Face dataset yields correct individual datasets
    base_hf_dataset = hf_dataset["train"]
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
def test_from_huggingface_streaming(batch_format, ray_start_regular_shared):
    hfds = datasets.load_dataset(
        "tweet_eval", "stance_climate", streaming=True, split="train"
    ).with_format(batch_format)
    assert isinstance(hfds, datasets.IterableDataset)
    ds = ray.data.from_huggingface(hfds)
    assert ds.count() == 355


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
