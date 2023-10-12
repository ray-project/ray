import pandas as pd
import pytest

import ray
from ray.data.datasource.filename_provider import DefaultFilenameProvider


@pytest.fixture(params=["csv", None])
def filename_provider(request):
    yield DefaultFilenameProvider(dataset_uuid="", file_format=request.param)


def test_default_filename_for_row_is_idempotent(filename_provider):
    row = {}

    first_filename = filename_provider.get_filename_for_row(row, file_index=0)
    second_filename = filename_provider.get_filename_for_row(row, file_index=0)

    assert first_filename == second_filename


def test_default_filename_for_block_is_idempotent(filename_provider):
    block = pd.DataFrame()

    first_filename = filename_provider.get_filename_for_block(block, file_index=0)
    second_filename = filename_provider.get_filename_for_block(block, file_index=0)

    assert first_filename == second_filename


def test_default_filename_for_row_is_unique(filename_provider):
    filenames = [
        filename_provider.get_filename_for_row({}, file_index=file_index)
        for file_index in range(100)
    ]
    assert len(set(filenames)) == len(filenames)


def test_default_filename_for_block_is_unique(filename_provider):
    filenames = [
        filename_provider.get_filename_for_block(pd.DataFrame(), file_index=file_index)
        for file_index in range(100)
    ]
    assert len(set(filenames)) == len(filenames)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
