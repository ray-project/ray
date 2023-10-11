import os
import random
import tempfile
from timeit import default_timer as timer
from typing import List

import pytest

from ray.anyscale.data import AnyscaleFileMetadataProvider
from ray.data.datasource import DefaultFileMetadataProvider
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem


@pytest.fixture(scope="module")
def synthetic_dataset_path():
    with tempfile.TemporaryDirectory() as tmp_path:
        for num_files in (5, 50, 500, 5000, 500_000):
            directory = os.path.join(tmp_path, str(num_files))
            os.makedirs(directory)
            for i in range(num_files):
                file_name = f"{i}.dat"
                with open(os.path.join(directory, file_name), "wb") as file:
                    file_size_bytes = random.randint(3, 6)
                    file.write(b"\0" * file_size_bytes)

        with open(os.path.join(tmp_path, "empty.dat"), "w"):
            pass

        yield tmp_path


def test_no_file_extensions(synthetic_dataset_path):
    input_path = os.path.join(synthetic_dataset_path, "5")
    input_paths, filesystem = _resolve_paths_and_filesystem(input_path)

    default_provider = DefaultFileMetadataProvider()
    expected_paths, expected_file_sizes = list(
        zip(*default_provider.expand_paths(input_paths, filesystem))
    )

    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=None)
    actual_paths, actual_file_sizes = list(
        zip(*anyscale_provider.expand_paths(input_paths, filesystem))
    )

    assert set(expected_paths) == set(actual_paths)

    expected_total_size = sum(expected_file_sizes)
    actual_total_size = sum(actual_file_sizes)
    percent_error = abs(expected_total_size - actual_total_size) / expected_total_size
    assert percent_error < 0.2


@pytest.mark.parametrize(
    "relative_paths",
    [
        ["5/"],
        ["50/"],
        ["500/"],
        ["5000/"],
        ["500/", "empty.dat"],
        ["50/", "500/"],
    ],
)
@pytest.mark.parametrize("expand_inputs", [False, True])
def test_metadata(
    synthetic_dataset_path, relative_paths: List[str], expand_inputs: bool
):
    input_paths = [
        os.path.join(synthetic_dataset_path, path) for path in relative_paths
    ]
    input_paths, filesystem = _resolve_paths_and_filesystem(input_paths)

    default_provider = DefaultFileMetadataProvider()
    expected_paths, expected_file_sizes = list(
        zip(*default_provider.expand_paths(input_paths, filesystem))
    )

    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=["dat"])
    if expand_inputs:
        input_paths = expected_paths
    actual_paths, actual_file_sizes = list(
        zip(*anyscale_provider.expand_paths(input_paths, filesystem))
    )

    assert set(expected_paths) == set(actual_paths)

    expected_total_size = sum(expected_file_sizes)
    actual_total_size = sum(actual_file_sizes)
    percent_error = abs(expected_total_size - actual_total_size) / expected_total_size
    assert percent_error < 0.2


def test_missing_path_raises_error():
    input_paths, filesystem = _resolve_paths_and_filesystem("nonexistant.dat")
    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=["dat"])
    with pytest.raises(FileNotFoundError):
        list(anyscale_provider.expand_paths(input_paths, filesystem))


def test_ignore_missing_paths():
    input_paths, filesystem = _resolve_paths_and_filesystem("nonexistant.dat")
    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=["dat"])
    metadata = list(
        anyscale_provider.expand_paths(
            input_paths, filesystem, ignore_missing_paths=True
        )
    )
    assert not metadata


def test_sampling_performance(synthetic_dataset_path):
    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=["dat"])
    input_paths = [
        os.path.join(synthetic_dataset_path, "500000", f"{i}.dat")
        for i in range(500000)
    ]
    input_paths, filesystem = _resolve_paths_and_filesystem(input_paths)

    start_time = timer()
    list(anyscale_provider.expand_paths(input_paths, filesystem))
    total_time = timer() - start_time

    assert total_time < 10


def test_stable_ordering(synthetic_dataset_path):
    anyscale_provider = AnyscaleFileMetadataProvider(file_extensions=["dat"])
    input_paths = [
        os.path.join(synthetic_dataset_path, "5000", f"{i}.dat") for i in range(5000)
    ]
    input_paths, filesystem = _resolve_paths_and_filesystem(input_paths)

    expanded_paths, _ = list(
        zip(*anyscale_provider.expand_paths(input_paths, filesystem))
    )

    assert input_paths == list(expanded_paths)
