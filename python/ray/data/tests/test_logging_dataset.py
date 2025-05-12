import logging
import os
import time

import pytest

import ray
from ray.data._internal.logging import get_log_directory, reset_logging
from ray.tests.conftest import *  # noqa


@pytest.fixture(name="reset_logging")
def reset_logging_fixture():
    yield
    reset_logging()


def _test_harness(filename_01: str, filename_02: str):
    """Test harness to check if the log files are created and contain the expected content."""
    dataset_01_log_file = None
    dataset_02_log_file = None
    directory = get_log_directory()
    for filename in os.listdir(directory):
        if filename.startswith(f"ray-data-{filename_01}") and filename.endswith(".log"):
            dataset_01_log_file = os.path.join(directory, filename)
        if filename.startswith(f"ray-data-{filename_02}") and filename.endswith(".log"):
            dataset_02_log_file = os.path.join(directory, filename)
        if dataset_01_log_file and dataset_02_log_file:
            break

    assert dataset_01_log_file is not None
    assert dataset_02_log_file is not None
    with open(dataset_01_log_file, "r") as f:
        log_01_contents = f.read()
    with open(dataset_02_log_file, "r") as f:
        log_02_contents = f.read()
    dataset_01_id = (
        os.path.basename(dataset_01_log_file)
        .removeprefix("ray-data-")
        .removesuffix(".log")
    )
    dataset_02_id = (
        os.path.basename(dataset_02_log_file)
        .removeprefix("ray-data-")
        .removesuffix(".log")
    )

    assert dataset_01_id in log_01_contents
    assert dataset_02_id in log_02_contents
    assert (
        f"Starting execution of Dataset {dataset_01_id}"
        in log_01_contents + log_02_contents
    )
    assert (
        f"Dataset {dataset_02_id} execution finished"
        in log_01_contents + log_02_contents
    )
    return log_01_contents, log_02_contents


def test_dataset_logging_concurrent(ray_start_regular_shared, reset_logging):
    from concurrent.futures import ThreadPoolExecutor

    def _short(x):
        logger = logging.getLogger("ray.data")
        logger.info("short function is running")
        time.sleep(0.1)
        return x

    def _long(x):
        logger = logging.getLogger("ray.data")
        logger.info("long function is running")
        time.sleep(1)
        return x

    ds01 = ray.data.range(1).map_batches(_short)
    ds01.set_name("test_dataset_logging_concurrent_01")

    ds02 = ray.data.range(1).map_batches(_long)
    ds02.set_name("test_dataset_logging_concurrent_02")

    with ThreadPoolExecutor() as executor:
        executor.submit(ds01.materialize)
        executor.submit(ds02.materialize)

    log_01_contents, log_02_contents = _test_harness(
        "test_dataset_logging_concurrent_01",
        "test_dataset_logging_concurrent_02",
    )
    # for concurrent datasets, which dataset contains the worker logs is not
    # deterministic, so we only check that the logs are present in the combined logs
    assert "short function is running" in log_01_contents + log_02_contents
    assert "long function is running" in log_01_contents + log_02_contents


def test_dataset_logging_sequential(ray_start_regular_shared, reset_logging):
    def _short(x):
        logger = logging.getLogger("ray.data")
        logger.info("short function is running")
        time.sleep(0.1)
        return x

    def _long(x):
        logger = logging.getLogger("ray.data")
        logger.info("long function is running")
        time.sleep(1)
        return x

    ds01 = ray.data.range(1).map_batches(_short)
    ds01.set_name("test_dataset_logging_sequential_01")
    ds01.materialize()

    ds02 = ray.data.range(1).map_batches(_long)
    ds02.set_name("test_dataset_logging_sequential_02")
    ds02.materialize()

    log_01_contents, log_02_contents = _test_harness(
        "test_dataset_logging_sequential_01",
        "test_dataset_logging_sequential_02",
    )
    assert "short function is running" in log_01_contents
    assert "long function is running" in log_02_contents


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
