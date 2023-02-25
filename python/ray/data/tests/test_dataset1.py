import itertools
import math
import os
import random
import signal
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.air.util.tensor_extensions.arrow import ArrowVariableShapedTensorType
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.stats import _StatsActor
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.pandas_block import PandasRow
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Std, Sum
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, _sliding_window
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorArray,
    TensorArray,
    TensorDtype,
)
from ray.data.row import TableRow
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


class SlowCSVDatasource(CSVDatasource):
    def _read_stream(self, f: "pa.NativeFile", path: str, **reader_args):
        for block in CSVDatasource._read_stream(self, f, path, **reader_args):
            time.sleep(3)
            yield block


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


class FlakyCSVDatasource(CSVDatasource):
    def __init__(self):
        self.counter = Counter.remote()

    def _read_stream(self, f: "pa.NativeFile", path: str, **reader_args):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            for block in CSVDatasource._read_stream(self, f, path, **reader_args):
                yield block

    def _write_block(self, f: "pa.NativeFile", block: BlockAccessor, **writer_args):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            CSVDatasource._write_block(self, f, block, **writer_args)


def test_dataset_retry_exceptions(ray_start_regular, local_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(local_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options={})
    # ds1 = ray.data.read_datasource(FlakyCSVDatasource(), parallelism=1, paths=path1)
    # ds1.write_datasource(FlakyCSVDatasource(), path=local_path, dataset_uuid="data")
    # assert df1.equals(
    #     pd.read_csv(os.path.join(local_path, "data_000000.csv"), storage_options={})
    # )

    counter = Counter.remote()

    def flaky_mapper(x):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            return ray.get(count)

    # assert sorted(ds1.map(flaky_mapper).take()) == [2, 3, 4]

    with pytest.raises(ValueError):
        ds = ray.data.read_datasource(
            FlakyCSVDatasource(),
            parallelism=1,
            paths=path1,
            ray_remote_args={"retry_exceptions": False},
        )
        print("XXXXX ds: plan:", ds._plan)
        for _ in ds.iter_batches():
            pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
