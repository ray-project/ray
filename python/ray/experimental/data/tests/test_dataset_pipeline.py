import os
import random
import requests
import shutil
import time

from unittest.mock import patch
import math
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray

from ray.tests.conftest import *  # noqa
from ray.experimental.data.datasource import DummyOutputDatasource
from ray.experimental.data.block import BlockAccessor
import ray.experimental.data.tests.util as util


def test_basic_pipeline(ray_start_regular_shared):
    ds = ray.experimental.data.range(10)

    pipe = ds.pipeline(1)
    for _ in range(2):
        assert pipe.count() == 10

    pipe = ds.pipeline(1)
    assert pipe.take() == list(range(10))

    pipe = ds.pipeline(999)
    assert pipe.count() == 10

    pipe = ds.repeat(10)
    for _ in range(2):
        assert pipe.count() == 100

    pipe = ds.repeat(10)
    assert pipe.sum() == 450


def test_iter_batches(ray_start_regular_shared):
    pipe = ray.experimental.data.range(10).pipeline(2)
    batches = list(pipe.iter_batches())
    assert len(batches) == 10
    assert all(len(e) == 1 for e in batches)


def test_iter_datasets(ray_start_regular_shared):
    pipe = ray.experimental.data.range(10).pipeline(2)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 5

    pipe = ray.experimental.data.range(10).pipeline(5)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 2


def test_foreach_dataset(ray_start_regular_shared):
    pipe = ray.experimental.data.range(5).pipeline(2)
    pipe = pipe.foreach_dataset(lambda ds: ds.map(lambda x: x * 2))
    assert pipe.take() == [0, 2, 4, 6, 8]


def test_schema(ray_start_regular_shared):
    pipe = ray.experimental.data.range(5).pipeline(2)
    assert pipe.schema() == int
