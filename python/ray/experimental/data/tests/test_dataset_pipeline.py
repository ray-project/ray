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
    assert pipe.count() == 10
    pipe = ds.repeat(10)
    assert pipe.count() == 100
