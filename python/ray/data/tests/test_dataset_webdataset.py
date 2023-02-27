import json
import os
import io

import numpy as np
from pandas.api.types import is_int64_dtype, is_float_dtype, is_object_dtype
import pytest
import tarfile
import glob

import ray

from ray.tests.conftest import *  # noqa
from ray.data import datasource
from ray.data.datasource import WebDatasetDatasource


def test_webdataset_read(ray_start_2_cpus, tmp_path):
    path = os.path.join(tmp_path, "bar_000000.tar")
    with open(path, "wb") as stream:
        tf = tarfile.open(fileobj=stream, mode="w")
        def write_file(name, data):
            f = tf.tarinfo()
            f.name = name
            f.size = len(data)
            tf.addfile(f, io.BytesIO(data))
        for i in range(100):
            write_file(f"{i}.a", str(i).encode("utf-8"))
            write_file(f"{i}.b", str(i**2).encode("utf-8"))
        tf.close()
    assert os.path.exists(path)
    assert len(glob.glob(f"{tmp_path}/*.tar")) == 1
    # ds = ray.data.read_datasource(WebDatasetDatasource(), paths=[str(tmp_path)], parallelism=1)
    ds = ray.data.read_webdataset(paths=[str(tmp_path)], parallelism=1)
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert sample["__key__"] == str(i)
        assert sample["a"].decode("utf-8") == str(i)
        assert sample["b"].decode("utf-8") == str(i**2)


def test_webdataset_write(ray_start_2_cpus, tmp_path):
    print(ray.available_resources())
    data = [dict(__key__=str(i), a=str(i), b=str(i**2)) for i in range(100)]
    ds = ray.data.from_items(data).repartition(1)
    # ds.write_datasource(WebDatasetDatasource(), path=tmp_path, try_create_dir=True, dataset_uuid="foo", overwrite=True, parallelism=1)
    ds.write_webdataset(path=tmp_path, try_create_dir=True)
    paths = glob.glob(f"{tmp_path}/*.tar")
    assert len(paths) == 1
    with open(paths[0], "rb") as stream:
        tf = tarfile.open(fileobj=stream)
        for i in range(100):
            assert tf.extractfile(f"{i}.a").read().decode("utf-8") == str(i)
            assert tf.extractfile(f"{i}.b").read().decode("utf-8") == str(i**2)

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
