# Copyright NVIDIA Corporation 2023
# SPDX-License-Identifier: Apache-2.0

import os
import io

import pytest
import tarfile
import glob

import ray

from ray.tests.conftest import *  # noqa


class TarWriter:
    def __init__(self, path):
        self.path = path
        self.tar = tarfile.open(path, "w")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.tar.close()

    def write(self, name, data):
        f = self.tar.tarinfo()
        f.name = name
        f.size = len(data)
        self.tar.addfile(f, io.BytesIO(data))


def test_webdataset_read(ray_start_2_cpus, tmp_path):
    path = os.path.join(tmp_path, "bar_000000.tar")
    with TarWriter(path) as tf:
        for i in range(100):
            tf.write(f"{i}.a", str(i).encode("utf-8"))
            tf.write(f"{i}.b", str(i**2).encode("utf-8"))
    assert os.path.exists(path)
    assert len(glob.glob(f"{tmp_path}/*.tar")) == 1
    ds = ray.data.read_webdataset(paths=[str(tmp_path)], parallelism=1)
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert isinstance(sample, dict), sample
        assert sample["__key__"] == str(i)
        assert sample["a"].decode("utf-8") == str(i)
        assert sample["b"].decode("utf-8") == str(i**2)


def test_webdataset_suffixes(ray_start_2_cpus, tmp_path):
    path = os.path.join(tmp_path, "bar_000000.tar")
    with TarWriter(path) as tf:
        for i in range(100):
            tf.write(f"{i}.txt", str(i).encode("utf-8"))
            tf.write(f"{i}.test.txt", str(i**2).encode("utf-8"))
            tf.write(f"{i}.cls", str(i**2).encode("utf-8"))
            tf.write(f"{i}.test.cls2", str(i**2).encode("utf-8"))
    assert os.path.exists(path)
    assert len(glob.glob(f"{tmp_path}/*.tar")) == 1

    # test simple suffixes
    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], parallelism=1, suffixes=["txt", "cls"]
    )
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "cls"}

    # test fnmatch patterns for suffixes
    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], parallelism=1, suffixes=["*.txt", "*.cls"]
    )
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "cls", "test.txt"}

    # test selection function
    def select(name):
        return name.endswith("txt")

    ds = ray.data.read_webdataset(paths=[str(tmp_path)], parallelism=1, suffixes=select)
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "test.txt"}

    # test filerename
    def renamer(name):
        result = name.replace("txt", "text")
        print("***", name, result)
        return result

    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], parallelism=1, filerename=renamer
    )
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {
            "__url__",
            "__key__",
            "text",
            "cls",
            "test.text",
            "test.cls2",
        }


def test_webdataset_write(ray_start_2_cpus, tmp_path):
    print(ray.available_resources())
    data = [dict(__key__=str(i), a=str(i), b=str(i**2)) for i in range(100)]
    ds = ray.data.from_items(data).repartition(1)
    ds.write_webdataset(path=tmp_path, try_create_dir=True)
    paths = glob.glob(f"{tmp_path}/*.tar")
    assert len(paths) == 1
    with open(paths[0], "rb") as stream:
        tf = tarfile.open(fileobj=stream)
        for i in range(100):
            assert tf.extractfile(f"{i}.a").read().decode("utf-8") == str(i)
            assert tf.extractfile(f"{i}.b").read().decode("utf-8") == str(i**2)


def custom_decoder(sample):
    for key, value in sample.items():
        if key == "png":
            # check that images have already been decoded
            assert not isinstance(value, bytes)
        elif key.endswith("custom"):
            sample[key] = "custom-value"
    return sample


def test_webdataset_coding(ray_start_2_cpus, tmp_path):
    import numpy as np
    import torch
    import PIL.Image

    image = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
    gray = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    dstruct = dict(a=[1], b=dict(c=2), d="hello")
    # Note: tensors are supported as numpy format only in strict mode.
    ttensor = torch.tensor([1, 2, 3]).numpy()

    sample = {
        "__key__": "foo",
        "jpg": image,
        "gray.png": gray,
        "mp": dstruct,
        "json": dstruct,
        "pt": ttensor,
        "und": b"undecoded",
        "custom": b"nothing",
    }

    # write the encoded data using the default encoder
    data = [sample]
    ds = ray.data.from_items(data).repartition(1)
    ds.write_webdataset(path=tmp_path, try_create_dir=True)

    # read the encoded data using the default decoder
    paths = glob.glob(f"{tmp_path}/*.tar")
    assert len(paths) == 1
    path = paths[0]
    assert os.path.exists(path)
    ds = ray.data.read_webdataset(paths=[str(tmp_path)], parallelism=1)
    samples = ds.take(1)
    assert len(samples) == 1
    for sample in samples:
        assert isinstance(sample, dict), sample
        assert sample["__key__"] == "foo"
        assert isinstance(sample["jpg"], np.ndarray)
        assert sample["jpg"].shape == (100, 100, 3)
        assert isinstance(sample["gray.png"], np.ndarray)
        assert sample["gray.png"].shape == (100, 100)
        assert isinstance(sample["mp"], dict)
        assert sample["mp"]["a"] == [1]
        assert sample["mp"]["b"]["c"] == 2
        assert isinstance(sample["json"], dict)
        assert sample["json"]["a"] == [1]
        assert isinstance(sample["pt"], np.ndarray)
        assert sample["pt"].tolist() == [1, 2, 3]

    # test the format argument to the default decoder and multiple decoders
    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], parallelism=1, decoder=["PIL", custom_decoder]
    )
    samples = ds.take(1)
    assert len(samples) == 1
    for sample in samples:
        assert isinstance(sample, dict), sample
        assert sample["__key__"] == "foo"
        assert isinstance(sample["jpg"], PIL.Image.Image)
        assert isinstance(sample["gray.png"], PIL.Image.Image)
        assert isinstance(sample["und"], bytes)
        assert sample["und"] == b"undecoded"
        assert sample["custom"] == "custom-value"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
