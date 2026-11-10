# Copyright NVIDIA Corporation 2023
# SPDX-License-Identifier: Apache-2.0

import glob
import io
import os
import pickle
import tarfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import webdataset as wds

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
    ds = ray.data.read_webdataset(paths=[str(tmp_path)])
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert isinstance(sample, dict), sample
        assert sample["__key__"] == str(i)
        assert sample["a"].decode("utf-8") == str(i)
        assert sample["b"].decode("utf-8") == str(i**2)


@pytest.fixture
def allow_unsafe_deserialization(monkeypatch):
    monkeypatch.setenv("RAY_DATA_WEBDATASET_ALLOW_UNSAFE_DESERIALIZATION", "1")


@pytest.fixture(params=[False, True], ids=["v1", "v2"])
def use_datasource_v2(request, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", request.param)
    return request.param


def _as_read_output(df, use_datasource_v2):
    """Convert an expected frame to the dtypes ``to_pandas`` returns.

    V2 blocks are Arrow tables, so ``to_pandas`` returns Arrow-backed columns;
    V1 blocks are pandas frames with object columns.
    """
    if not use_datasource_v2:
        return df
    types = {"__key__": pa.string()}
    return df.astype({c: pd.ArrowDtype(types.get(c, pa.binary())) for c in df.columns})


def test_webdataset_expand_json(
    ray_start_2_cpus, tmp_path, allow_unsafe_deserialization
):
    import numpy as np
    import torch

    image = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
    gray = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    dstruct = dict(a=[1, 2], b=dict(c=2), d="hello")
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
    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], override_num_blocks=1, expand_json=True
    )
    record = ds.take(1)
    assert [1, 2] == record[0]["a"]


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
    ds = ray.data.read_webdataset(paths=[str(tmp_path)], suffixes=["txt", "cls"])
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "cls"}

    # test fnmatch patterns for suffixes
    ds = ray.data.read_webdataset(paths=[str(tmp_path)], suffixes=["*.txt", "*.cls"])
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "cls", "test.txt"}

    # test selection function
    def select(name):
        return name.endswith("txt")

    ds = ray.data.read_webdataset(paths=[str(tmp_path)], suffixes=select)
    samples = ds.take(100)
    assert len(samples) == 100
    for i, sample in enumerate(samples):
        assert set(sample.keys()) == {"__url__", "__key__", "txt", "test.txt"}

    # test filerename
    def renamer(name):
        result = name.replace("txt", "text")
        print("***", name, result)
        return result

    ds = ray.data.read_webdataset(paths=[str(tmp_path)], filerename=renamer)
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


def test_webdataset_non_contiguous_keys_raises(tmp_path):
    """Regression test for GH #44068.

    When a tar archive's entries are not sorted by sample key, `_group_by_keys`
    used to silently emit fragmented samples (one per contiguous run of a
    given key). We now detect that and surface a clear error pointing the user
    at the WebDataset sorted-archive requirement, so they don't silently lose
    data.
    """
    from ray.data._internal.datasource.webdataset_datasource import (
        _group_by_keys,
    )

    # Construct an iterator of file samples where key "001" appears in two
    # non-contiguous runs. This mirrors what `_tar_file_iterator` would yield
    # for a tar that wasn't sorted by name.
    data = [
        {"fname": "001.jpg", "data": b"jpg-001"},
        {"fname": "002.jpg", "data": b"jpg-002"},
        {"fname": "001.txt", "data": b"txt-001"},  # non-contiguous: bug case
    ]
    with pytest.raises(ValueError, match="not contiguous"):
        list(_group_by_keys(data, meta={"__url__": "test.tar"}))


def test_webdataset_contiguous_keys_no_false_positive(tmp_path):
    """Repeated suffixes within the same prefix run must not trip the
    non-contiguous detection (they're a separate, already-handled error)."""
    from ray.data._internal.datasource.webdataset_datasource import (
        _group_by_keys,
    )

    data = [
        {"fname": "001.jpg", "data": b"a"},
        {"fname": "001.txt", "data": b"b"},
        {"fname": "002.jpg", "data": b"c"},
        {"fname": "002.txt", "data": b"d"},
    ]
    samples = list(_group_by_keys(data, meta={"__url__": "test.tar"}))
    keys = sorted(s["__key__"] for s in samples)
    assert keys == ["001", "002"]


def test_webdataset_consumer_mutates_yielded_sample(tmp_path):
    """`_group_by_keys` must not assume the consumer leaves the yielded dict
    intact. If a downstream decoder mutates or clears it, the generator must
    still be able to bookkeep the seen-keys set when it resumes."""
    from ray.data._internal.datasource.webdataset_datasource import (
        _group_by_keys,
    )

    data = [
        {"fname": "001.jpg", "data": b"a"},
        {"fname": "002.jpg", "data": b"b"},
    ]
    gen = _group_by_keys(data, meta={"__url__": "test.tar"})
    first = next(gen)
    # Simulate a decoder that wipes the dict before the generator resumes.
    first.clear()
    second = next(gen)
    assert second["__key__"] == "002"
    with pytest.raises(StopIteration):
        next(gen)


def test_webdataset_write(ray_start_2_cpus, tmp_path):
    print(ray.available_resources())
    data = [dict(__key__=str(i), a=str(i), b=str(i**2)) for i in range(100)]
    ds = ray.data.from_items(data).repartition(1)

    def assert_archive(path, expected_a):
        paths = glob.glob(f"{path}/*.tar")
        assert len(paths) == 1
        with tarfile.open(paths[0]) as tf:
            for i in range(100):
                assert tf.extractfile(f"{i}.a").read().decode("utf-8") == expected_a(i)
                assert tf.extractfile(f"{i}.b").read().decode("utf-8") == str(i**2)

    default_path = tmp_path / "default"
    ds.write_webdataset(path=default_path, try_create_dir=True)
    assert_archive(default_path, lambda i: str(i))

    def encode_a(sample):
        sample = dict(sample)
        sample["a"] = f"encoded-{sample['a']}".encode("utf-8")
        return sample

    callable_path = tmp_path / "callable"
    ds.write_webdataset(path=callable_path, encoder=encode_a)
    assert_archive(callable_path, lambda i: f"encoded-{i}")

    def prefix_a(sample):
        sample = dict(sample)
        sample["a"] = f"prefixed-{sample['a']}"
        return sample

    chained_path = tmp_path / "chained"
    ds.write_webdataset(path=chained_path, encoder=[prefix_a, encode_a])
    assert_archive(chained_path, lambda i: f"encoded-prefixed-{i}")


def custom_decoder(sample):
    for key, value in sample.items():
        if key == "png":
            # check that images have already been decoded
            assert not isinstance(value, bytes)
        elif key.endswith("custom"):
            sample[key] = "custom-value"
    return sample


def test_webdataset_coding(
    ray_start_2_cpus, tmp_path, allow_unsafe_deserialization, use_datasource_v2
):
    import numpy as np
    import PIL.Image
    import torch

    image = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
    gray = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    dstruct = dict(a=[1], b=dict(c=2), d="hello")
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
    ds = ray.data.read_webdataset(paths=[str(tmp_path)])
    samples = ds.take(1)
    assert len(samples) == 1
    for sample in samples:
        assert isinstance(sample, dict), sample
        assert sample["__key__"] == "foo"
        assert isinstance(sample["jpg"], np.ndarray)
        assert sample["jpg"].shape == (100, 100, 3)
        assert isinstance(sample["gray.png"], np.ndarray)
        # V2 returns grayscale images with a channel axis, like color images.
        expected_gray_shape = (100, 100, 1) if use_datasource_v2 else (100, 100)
        assert sample["gray.png"].shape == expected_gray_shape
        assert isinstance(sample["mp"], dict)
        assert sample["mp"]["a"] == [1]
        assert sample["mp"]["b"]["c"] == 2
        assert isinstance(sample["json"], dict)
        assert sample["json"]["a"] == [1]
        assert isinstance(sample["pt"], np.ndarray)
        assert sample["pt"].tolist() == [1, 2, 3]

    # test the format argument to the default decoder and multiple decoders
    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)], decoder=["PIL", custom_decoder]
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


def test_webdataset_decoding(ray_start_2_cpus, tmp_path):
    import numpy as np
    import torch

    image = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
    gray = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    dstruct = dict(a=np.nan, b=dict(c=2), d="hello", e={"img_filename": "for_test.jpg"})
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

    ds = ray.data.read_webdataset(
        paths=[str(tmp_path)],
        override_num_blocks=1,
        decoder=None,
    )
    samples = ds.take(1)
    import json

    meta_json = json.loads(samples[0]["json"].decode("utf-8"))
    assert meta_json["e"]["img_filename"] == "for_test.jpg"


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_write_min_rows_per_file(tmp_path, ray_start_2_cpus, min_rows_per_file):
    ray.data.from_items(
        [{"id": str(i)} for i in range(100)], override_num_blocks=20
    ).write_webdataset(tmp_path, min_rows_per_file=min_rows_per_file)

    for filename in os.listdir(tmp_path):
        dataset = wds.WebDataset(os.path.join(tmp_path, filename))
        assert len(list(dataset)) == min_rows_per_file


@pytest.mark.parametrize(
    "filename",
    ["000000.pkl", "000000.pickle", "000000.pt", "000000.pth"],
)
def test_default_decoder_rejects_unsafe_extensions(
    ray_start_2_cpus, tmp_path, filename, use_datasource_v2
):
    path = os.path.join(tmp_path, "unsafe.tar")
    with TarWriter(path) as tf:
        tf.write(filename, b"fake-payload")

    ds = ray.data.read_webdataset(paths=[str(tmp_path)])
    with pytest.raises(Exception, match="Refusing to"):
        ds.take_all()


def test_default_decoder_allows_unsafe_with_env_var(
    ray_start_2_cpus, tmp_path, allow_unsafe_deserialization
):
    path = os.path.join(tmp_path, "trusted.tar")
    with TarWriter(path) as tf:
        tf.write("000000.pkl", pickle.dumps({"key": "value"}))

    ds = ray.data.read_webdataset(paths=[str(tmp_path)])
    rows = ds.take_all()

    assert len(rows) == 1
    assert rows[0]["pkl"] == {"key": "value"}


def test_custom_decoder_bypasses_unsafe_guard(ray_start_2_cpus, tmp_path):
    path = os.path.join(tmp_path, "custom.tar")
    with TarWriter(path) as tf:
        tf.write("000000.pkl", pickle.dumps({"key": "value"}))

    def safe_pkl_decoder(sample):
        sample = dict(sample)
        for key, value in sample.items():
            if key == "pkl":
                sample[key] = pickle.loads(value)
        return sample

    ds = ray.data.read_webdataset(paths=[str(tmp_path)], decoder=safe_pkl_decoder)
    rows = ds.take_all()

    assert len(rows) == 1
    assert rows[0]["pkl"] == {"key": "value"}


@pytest.mark.parametrize("num_samples", [1, 511, 512, 513, 1000])
def test_read_webdataset_chunked_samples(
    ray_start_2_cpus, tmp_path, num_samples, use_datasource_v2
):
    path = os.path.join(tmp_path, "data-000000.tar")
    with wds.TarWriter(path) as writer:
        for i in range(num_samples):
            writer.write({"__key__": f"{i:06d}", "cls": str(i).encode("utf-8")})

    actual = (
        ray.data.read_webdataset([path], suffixes=["cls"], decoder=None)
        .to_pandas()
        .sort_values("__key__")
        .reset_index(drop=True)
    )
    expected = pd.DataFrame(
        {
            "__key__": [f"{i:06d}" for i in range(num_samples)],
            "cls": [str(i).encode("utf-8") for i in range(num_samples)],
        }
    )
    pd.testing.assert_frame_equal(
        actual[["__key__", "cls"]], _as_read_output(expected, use_datasource_v2)
    )


def test_read_webdataset_chunked_heterogeneous_keys(
    ray_start_2_cpus, tmp_path, use_datasource_v2
):
    path = os.path.join(tmp_path, "data-000000.tar")
    n = 6
    with wds.TarWriter(path) as writer:
        for i in range(n):
            sample = {"__key__": f"{i:06d}", "cls": str(i).encode("utf-8")}
            if i % 2 == 0:
                sample["aux"] = str(i * 10).encode("utf-8")
            writer.write(sample)

    actual = (
        ray.data.read_webdataset([path], suffixes=["cls", "aux"], decoder=None)
        .to_pandas()
        .sort_values("__key__")
        .reset_index(drop=True)
    )
    expected = pd.DataFrame(
        {
            "__key__": [f"{i:06d}" for i in range(n)],
            "cls": [str(i).encode("utf-8") for i in range(n)],
            "aux": [
                str(i * 10).encode("utf-8") if i % 2 == 0 else np.nan for i in range(n)
            ],
        }
    )
    pd.testing.assert_frame_equal(
        actual[["__key__", "cls", "aux"]], _as_read_output(expected, use_datasource_v2)
    )


def test_read_webdataset_forwards_ray_remote_args(ray_start_2_cpus, tmp_path):
    path = os.path.join(tmp_path, "data-000000.tar")
    with wds.TarWriter(path) as writer:
        writer.write({"__key__": "000000", "cls": b"0"})

    ds = ray.data.read_webdataset(
        [path],
        suffixes=["cls"],
        decoder=None,
        num_cpus=1,
        memory=512,
        ray_remote_args={"max_retries": 5},
    )

    read_op = ds._logical_plan.dag
    assert read_op.ray_remote_args["num_cpus"] == 1
    assert read_op.ray_remote_args["memory"] == 512
    assert read_op.ray_remote_args["max_retries"] == 5

    # smoke run
    assert len(ds.take_all()) == 1


def test_default_decoder_rejects_disallowed_image_format():
    # EPS bytes in a member named `.jpg`. PIL picks a plugin by file signature,
    # not extension, and the EPS plugin shells out to Ghostscript. The default
    # decoder must refuse formats outside its raster allowlist rather than
    # dispatch untrusted bytes to such a parser.
    from PIL import UnidentifiedImageError

    from ray.data._internal.datasource.webdataset_datasource import _default_decoder

    eps_bytes = (
        b"%!PS-Adobe-3.0 EPSF-3.0\n" b"%%BoundingBox: 0 0 100 100\n" b"showpage\n"
    )
    with pytest.raises(UnidentifiedImageError):
        _default_decoder({"__key__": "0001", "jpg": eps_bytes})


def test_default_decoder_allows_png(tmp_path):
    from PIL import Image

    from ray.data._internal.datasource.webdataset_datasource import _default_decoder

    buf = io.BytesIO()
    Image.new("RGB", (4, 4), (1, 2, 3)).save(buf, "PNG")
    out = _default_decoder({"__key__": "0001", "png": buf.getvalue()})
    assert out["png"].shape == (4, 4, 3)


def _png_bytes(image: np.ndarray) -> bytes:
    from PIL import Image

    buf = io.BytesIO()
    Image.fromarray(image).save(buf, "PNG")
    return buf.getvalue()


def _write_png_shard(path, images):
    with TarWriter(path) as writer:
        for key, image in images.items():
            writer.write(f"{key}.png", _png_bytes(image))


@pytest.mark.parametrize(
    "expand_grayscale, expected_gray_shape", [(False, (3, 4)), (True, (3, 4, 1))]
)
def test_default_decoder_expand_grayscale(expand_grayscale, expected_gray_shape):
    from ray.data._internal.datasource_v2.formats.webdataset.webdataset_utils import (
        _default_decoder,
    )

    gray = np.arange(12, dtype=np.uint8).reshape(3, 4)
    color = np.zeros((3, 4, 3), dtype=np.uint8)
    out = _default_decoder(
        {"__key__": "0", "a.png": _png_bytes(gray), "b.png": _png_bytes(color)},
        expand_grayscale=expand_grayscale,
    )
    assert out["a.png"].shape == expected_gray_shape
    np.testing.assert_array_equal(out["a.png"].reshape(3, 4), gray)
    assert out["b.png"].shape == (3, 4, 3)


def test_read_webdataset_v2_grayscale_and_color_images(
    ray_start_2_cpus, tmp_path, monkeypatch
):
    # Each chunk of samples becomes its own Arrow table. The first chunk holds
    # only color images and the second mixes in grayscale ones, so the chunks
    # concatenate only if grayscale images get the same ndim as color images.
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    rng = np.random.default_rng(0)
    images = {}
    for i in range(600):
        shape = (4 + i % 3, 5 + i % 2)
        if i < 512 or i % 2:
            shape += (3,)
        images[f"{i:06d}"] = rng.integers(0, 256, shape, dtype=np.uint8)
    path = os.path.join(tmp_path, "data-000000.tar")
    _write_png_shard(path, images)

    rows = ray.data.read_webdataset([path]).take_all()

    assert len(rows) == len(images)
    for row in rows:
        expected = images[row["__key__"]]
        if expected.ndim == 2:
            expected = expected[:, :, np.newaxis]
        np.testing.assert_array_equal(row["png"], expected)


def test_read_webdataset_v2_mixed_image_dtypes(ray_start_2_cpus, tmp_path, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    images = {
        f"{i:06d}": np.zeros((4, 5), dtype=np.uint8 if i < 512 else np.uint16)
        for i in range(600)
    }
    path = os.path.join(tmp_path, "data-000000.tar")
    _write_png_shard(path, images)

    # Known limitation: tensor columns whose dtypes differ between chunks can't
    # be concatenated.
    with pytest.raises(Exception, match="divergent scalar types"):
        ray.data.read_webdataset([path]).take_all()


def test_read_webdataset_v2_npy_mixed_ndim(ray_start_2_cpus, tmp_path, monkeypatch):
    # Known limitation: grayscale expansion only applies to images. The first
    # chunk holds only 2D arrays and the second mixes 2D and 3D ones, so the
    # second chunk falls back to objects and can't be concatenated.
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    path = os.path.join(tmp_path, "data-000000.tar")
    with TarWriter(path) as writer:
        for i in range(600):
            buf = io.BytesIO()
            np.save(buf, np.zeros((4, 5) if i < 512 or i % 2 else (4, 5, 1)))
            writer.write(f"{i:06d}.npy", buf.getvalue())

    with pytest.raises(Exception, match="both objects and tensors"):
        ray.data.read_webdataset([path]).take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
