import os
import tempfile
from typing import Dict

import numpy as np
import pyarrow as pa
import pytest
from fsspec.implementations.local import LocalFileSystem
from PIL import Image

import ray
from ray.data._internal.datasource.image_datasource import (
    ImageDatasource,
    ImageFileMetadataProvider,
)
from ray.data.datasource import Partitioning
from ray.data.datasource.file_meta_provider import FastFileMetadataProvider
from ray.data.extensions import ArrowTensorType
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


class TestReadImages:
    def test_basic(self, ray_start_regular_shared):
        # "simple" contains three 32x32 RGB images.
        ds = ray.data.read_images("example://image-datasets/simple")
        assert ds.schema().names == ["image"]
        column_type = ds.schema().types[0]
        assert isinstance(column_type, ArrowTensorType)
        assert all(record["image"].shape == (32, 32, 3) for record in ds.take())

    @pytest.mark.parametrize("num_threads", [-1, 0, 1, 2, 4])
    def test_multi_threading(self, ray_start_regular_shared, num_threads, monkeypatch):
        monkeypatch.setattr(
            ray.data._internal.datasource.image_datasource.ImageDatasource,
            "_NUM_THREADS_PER_TASK",
            num_threads,
        )
        ds = ray.data.read_images(
            "example://image-datasets/simple",
            override_num_blocks=1,
            include_paths=True,
        )
        paths = [item["path"][-len("image1.jpg") :] for item in ds.take_all()]
        if num_threads > 1:
            # If there are more than 1 threads, the order is not guaranteed.
            paths = sorted(paths)
        expected_paths = ["image1.jpg", "image2.jpg", "image3.jpg"]
        assert paths == expected_paths

    def test_multiple_paths(self, ray_start_regular_shared):
        ds = ray.data.read_images(
            paths=[
                "example://image-datasets/simple/image1.jpg",
                "example://image-datasets/simple/image2.jpg",
            ]
        )
        assert ds.count() == 2

    def test_file_metadata_provider(self, ray_start_regular_shared):
        ds = ray.data.read_images(
            paths=[
                "example://image-datasets/simple/image1.jpg",
                "example://image-datasets/simple/image2.jpg",
                "example://image-datasets/simple/image2.jpg",
            ],
            meta_provider=FastFileMetadataProvider(),
        )
        assert ds.count() == 3

    @pytest.mark.parametrize("ignore_missing_paths", [True, False])
    def test_ignore_missing_paths(self, ray_start_regular_shared, ignore_missing_paths):
        paths = [
            "example://image-datasets/simple/image1.jpg",
            "example://missing.jpg",
            "example://image-datasets/missing/",
        ]

        if ignore_missing_paths:
            ds = ray.data.read_images(paths, ignore_missing_paths=ignore_missing_paths)
            # example:// directive redirects to /ray/python/ray/data/examples/data
            assert len(ds.input_files()) == 1 and ds.input_files()[0].endswith(
                "ray/data/examples/data/image-datasets/simple/image1.jpg",
            )
        else:
            with pytest.raises(FileNotFoundError):
                ds = ray.data.read_images(
                    paths, ignore_missing_paths=ignore_missing_paths
                )
                ds.materialize()

    def test_filtering(self, ray_start_regular_shared):
        # "different-extensions" contains three images and two non-images.
        ds = ray.data.read_images("example://image-datasets/different-extensions")
        assert ds.count() == 3

    def test_size(self, ray_start_regular_shared):
        # "different-sizes" contains RGB images with different heights and widths.
        ds = ray.data.read_images(
            "example://image-datasets/different-sizes", size=(32, 32)
        )
        assert all(record["image"].shape == (32, 32, 3) for record in ds.take())

    def test_different_sizes(self, ray_start_regular_shared):
        ds = ray.data.read_images("example://image-datasets/different-sizes")
        assert sorted(record["image"].shape for record in ds.take()) == [
            (16, 16, 3),
            (32, 32, 3),
            (64, 64, 3),
        ]

    @pytest.mark.parametrize("size", [(-32, 32), (32, -32), (-32, -32)])
    def test_invalid_size(self, ray_start_regular_shared, size):
        with pytest.raises(ValueError):
            ray.data.read_images("example://image-datasets/simple", size=size)

    @pytest.mark.parametrize(
        "mode, expected_shape", [("L", (32, 32)), ("RGB", (32, 32, 3))]
    )
    def test_mode(
        self,
        mode,
        expected_shape,
        ray_start_regular_shared,
    ):
        # "different-modes" contains 32x32 images with modes "CMYK", "L", and "RGB"
        ds = ray.data.read_images("example://image-datasets/different-modes", mode=mode)
        assert all([record["image"].shape == expected_shape for record in ds.take()])

    def test_partitioning(
        self, ray_start_regular_shared, enable_automatic_tensor_extension_cast
    ):
        root = "example://image-datasets/dir-partitioned"
        partitioning = Partitioning("dir", base_dir=root, field_names=["label"])

        ds = ray.data.read_images(root, partitioning=partitioning)

        assert ds.schema().names == ["image", "label"]

        image_type, label_type = ds.schema().types
        assert isinstance(image_type, ArrowTensorType)
        assert pa.types.is_string(label_type)

        df = ds.to_pandas()
        assert sorted(df["label"]) == ["cat", "cat", "dog"]
        if enable_automatic_tensor_extension_cast:
            assert all(tensor.shape == (32, 32, 3) for tensor in df["image"])
        else:
            assert all(tensor.numpy_shape == (32, 32, 3) for tensor in df["image"])

    def test_random_shuffle(self, ray_start_regular_shared, restore_data_context):
        # NOTE: set preserve_order to True to allow consistent output behavior.
        context = ray.data.DataContext.get_current()
        context.execution_options.preserve_order = True

        dir_path = "s3://anonymous@air-example-data/mnist"
        file_paths = [f"{i:05d}.png" for i in range(10)]
        input_uris = [f"{dir_path}/{file_path}" for file_path in file_paths]

        ds = ray.data.read_images(
            paths=input_uris,
            include_paths=True,
            shuffle="files",
        )

        # Execute 10 times to get a set of output paths.
        output_paths_list = []
        for _ in range(10):
            paths = [row["path"][-len(file_paths[0]) :] for row in ds.take_all()]
            output_paths_list.append(paths)
        all_paths_matched = [
            file_paths == output_paths for output_paths in output_paths_list
        ]

        # Check when shuffle is enabled, output order has at least one different
        # case.
        assert not all(all_paths_matched)
        # Check all files are output properly without missing one.
        assert all(
            [file_paths == sorted(output_paths) for output_paths in output_paths_list]
        )

    def test_e2e_prediction(self, shutdown_only):
        import torch
        from torchvision import transforms
        from torchvision.models import resnet18

        ray.shutdown()
        ray.init(num_cpus=2)

        dataset = ray.data.read_images("example://image-datasets/simple")
        transform = transforms.ToTensor()

        def preprocess(batch: Dict[str, np.ndarray]):
            return {"out": np.stack([transform(image) for image in batch["image"]])}

        dataset = dataset.map_batches(preprocess, batch_format="numpy")

        class Predictor:
            def __init__(self):
                self.model = resnet18(pretrained=True)

            def __call__(self, batch: Dict[str, np.ndarray]):
                with torch.inference_mode():
                    torch_tensor = torch.as_tensor(batch["out"])
                    return {"prediction": self.model(torch_tensor)}

        predictions = dataset.map_batches(
            Predictor, compute=ray.data.ActorPoolStrategy(min_size=1), batch_size=4096
        )

        for _ in predictions.iter_batches():
            pass

    @pytest.mark.parametrize(
        "image_size,image_mode,expected_size,expected_ratio",
        [(64, "RGB", 30000, 4), (32, "L", 3500, 0.5), (256, "RGBA", 750000, 85)],
    )
    def test_data_size_estimate(
        self,
        ray_start_regular_shared,
        image_size,
        image_mode,
        expected_size,
        expected_ratio,
    ):
        root = "example://image-datasets/different-sizes"
        ds = ray.data.read_images(
            root, size=(image_size, image_size), mode=image_mode, override_num_blocks=1
        )

        data_size = ds.size_bytes()
        assert data_size >= 0, "estimated data size is out of expected bound"
        data_size = ds.materialize().size_bytes()
        assert data_size >= 0, "actual data size is out of expected bound"

        datasource = ImageDatasource(
            paths=[root],
            size=(image_size, image_size),
            mode=image_mode,
            filesystem=LocalFileSystem(),
            partitioning=None,
            meta_provider=ImageFileMetadataProvider(),
        )
        assert (
            datasource._encoding_ratio >= expected_ratio
            and datasource._encoding_ratio <= expected_ratio * 1.5
        ), "encoding ratio is out of expected bound"
        data_size = datasource.estimate_inmemory_data_size()
        assert data_size >= 0, "estimated data size is out of expected bound"

    def test_dynamic_block_split(ray_start_regular_shared):
        ctx = ray.data.context.DataContext.get_current()
        target_max_block_size = ctx.target_max_block_size
        # Reduce target max block size to trigger block splitting on small input.
        # Otherwise we have to generate big input files, which is unnecessary.
        ctx.target_max_block_size = 1
        try:
            root = "example://image-datasets/simple"
            ds = ray.data.read_images(root, override_num_blocks=1)
            assert ds._plan.initial_num_blocks() == 1
            ds = ds.materialize()
            # Verify dynamic block splitting taking effect to generate more blocks.
            assert ds._plan.initial_num_blocks() == 3

            # Test union of same datasets
            union_ds = ds.union(ds, ds, ds).materialize()
            assert union_ds._plan.initial_num_blocks() == 12
        finally:
            ctx.target_max_block_size = target_max_block_size

    def test_unidentified_image_error(ray_start_regular_shared):
        with tempfile.NamedTemporaryFile(suffix=".png") as file:
            with pytest.raises(ValueError):
                ray.data.read_images(paths=file.name).materialize()


class TestWriteImages:
    def test_write_images(ray_start_regular_shared, tmp_path):
        ds = ray.data.read_images("example://image-datasets/simple")
        ds.write_images(
            path=tmp_path,
            column="image",
        )

        assert len(os.listdir(tmp_path)) == ds.count()

        for filename in os.listdir(tmp_path):
            path = os.path.join(tmp_path, filename)
            Image.open(path)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
