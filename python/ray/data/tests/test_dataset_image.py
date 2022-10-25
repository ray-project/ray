import time

import pyarrow as pa
import pytest

from fsspec.implementations.local import LocalFileSystem

import ray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.data.datasource import Partitioning
from ray.data.datasource.image_datasource import (
    _ImageDatasourceReader,
    ImageDatasource,
)
from ray.data.extensions import ArrowTensorType
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


class TestReadImages:
    def test_basic(self, ray_start_regular_shared):
        """Test basic `read_images` functionality.
        The folder "simple" contains three 32x32 RGB images.
        """
        # "simple" contains three 32x32 RGB images.
        ds = ray.data.read_images("example://image-datasets/simple")
        assert ds.schema().names == [TENSOR_COLUMN_NAME]
        column_type = ds.schema().types[0]
        assert isinstance(column_type, ArrowTensorType)
        assert all(array.shape == (32, 32, 3) for array in ds.take())

    def test_multiple_paths(self, ray_start_regular_shared):
        ds = ray.data.read_images(
            paths=[
                "example://image-datasets/simple/image1.jpg",
                "example://image-datasets/simple/image2.jpg",
            ]
        )
        assert ds.count() == 2

    def test_filtering(self, ray_start_regular_shared):
        # "different-extensions" contains three images and two non-images.
        ds = ray.data.read_images("example://image-datasets/different-extensions")
        assert ds.count() == 3

    def test_size(self, ray_start_regular_shared):
        # "different-sizes" contains RGB images with different heights and widths.
        ds = ray.data.read_images(
            "example://image-datasets/different-sizes", size=(32, 32)
        )
        assert all(array.shape == (32, 32, 3) for array in ds.take())

    def test_different_sizes(self, ray_start_regular_shared):
        ds = ray.data.read_images("example://image-datasets/different-sizes")
        assert sorted(array.shape for array in ds.take()) == [
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
        assert all([array.shape == expected_shape for array in ds.take()])

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

    def test_e2e_prediction(self, ray_start_regular_shared):
        from ray.train.torch import TorchCheckpoint, TorchPredictor
        from ray.train.batch_predictor import BatchPredictor

        from torchvision import transforms
        from torchvision.models import resnet18

        dataset = ray.data.read_images("example://image-datasets/simple")

        transform = transforms.ToTensor()
        dataset = dataset.map(transform)

        model = resnet18(pretrained=True)
        checkpoint = TorchCheckpoint.from_model(model=model)
        predictor = BatchPredictor.from_checkpoint(checkpoint, TorchPredictor)
        predictor.predict(dataset)

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
            root, size=(image_size, image_size), mode=image_mode, parallelism=1
        )

        data_size = ds.size_bytes()
        assert data_size >= 0, "estimated data size is out of expected bound"
        data_size = ds.fully_executed().size_bytes()
        assert data_size >= 0, "actual data size is out of expected bound"

        reader = _ImageDatasourceReader(
            delegate=ImageDatasource(),
            paths=[root],
            filesystem=LocalFileSystem(),
            partition_filter=ImageDatasource.file_extension_filter(),
            partitioning=None,
            size=(image_size, image_size),
            mode=image_mode,
        )
        assert (
            reader._encoding_ratio >= expected_ratio
            and reader._encoding_ratio <= expected_ratio * 1.5
        ), "encoding ratio is out of expected bound"
        data_size = reader.estimate_inmemory_data_size()
        assert data_size >= 0, "estimated data size is out of expected bound"

    def test_dynamic_block_split(ray_start_regular_shared):
        ctx = ray.data.context.DatasetContext.get_current()
        target_max_block_size = ctx.target_max_block_size
        block_splitting_enabled = ctx.block_splitting_enabled
        # Reduce target max block size to trigger block splitting on small input.
        # Otherwise we have to generate big input files, which is unnecessary.
        ctx.target_max_block_size = 1
        ctx.block_splitting_enabled = True
        try:
            root = "example://image-datasets/simple"
            ds = ray.data.read_images(root, parallelism=1)
            assert ds.num_blocks() == 1
            ds.fully_executed()
            # Verify dynamic block splitting taking effect to generate more blocks.
            assert ds.num_blocks() == 3

            # NOTE: Need to wait for 1 second before checking stats, because we report
            # stats to stats actors asynchronously when returning the blocks metadata.
            # TODO(chengsu): clean it up after refactoring lazy block list.
            time.sleep(1)
            assert "3 blocks executed" in ds.stats()

            # Test union of same datasets
            union_ds = ds.union(ds, ds, ds).fully_executed()
            assert union_ds.num_blocks() == 12
            assert "3 blocks executed" in union_ds.stats()
        finally:
            ctx.target_max_block_size = target_max_block_size
            ctx.block_splitting_enabled = block_splitting_enabled


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
