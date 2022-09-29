import numpy as np
import pytest
from ray.data.datasource.image_datasource import (
    IMAGE_EXTENSIONS,
    _ImageDatasourceReader,
    ImageDatasource,
)
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data.datasource import Partitioning
from ray.data.extensions import TensorDtype
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


class TestReadImages:
    def test_basic(self, ray_start_regular_shared):
        """Test basic `read_images` functionality.
        The folder "simple" contains three 32x32 RGB images.
        """
        ds = ray.data.read_images("example://image-datasets/simple")
        assert ds.schema() is np.ndarray
        assert all(array.shape == (32, 32, 3) for array in ds.take())

    def test_filtering(self, ray_start_regular_shared):
        """Test `read_images` correctly filters non-image files.
        The folder "different-extensions" contains three 32x32 RGB images and two
        non-images.
        """
        ds = ray.data.read_images("example://image-datasets/different-extensions")
        assert ds.count() == 3

    def test_size(self, ray_start_regular_shared):
        """Test `read_images` size parameter works with differently-sized images.
        The folder "different-sizes" contains three RGB images. Each image has a
        different size, with the size described in file names (e.g., 32x32.png).
        """
        ds = ray.data.read_images(
            "example://image-datasets/different-sizes", size=(32, 32)
        )
        assert all(array.shape == (32, 32, 3) for array in ds.take())

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
        """Test `read_images` works with images from different colorspaces.
        The folder "different-modes" contains three 32x32 images with modes "CMYK", "L",
        and "RGB".
        """
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
        if enable_automatic_tensor_extension_cast:
            assert isinstance(image_type, TensorDtype)
        else:
            assert image_type == object
        assert label_type == object

        df = ds.to_pandas()
        assert sorted(df["label"]) == ["cat", "cat", "dog"]
        assert all(array.shape == (32, 32, 3) for array in df["image"])

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
