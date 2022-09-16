

import numpy as np
import pytest
from ray.data.datasource.image_folder_datasource import (
    IMAGE_EXTENSIONS,
    _ImageFolderDatasourceReader,
)
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data.datasource import (
    ImageFolderDatasource
)
from ray.data.extensions import TensorDtype
from ray.data.preprocessors import BatchMapper
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

def test_image_folder_datasource(
    ray_start_regular_shared, enable_automatic_tensor_extension_cast
):
    """Test basic `ImageFolderDatasource` functionality.

    The folder "simple" contains two cat images and one dog images, all of which are
    are 32x32 RGB images.
    """
    root = "example://image-folders/simple"
    ds = ray.data.read_datasource(ImageFolderDatasource(), root=root)

    _, types = ds.schema()
    image_type, label_type = types
    if enable_automatic_tensor_extension_cast:
        assert isinstance(image_type, TensorDtype)
    else:
        assert image_type == np.dtype("O")
    assert label_type == np.dtype("O")

    df = ds.to_pandas()
    assert sorted(df["label"]) == ["cat", "cat", "dog"]

    tensors = df["image"]
    assert all(tensor.shape == (32, 32, 3) for tensor in tensors)


def test_image_folder_datasource_filtering(
    ray_start_regular_shared, enable_automatic_tensor_extension_cast
):
    """Test `ImageFolderDatasource` correctly filters non-image files.

    The folder "different-extensions" contains two cat images, one dog image, and two
    non-images. All images are 32x32 RGB images.
    """
    root = "example://image-folders/different-extensions"
    ds = ray.data.read_datasource(ImageFolderDatasource(), root=root)

    assert ds.count() == 3
    assert sorted(ds.to_pandas()["label"]) == ["cat", "cat", "dog"]


def test_image_folder_datasource_size_parameter(
    ray_start_regular_shared, enable_automatic_tensor_extension_cast
):
    """Test `ImageFolderDatasource` size parameter works with differently-sized images.

    The folder "different-sizes" contains two cat images and one dog image. Each image
    has a different size, with the size described in file names (e.g., 32x32.png). All
    images are RGB images.
    """
    root = "example://image-folders/different-sizes"
    ds = ray.data.read_datasource(ImageFolderDatasource(), root=root, size=(32, 32))

    tensors = ds.to_pandas()["image"]
    assert all(tensor.shape == (32, 32, 3) for tensor in tensors)


def test_image_folder_datasource_retains_shape_without_cast(
    ray_start_regular_shared, enable_automatic_tensor_extension_cast
):
    """Test `ImageFolderDatasource` retains image shapes if casting is disabled.

    The folder "different-sizes" contains two cat images and one dog image. The image
    sizes are 16x16, 32x32, and 64x32. All images are RGB images.
    """
    if enable_automatic_tensor_extension_cast:
        return

    root = "example://image-folders/different-sizes"
    ds = ray.data.read_datasource(ImageFolderDatasource(), root=root)
    arrays = ds.to_pandas()["image"]
    shapes = sorted(array.shape for array in arrays)
    assert shapes == [(16, 16, 3), (32, 32, 3), (64, 64, 3)]


@pytest.mark.parametrize(
    "mode, expected_shape", [("L", (32, 32)), ("RGB", (32, 32, 3))]
)
def test_image_folder_datasource_mode_parameter(
    mode,
    expected_shape,
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    """Test `ImageFolderDatasource` works with images from different colorspaces.

    The folder "different-modes" contains two cat images and one dog image. Their modes
    are "CMYK", "L", and "RGB". All images are 32x32.
    """
    root = "example://image-folders/different-modes"
    ds = ray.data.read_datasource(ImageFolderDatasource(), root=root, mode=mode)

    tensors = ds.to_pandas()["image"]
    assert all([tensor.shape == expected_shape for tensor in tensors])


@pytest.mark.parametrize("size", [(-32, 32), (32, -32), (-32, -32)])
def test_image_folder_datasource_value_error(ray_start_regular_shared, size):
    root = "example://image-folders/simple"
    with pytest.raises(ValueError):
        ray.data.read_datasource(ImageFolderDatasource(), root=root, size=size)


def test_image_folder_datasource_e2e(ray_start_regular_shared):
    from ray.train.torch import TorchCheckpoint, TorchPredictor
    from ray.train.batch_predictor import BatchPredictor

    from torchvision import transforms
    from torchvision.models import resnet18

    root = "example://image-folders/simple"
    dataset = ray.data.read_datasource(
        ImageFolderDatasource(), root=root, size=(32, 32)
    )

    def preprocess(df):
        preprocess = transforms.Compose([transforms.ToTensor()])
        df.loc[:, "image"] = [preprocess(image).numpy() for image in df["image"]]
        return df

    preprocessor = BatchMapper(preprocess)

    model = resnet18(pretrained=True)
    checkpoint = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

    predictor = BatchPredictor.from_checkpoint(checkpoint, TorchPredictor)
    predictor.predict(dataset, feature_columns=["image"])


@pytest.mark.parametrize(
    "image_size,image_mode,expected_size,expected_ratio",
    [(64, "RGB", 30000, 4), (32, "L", 3500, 0.5), (256, "RGBA", 750000, 85)],
)
def test_image_folder_reader_estimate_data_size(
    ray_start_regular_shared, image_size, image_mode, expected_size, expected_ratio
):
    root = "example://image-folders/different-sizes"
    ds = ray.data.read_datasource(
        ImageFolderDatasource(),
        root=root,
        size=(image_size, image_size),
        mode=image_mode,
    )

    data_size = ds.size_bytes()
    assert (
        data_size >= expected_size and data_size <= expected_size * 1.5
    ), "estimated data size is out of expected bound"
    data_size = ds.fully_executed().size_bytes()
    assert (
        data_size >= expected_size and data_size <= expected_size * 1.5
    ), "actual data size is out of expected bound"

    reader = _ImageFolderDatasourceReader(
        delegate=ImageFolderDatasource(),
        paths=[root],
        filesystem=LocalFileSystem(),
        partition_filter=FileExtensionFilter(file_extensions=IMAGE_EXTENSIONS),
        root=root,
        size=(image_size, image_size),
        mode=image_mode,
    )
    assert (
        reader._encoding_ratio >= expected_ratio
        and reader._encoding_ratio <= expected_ratio * 1.5
    ), "encoding ratio is out of expected bound"
    data_size = reader.estimate_inmemory_data_size()
    assert (
        data_size >= expected_size and data_size <= expected_size * 1.5
    ), "estimated data size is out of expected bound"