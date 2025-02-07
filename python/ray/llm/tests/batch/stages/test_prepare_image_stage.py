import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from PIL import Image
import io
import base64

from ray.llm._internal.batch.stages.prepare_image_stage import (
    PrepareImageUDF,
    ImageProcessor,
)


@pytest.fixture
def mock_image():
    # Create a small test image
    img = Image.new("RGB", (100, 100), color="red")
    return img


@pytest.fixture
def mock_http_connection():
    with patch(
        "ray.llm._internal.batch.stages.prepare_image_stage.HTTPConnection"
    ) as mock:
        connection = MagicMock()
        connection.async_get_bytes = AsyncMock()
        mock.return_value = connection
        yield connection


@pytest.fixture
def mock_image_processor(mock_http_connection, mock_image):
    with patch(
        "ray.llm._internal.batch.stages.prepare_image_stage.ImageProcessor"
    ) as mock:
        processor = MagicMock()
        processor.process = AsyncMock(
            side_effect=lambda images: [mock_image] * len(images)
        )
        mock.return_value = processor
        yield processor


@pytest.mark.asyncio
async def test_prepare_image_udf_basic(mock_image_processor, mock_image):
    udf = PrepareImageUDF(data_column="__data")

    # Test batch with one message containing an image URL
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "image", "image": "http://example.com/image.jpg"}
                        ]
                    }
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert "image" in results[0]
    assert "image_sizes" in results[0]
    assert len(results[0]["image"]) == 1
    assert all(isinstance(img, Image.Image) for img in results[0]["image"])


@pytest.mark.asyncio
async def test_prepare_image_udf_multiple_images(mock_image_processor, mock_image):
    udf = PrepareImageUDF(data_column="__data")

    # Test batch with multiple images in one message
    batch = {
        "__data": [
            {
                "messages": [
                    {
                        "content": [
                            {"type": "image", "image": "http://example.com/image1.jpg"},
                            {"type": "image", "image": "http://example.com/image2.jpg"},
                        ]
                    }
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert len(results[0]["image"]) == 2
    assert len(results[0]["image_sizes"]) == 2


@pytest.mark.asyncio
async def test_prepare_image_udf_no_images(mock_image_processor):
    udf = PrepareImageUDF(data_column="__data")

    # Test batch with no images
    batch = {"__data": [{"messages": [{"content": "Hello, world!"}]}]}

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert results[0] == {"messages": [{"content": "Hello, world!"}]}


@pytest.mark.asyncio
async def test_image_processor_fetch_images(mock_http_connection, mock_image):
    processor = ImageProcessor()

    # Create a base64 image
    img_byte_arr = io.BytesIO()
    mock_image.save(img_byte_arr, format="PNG")
    img_byte_arr = img_byte_arr.getvalue()
    base64_image = f"data:image/png;base64,{base64.b64encode(img_byte_arr).decode()}"

    # Test HTTP image
    mock_http_connection.async_get_bytes.return_value = img_byte_arr
    http_images = await processor.fetch_images(["http://example.com/image.jpg"])
    assert len(http_images) == 1
    assert isinstance(http_images[0], Image.Image)

    # Test base64 image
    base64_images = await processor.fetch_images([base64_image])
    assert len(base64_images) == 1
    assert isinstance(base64_images[0], Image.Image)


def test_prepare_image_udf_expected_keys():
    udf = PrepareImageUDF(data_column="__data")
    assert udf.expected_input_keys == ["messages"]


@pytest.mark.asyncio
async def test_prepare_image_udf_invalid_image_type(mock_image_processor):
    udf = PrepareImageUDF(data_column="__data")

    # Test batch with invalid image type
    batch = {
        "__data": [
            {
                "messages": [
                    {"content": [{"type": "image", "image": 123}]}  # Invalid image type
                ]
            }
        ]
    }

    with pytest.raises(ValueError, match="Cannot handle image type"):
        async for _ in udf(batch):
            pass
