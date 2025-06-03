import base64
import io
import sys
from unittest.mock import AsyncMock, patch

import pytest
from PIL import Image

from ray.llm._internal.serve.deployments.llm.image_retriever import ImageRetriever


def create_dummy_image_bytes():
    image = Image.new("RGB", (1300, 876), color="red")
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format="PNG")
    return img_byte_arr.getvalue()


def get_mock_resp_ctx():
    image_bytes = create_dummy_image_bytes()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=image_bytes)

    mock_resp_ctx = AsyncMock()
    mock_resp_ctx.__aenter__ = AsyncMock(return_value=mock_response)

    return mock_resp_ctx


@pytest.mark.asyncio
async def test_image_processor_with_base64():
    image_bytes = create_dummy_image_bytes()
    base64_encoded_str = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/png;base64,{base64_encoded_str}"

    retriever = ImageRetriever()
    image = await retriever.get(data_url)

    assert isinstance(image, Image.Image)


@pytest.mark.asyncio
async def test_image_processor_with_bad_base64_enc():
    data_url = "data:image/png;base64,invalid_base64_string"

    retriever = ImageRetriever()
    # Act and Assert
    with pytest.raises(ValueError, match="Failed to decode base64 string"):
        await retriever.get(data_url)


@pytest.mark.asyncio
async def test_image_processor_with_http_url():
    mock_resp_ctx = get_mock_resp_ctx()

    retriever = ImageRetriever()
    with patch("aiohttp.ClientSession.get", return_value=mock_resp_ctx):
        image = await retriever.get("http://dummyurl.com/image.png")

    assert isinstance(image, Image.Image)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
