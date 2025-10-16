"""Prepare Image Stage"""

import asyncio
import base64
import importlib
import logging
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Union,
)
from urllib.parse import urlparse

import aiohttp
import requests

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)

# TODO: Remove the guard once Pillow is added into the dependencies.
if TYPE_CHECKING:
    from PIL import Image

logger = logging.getLogger(__name__)

_ImageType = Union[str, "Image.Image"]


class HTTPConnection:
    """Adapted from vllm.connections.HTTPConnection.
    Helper class to send HTTP requests.
    """

    def __init__(self, *, reuse_client: bool = True) -> None:
        super().__init__()

        self.reuse_client = reuse_client

        self._sync_client: Optional[requests.Session] = None
        self._async_client: Optional[aiohttp.ClientSession] = None

    def get_sync_client(self) -> requests.Session:
        if self._sync_client is None or not self.reuse_client:
            self._sync_client = requests.Session()

        return self._sync_client

    # NOTE: We intentionally use an async function even though it is not
    # required, so that the client is only accessible inside async event loop
    async def get_async_client(self) -> aiohttp.ClientSession:
        if self._async_client is None or not self.reuse_client:
            self._async_client = aiohttp.ClientSession()

        return self._async_client

    def _validate_http_url(self, url: str):
        parsed_url = urlparse(url)

        if parsed_url.scheme not in ("http", "https"):
            raise ValueError(
                "Invalid HTTP URL: A valid HTTP URL "
                "must have scheme 'http' or 'https'."
            )

    def _headers(self, **extras: str) -> MutableMapping[str, str]:
        return {"User-Agent": "RayLLM-Batch", **extras}

    def get_response(
        self,
        url: str,
        *,
        stream: bool = False,
        timeout: Optional[float] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
    ):
        self._validate_http_url(url)

        client = self.get_sync_client()
        extra_headers = extra_headers or {}

        return client.get(
            url, headers=self._headers(**extra_headers), stream=stream, timeout=timeout
        )

    async def get_async_response(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
    ):
        self._validate_http_url(url)

        client = await self.get_async_client()
        extra_headers = extra_headers or {}

        return client.get(url, headers=self._headers(**extra_headers), timeout=timeout)

    def get_bytes(self, url: str, *, timeout: Optional[float] = None) -> bytes:
        with self.get_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return r.content

    async def async_get_bytes(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
    ) -> bytes:
        async with await self.get_async_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return await r.read()

    def get_text(self, url: str, *, timeout: Optional[float] = None) -> str:
        with self.get_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return r.text

    async def async_get_text(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
    ) -> str:
        async with await self.get_async_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return await r.text()

    def get_json(self, url: str, *, timeout: Optional[float] = None) -> str:
        with self.get_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return r.json()

    async def async_get_json(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
    ) -> str:
        async with await self.get_async_response(url, timeout=timeout) as r:
            r.raise_for_status()

            return await r.json()

    def download_file(
        self,
        url: str,
        save_path: Path,
        *,
        timeout: Optional[float] = None,
        chunk_size: int = 128,
    ) -> Path:
        with self.get_response(url, timeout=timeout) as r:
            r.raise_for_status()

            with save_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)

        return save_path

    async def async_download_file(
        self,
        url: str,
        save_path: Path,
        *,
        timeout: Optional[float] = None,
        chunk_size: int = 128,
    ) -> Path:
        async with await self.get_async_response(url, timeout=timeout) as r:
            r.raise_for_status()

            with save_path.open("wb") as f:
                async for chunk in r.content.iter_chunked(chunk_size):
                    f.write(chunk)

        return save_path


class ImageProcessor:
    """Download and load images."""

    def __init__(self):
        self.Image = importlib.import_module("PIL.Image")
        self.http_connection = HTTPConnection()

    async def download_image_from_url(self, image_url: str) -> Optional[bytes]:
        """Download the image from the Internet with up to 3 retries.

        Args:
            image_url: The image URL to download.

        Returns:
            The image bytes (None if failed to download).
        """
        for _ in range(3):
            try:
                image_raw = await self.http_connection.async_get_bytes(
                    image_url, timeout=5
                )
                return image_raw
            except Exception:
                await asyncio.sleep(1)
        return None

    async def load_image_bytes_from_url(self, image_urls: List[str]) -> List[bytes]:
        """Load an image from a URL.

        Args:
            image_urls: The image URLs to load.

        Returns:
            The image bytes.
        """
        return await asyncio.gather(
            *[self.download_image_from_url(image_url) for image_url in image_urls]
        )

    async def fetch_images(
        self, image_urls: List[str], *, image_mode: Optional[str] = None
    ) -> List["Image.Image"]:
        """
        Adapted from vllm.multimodal.utils.fetch_image.
        Load a PIL image from a HTTP or base64 data URL.

        Args:
            image_urls: A list of URLs of the images.
            image_mode: The mode of the image. If None, the image is not converted.

        Returns:
            A list of loaded images.
        """

        def _load_image_from_bytes(b: bytes):
            image = self.Image.open(BytesIO(b))
            image.load()
            return image

        def _load_image_from_data_url(image_url: str):
            # Only split once and assume the second part is the base64 encoded image
            _, image_base64 = image_url.split(",", 1)
            return _load_image_from_bytes(base64.b64decode(image_base64))

        # Check if all image URLs are of the same type.
        if image_urls[0].startswith("http"):
            image_url_prefix = "http"
        elif image_urls[0].startswith("data:image"):
            image_url_prefix = "data:image"
        else:
            raise ValueError(f"Invalid image URL prefix: {image_urls[0]}")

        if not all(url.startswith(image_url_prefix) for url in image_urls):
            raise ValueError(
                f"All image URLs must have the same prefix, got {image_url_prefix=}"
            )

        if image_url_prefix == "http":
            image_raws = await self.load_image_bytes_from_url(image_urls)
            images = [_load_image_from_bytes(image_raw) for image_raw in image_raws]
        elif image_url_prefix == "data:image":
            images = [_load_image_from_data_url(image_url) for image_url in image_urls]
        else:
            raise ValueError(
                "Invalid 'image_url': A valid 'image_url' must start "
                "with either 'data:image' or 'http'."
            )

        if image_mode is not None and images[0].mode != image_mode:
            images = [image.convert(image_mode) for image in images]
        return images

    async def process(self, images: List[_ImageType]) -> List["Image.Image"]:
        """Load and resize an image for the model.
        Args:
            image: A list of images.

        Returns:
            A list of processed images.
        """
        if not images:
            return []

        # Check if all images are of the same type.
        image_type = type(images[0])
        if not all(isinstance(img, image_type) for img in images):
            raise ValueError(f"All images must be of the same type, got {image_type=}")

        if not issubclass(image_type, self.Image.Image):
            images = await self.fetch_images(images)
        return images


class PrepareImageUDF(StatefulStageUDF):
    def __init__(self, data_column: str, expected_input_keys: List[str]):
        super().__init__(data_column, expected_input_keys)
        self.Image = importlib.import_module("PIL.Image")
        self.image_processor = ImageProcessor()

    def extract_image_info(self, messages: List[Dict]) -> List[_ImageType]:
        """Extract image information from chat messages.

        Args:
            messages: List of chat messages.

        Returns:
            List of _ImageType.

        Note:
            The optional 'detail' parameter from the OpenAI schema is not
            passed forward to downstream templates.
        """

        image_info: List[_ImageType] = []
        for message in messages:
            content = message["content"]

            # Convert PyArrow objects to Python objects if needed (like ChatTemplateStage).
            # This handles the case where unform content types are serialized with PyArrow
            # instead of pickle- happens when all messages have the same content structure
            # (e.g., no system prompt + string content mixed with user messages with list content).
            if hasattr(content, "tolist"):
                content = content.tolist()

            if not isinstance(content, list):
                continue
            for content_item in content:
                if content_item["type"] not in ("image", "image_url"):
                    continue

                image_data = content_item[content_item["type"]]

                if content_item["type"] == "image_url" and isinstance(image_data, dict):
                    # OpenAI nested format: {"image_url": {"url": "..."}}
                    image = image_data.get("url")
                    if not isinstance(image, str) or not image:
                        raise ValueError(
                            "image_url must be an object with a non-empty 'url' string"
                        )
                else:
                    # Simple format: {"image": "..."} or {"image_url": "..."}
                    image = image_data

                if not isinstance(image, str) and not isinstance(
                    image, self.Image.Image
                ):
                    raise ValueError(f"Cannot handle image type {type(image)}")
                image_info.append(image)
        return image_info

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        messages = [row["messages"] for row in batch]

        # Process all images in this batch.
        all_image_info = [self.extract_image_info(message) for message in messages]
        flat_all_image_info = [img for imgs in all_image_info for img in imgs]
        flat_all_images = await self.image_processor.process(flat_all_image_info)

        # TODO: We now use asyncio.gather to process all images in this batch,
        # so the outputs here must be in order. However, it is more efficient
        # to support out-of-order outputs so that we won't be blocked by slow
        # downloaded images.
        img_start_idx = 0
        idx_in_batch = 0
        for image_info_per_req in all_image_info:
            num_images_in_req = len(image_info_per_req)
            ret = {self.IDX_IN_BATCH_COLUMN: idx_in_batch}
            idx_in_batch += 1
            if num_images_in_req > 0:
                images = flat_all_images[
                    img_start_idx : img_start_idx + num_images_in_req
                ]
                ret.update(
                    {
                        "image": images,
                        "image_sizes": [(img.width, img.height) for img in images],
                    }
                )
                img_start_idx += num_images_in_req
            yield ret


class PrepareImageStage(StatefulStage):
    """A stage to prepare images from OpenAI chat template messages."""

    fn: StatefulStageUDF = PrepareImageUDF

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        return {
            "messages": "A list of messages in OpenAI chat format. "
            "See https://platform.openai.com/docs/api-reference/chat/create "
            "for details."
        }
