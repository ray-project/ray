from typing import TYPE_CHECKING

import asyncio
import base64
import io

import aiohttp

from ray.llm._internal.utils import try_import

if TYPE_CHECKING:
    from PIL.Image import Image

PIL = try_import("PIL")

# TODO(xwjiang): Make this configurable in Launch Darkly.
TIMEOUT = 10  # seconds
RETRIES = 3  # Number of retries on timeout


class ImageRetriever:
    """Retrieves images."""

    async def get(self, url: str) -> "Image":
        """Retrieves an image."""
        if url.startswith("data"):
            base64_encoded_str = url.split(",")[1]
            try:
                image_data = base64.b64decode(base64_encoded_str)
            except base64.binascii.Error as e:
                raise ValueError("Failed to decode base64 string") from e
        else:
            for attempt in range(RETRIES):
                try:
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=TIMEOUT)
                    ) as session:
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                image_data = await resp.read()
                                break
                            else:
                                raise RuntimeError(
                                    f"Failed to fetch image from {url}, received status code: {resp.status}"
                                )
                except asyncio.TimeoutError:
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    else:
                        raise RuntimeError(
                            "Request timed out after several retries"
                        ) from None
                except aiohttp.ClientError as e:
                    raise RuntimeError("Network error occurred") from e

        try:
            image = PIL.Image.open(io.BytesIO(image_data))
            return image
        except PIL.UnidentifiedImageError as e:
            raise ValueError("Failed to identify image") from e
