"""HTTP helper utilities for the Ray Data video processing example."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional
from urllib.parse import urlparse

import aiohttp
import requests

_USER_AGENT = "Ray-Video-Example/1.0"


class HTTPConnection:
    """Small helper around ``requests``/``aiohttp`` for reuseable HTTP clients."""

    def __init__(self, *, reuse_client: bool = True) -> None:
        self.reuse_client = reuse_client
        self._sync_client: Optional[Any] = None
        self._async_client: Optional[Any] = None

    def get_sync_client(self):
        if requests is None:
            raise ImportError(
                "requests is required for HTTPConnection. Install with `pip install requests`."
            )
        if self._sync_client is None or not self.reuse_client:
            self._sync_client = requests.Session()
        return self._sync_client

    async def get_async_client(self):
        if aiohttp is None:
            raise ImportError(
                "aiohttp is required for HTTPConnection. Install with `pip install aiohttp`."
            )
        if self._async_client is None or not self.reuse_client:
            self._async_client = aiohttp.ClientSession()
        return self._async_client

    def _validate_http_url(self, url: str) -> None:
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ("http", "https"):
            raise ValueError("Invalid HTTP URL: scheme must be 'http' or 'https'.")

    def _headers(self, **extras: str) -> MutableMapping[str, str]:
        return {"User-Agent": _USER_AGENT, **extras}

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
            url,
            headers=self._headers(**extra_headers),
            stream=stream,
            timeout=timeout,
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
        return client.get(
            url,
            headers=self._headers(**extra_headers),
            timeout=timeout,
        )

    def get_bytes(self, url: str, *, timeout: Optional[float] = None) -> bytes:
        with self.get_response(url, stream=False, timeout=timeout) as r:
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

    def download_file(
        self,
        url: str,
        save_path: Path,
        *,
        timeout: Optional[float] = None,
        chunk_size: int = 512 * 1024,
    ) -> Path:
        with self.get_response(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with save_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size):
                    if chunk:
                        f.write(chunk)
        return save_path

    async def async_download_file(
        self,
        url: str,
        save_path: Path,
        *,
        timeout: Optional[float] = None,
        chunk_size: int = 512 * 1024,
    ) -> Path:
        async with await self.get_async_response(url, timeout=timeout) as r:
            r.raise_for_status()
            with save_path.open("wb") as f:
                async for chunk in r.content.iter_chunked(chunk_size):
                    if chunk:
                        f.write(chunk)
        return save_path

    def download_bytes_chunked(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
        chunk_size: int = 512 * 1024,
    ) -> bytes:
        """Stream a response into memory to avoid large one-shot downloads."""
        with self.get_response(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            bio = BytesIO()
            for chunk in r.iter_content(chunk_size):
                if chunk:
                    bio.write(chunk)
            return bio.getvalue()
