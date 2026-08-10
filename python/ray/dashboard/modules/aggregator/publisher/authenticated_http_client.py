import logging
from typing import Optional

import aiohttp

from ray._private.authentication.http_token_authentication import (
    get_auth_headers_if_auth_enabled,
)
from ray.dashboard.modules.aggregator.publisher.configs import (
    PUBLISHER_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


class AuthenticatedHttpClient:
    """Reusable aiohttp client that attaches Ray's cluster auth token on every request.

    Wraps a single lazily-created ``aiohttp.ClientSession`` and injects the auth headers
    in one place, so callers POSTing to authenticated dashboard endpoints can't
    accidentally send an unauthenticated request.
    """

    def __init__(self, timeout_s: float = PUBLISHER_TIMEOUT_SECONDS) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None

    def post(self, url: str, data: bytes):
        """POST ``data`` to ``url`` with auth headers attached.

        Mirrors ``aiohttp.ClientSession.post`` and returns its request context manager,
        so callers use ``async with client.post(...) as resp``.
        """
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        headers = get_auth_headers_if_auth_enabled({})
        return self._session.post(url, data=data, headers=headers)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
