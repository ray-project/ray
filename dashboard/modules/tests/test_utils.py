import logging

import async_timeout

logger = logging.getLogger(__name__)


async def http_get(http_session, url, timeout_seconds=60):
    with async_timeout.timeout(timeout_seconds):
        async with http_session.get(url) as response:
            return await response.json()
