import logging
import sys

# Import asyncio timeout depends on python version
if sys.version_info >= (3, 11):
    from asyncio import timeout as asyncio_timeout
else:
    from async_timeout import timeout as asyncio_timeout


logger = logging.getLogger(__name__)


async def http_get(http_session, url, timeout_seconds=60):
    async with asyncio_timeout(timeout_seconds):
        async with http_session.get(url) as response:
            return await response.json()


if __name__ == "__main__":
    pass
