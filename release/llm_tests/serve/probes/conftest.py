import dataclasses
from uuid import uuid4

import pytest
import pytest_asyncio

from probes.openai_client import create_async_client


@pytest.fixture
def test_id():
    test_uuid = f"+TRACE_{uuid4().hex}"
    print(f"Beginning test {test_uuid}")
    yield test_uuid
    print(
        f'Ending test {test_uuid}. Search in Honeycomb with `http.request.header.anyscale_trace_id contains "{test_uuid}"`'
    )


@pytest.hookimpl(tryfirst=True)
def pytest_unconfigure(config):
    plugin = getattr(config, "_buildkite", None)
    if not plugin:
        return
    data = plugin.payload.data
    for d in data:
        plugin.payload = plugin.payload.push_test_data(
            # Test data name is in the format of "test_name[param1-param2-...]". This
            # adds an entry for test data without parameters
            dataclasses.replace(d, name=d.name.split("[")[0]),
        )
    config._buildkite = plugin


@pytest_asyncio.fixture
async def openai_async_client(test_id: str):
    async with create_async_client() as c:
        yield c
