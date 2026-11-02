import logging
import sys
from unittest.mock import patch

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer
from aiohttp.web import Application, StreamResponse

from ray.dashboard.optional_utils import init_ray_and_catch_exceptions
from ray.dashboard.routes import method_route_table_factory
from ray.dashboard.utils import close_logger_file_descriptor


def test_close_logger_file_descriptor():
    logger_format = "%(message)s"
    logger = logging.getLogger("test_job_id")

    job_driver_log_path = "/tmp/ray.log"
    job_driver_handler = logging.FileHandler(job_driver_log_path)
    job_driver_formatter = logging.Formatter(logger_format)
    job_driver_handler.setFormatter(job_driver_formatter)
    logger.addHandler(job_driver_handler)

    assert job_driver_handler.stream.closed is False
    close_logger_file_descriptor(logger)
    assert job_driver_handler.stream is None


@pytest.mark.asyncio
async def test_route_wrappers_do_not_replace_started_response():
    routes = method_route_table_factory()

    class Handler:
        @routes.get("/before")
        @init_ray_and_catch_exceptions()
        async def fail_before_prepare(self, request):
            raise RuntimeError("test error")

        @routes.get("/after")
        @init_ray_and_catch_exceptions()
        async def fail_after_prepare(self, request):
            response = StreamResponse()
            await response.prepare(request)
            await response.write(b"partial")
            response.force_close()
            raise RuntimeError("test error")

    app = Application()
    handler = Handler()
    routes.bind(handler)
    app.add_routes(routes.bound_routes())

    with patch("ray.dashboard.optional_utils.ray.is_initialized", return_value=True):
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/before")
            assert response.status == 500

            response = await client.get("/after")
            assert response.status == 200
            with pytest.raises(aiohttp.ClientPayloadError):
                await response.read()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
