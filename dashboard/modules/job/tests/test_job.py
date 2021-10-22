import sys

import logging
import requests
from uuid import uuid4
import pytest

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url,
                                     wait_until_server_available)
from ray._private.job_manager import JobStatus

logger = logging.getLogger(__name__)


def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    job_id = str(uuid4())

    resp = requests.post(f"{webui_url}/submit", json={"job_id": job_id})
    resp.raise_for_status()
    result = resp.json()
    assert result["data"]["data"] == job_id

    resp = requests.get(f"{webui_url}/status", json={"job_id": job_id})
    resp.raise_for_status()
    result = resp.json()
    assert result["data"]["data"] == JobStatus.SUCCEEDED.value

    resp = requests.get(f"{webui_url}/logs", json={"job_id": job_id})
    resp.raise_for_status()
    result = resp.json()
    assert result["data"]["data"]["stdout"] == "hello"
    assert result["data"]["data"]["stderr"] == ""


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
