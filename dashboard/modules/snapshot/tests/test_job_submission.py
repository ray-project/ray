import logging
import os
import sys
import json
import jsonschema

import pprint
import pytest
import requests

from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.dashboard import dashboard
from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.modules.job.sdk import JobSubmissionClient

logger = logging.getLogger(__name__)


def _get_snapshot(address: str):
    response = requests.get(f"{address}/api/snapshot")
    response.raise_for_status()
    data = response.json()
    schema_path = os.path.join(
        os.path.dirname(dashboard.__file__), "modules/snapshot/snapshot_schema.json"
    )
    pprint.pprint(data)
    jsonschema.validate(instance=data, schema=json.load(open(schema_path)))
    return data


def test_successful_job_status(
    ray_start_with_dashboard, disable_aiohttp_cache, enable_test_module
):
    address = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(address)
    address = format_web_url(address)

    entrypoint_cmd = (
        'python -c"' "import ray;" "ray.init();" "import time;" "time.sleep(5);" '"'
    )

    client = JobSubmissionClient(address)
    job_id = client.submit_job(entrypoint=entrypoint_cmd)

    def wait_for_job_to_succeed():
        data = _get_snapshot(address)
        for job_entry in data["data"]["snapshot"]["jobs"].values():
            if job_entry["status"] is not None:
                assert job_entry["config"]["metadata"]["jobSubmissionId"] == job_id
                assert job_entry["status"] in {"PENDING", "RUNNING", "SUCCEEDED"}
                assert job_entry["statusMessage"] is not None
                return job_entry["status"] == "SUCCEEDED"

        return False

    wait_for_condition(wait_for_job_to_succeed, timeout=30)


def test_failed_job_status(
    ray_start_with_dashboard, disable_aiohttp_cache, enable_test_module
):
    address = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(address)
    address = format_web_url(address)

    entrypoint_cmd = (
        'python -c"'
        "import ray;"
        "ray.init();"
        "import time;"
        "time.sleep(5);"
        "import sys;"
        "sys.exit(1);"
        '"'
    )
    client = JobSubmissionClient(address)
    job_id = client.submit_job(entrypoint=entrypoint_cmd)

    def wait_for_job_to_fail():
        data = _get_snapshot(address)
        for job_entry in data["data"]["snapshot"]["jobs"].values():
            if job_entry["status"] is not None:
                assert job_entry["config"]["metadata"]["jobSubmissionId"] == job_id
                assert job_entry["status"] in {"PENDING", "RUNNING", "FAILED"}
                assert job_entry["statusMessage"] is not None
                return job_entry["status"] == "FAILED"

        return False

    wait_for_condition(wait_for_job_to_fail, timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
