import sys
import copy
import logging
import requests
import tempfile
import zipfile
import hashlib
import shutil

import pytest
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)

JOB_ROOT_DIR = "/tmp/ray/job"
TEST_PYTHON_JOB = {
    'name': 'Test job',
    'owner': 'abc.xyz',
    'language': 'PYTHON',
    'url': 'http://xxx/yyy.zip',
    'driverEntry': 'python_file_name_without_ext',
    'driverArgs': [],
    'customConfig': {
        'k1': 'v1',
        'k2': 'v2'
    },
    'jvmOptions': '-Dabc=123 -Daaa=xxx',
    'dependencies': {
        'python': [
            'py-spy >= 0.2.0',
        ],
        'java': [{
            "name": "spark",
            "version": "2.1",
            "url": "http://xxx/yyy.jar",
            "md5": "<md5 hex>"
        }]
    }
}

TEST_PYTHON_JOB_CODE = """
import os
import ray
import time


@ray.remote
class Actor:
    def __init__(self, index):
        self._index = index

    def foo(self, x):
        print("worker job dir {}".format(os.environ["RAY_JOB_DIR"]))
        print("worker cwd {}".format(os.getcwd()))
        return "Actor {}: {}".format(self._index, x)


def main():
    actors = []
    print("driver job dir {}".format(os.environ["RAY_JOB_DIR"]))
    print("driver cwd {}".format(os.getcwd()))
    for x in range(2):
        actors.append(Actor.remote(x))

    counter = 0
    while True:
        for a in actors:
            r = a.foo.remote(counter)
            print(ray.get(r))
            counter += 1
            time.sleep(1)


if __name__ == "__main__":
    ray.init()
    main()
"""


def _gen_job_zip(job_code, driver_entry):
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        with zipfile.ZipFile(f, mode="w") as zip_f:
            with zip_f.open(f"{driver_entry}.py", "w") as driver:
                driver.write(job_code.encode())
        return f.name


def _gen_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _gen_url(weburl, path):
    return f"{weburl}/test/file?path={path}"


def _get_python_job(web_url,
                    java_dependency_url=None,
                    java_dependency_md5=None):
    driver_entry = "simple_job"
    path = _gen_job_zip(TEST_PYTHON_JOB_CODE, driver_entry)
    url = _gen_url(web_url, path)
    job = copy.deepcopy(TEST_PYTHON_JOB)
    job["url"] = url
    job["driverEntry"] = driver_entry
    if java_dependency_url:
        job["dependencies"]["java"][0]["url"] = java_dependency_url
    if java_dependency_md5:
        job["dependencies"]["java"][0]["md5"] = java_dependency_md5
    return job


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_load_code_from_local": True,
    }],
    indirect=True)
def test_submit_job_with_invalid_url(disable_aiohttp_cache, enable_test_module,
                                     ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs", json=_get_python_job(webui_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED", job_info["failErrorMessage"]
            assert "ClientConnectorError" in job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=20)


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_load_code_from_local": True,
    }],
    indirect=True)
def test_submit_job_with_incorrect_md5(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url, java_dependency_url=fake_jar_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED", job_info["failErrorMessage"]
            assert "is corrupted" in job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=20)


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_load_code_from_local": True,
    }],
    indirect=True)
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)
    fake_jar_md5 = _gen_md5(__file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 1

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_running():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "RUNNING", job_info["failErrorMessage"]
            job_actors = result["data"]["detail"]["jobActors"]
            job_workers = result["data"]["detail"]["jobWorkers"]
            assert len(job_actors) > 0
            assert len(job_workers) > 0
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_running, timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
