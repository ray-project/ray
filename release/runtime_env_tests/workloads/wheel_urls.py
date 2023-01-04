"""Test downloading Ray wheels for currently running commit

This test runs on a single node and verifies that wheel URLs on all platforms
for the currently running Ray commit are valid.  This test is necessary to
catch changes in the format or location of uploaded wheels. A test like this is
is not straightforward to add in pre-merge CI because at pre-merge time, there
is no commit to master yet and no uploaded wheels.

Runtime environments use these URLs to download the currently running Ray wheel
into isolated conda environments on each worker.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import ray
import os
import json
import time
import requests
import pprint

import ray._private.ray_constants as ray_constants
from ray._private.utils import get_master_wheel_url, get_release_wheel_url


def update_progress(result):
    result["last_update"] = time.time()
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


if __name__ == "__main__":
    # Fail if running on a build from source that doesn't have a commit and
    # hasn't been uploaded as a wheel to AWS.
    assert "RAY_COMMIT_SHA" not in ray.__commit__, ray.__commit__

    retry = set()
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS:
            if "dev" in ray.__version__:
                url = get_master_wheel_url(
                    ray_commit=ray.__commit__,
                    sys_platform=sys_platform,
                    ray_version=ray.__version__,
                    py_version=py_version,
                )
            else:
                url = get_release_wheel_url(
                    ray_commit=ray.__commit__,
                    sys_platform=sys_platform,
                    ray_version=ray.__version__,
                    py_version=py_version,
                )
            if requests.head(url).status_code != 200:
                print("URL not found (yet?):", url)
                retry.add(url)
                continue
            print("Successfully tested URL: ", url)
            update_progress({"url": url})

    num_retries = 0
    MAX_NUM_RETRIES = 12

    while retry and num_retries < MAX_NUM_RETRIES:
        print(
            f"There are {len(retry)} URLs to retry. Sleeping 10 minutes "
            f"to give some time for wheels to be built. "
            f"Trial {num_retries + 1}/{MAX_NUM_RETRIES}."
        )
        print("List of URLs to retry:", retry)
        time.sleep(600)
        print("Retrying now...")
        for url in list(retry):
            if requests.head(url).status_code != 200:
                print(f"URL still not found: {url}")
            else:
                print("Successfully tested URL: ", url)
                update_progress({"url": url})
                retry.remove(url)
        num_retries = num_retries + 1

    if retry:
        print("FAILED")
        print("List of URLs not available after all retries: ")
        pprint.pprint(list(retry))
    else:
        print("PASSED")
