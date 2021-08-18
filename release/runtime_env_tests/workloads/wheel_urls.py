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

from ray._private.utils import get_master_wheel_url, get_release_wheel_url


def update_progress(result):
    result["last_update"] = time.time()
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/release_test_output.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


if __name__ == "__main__":
    # Fail if running on a build from source that doesn't have a commit and
    # hasn't been uploaded as a wheel to AWS.
    assert "RAY_COMMIT_SHA" not in ray.__commit__, ray.__commit__

    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            if "dev" in ray.__version__:
                url = get_master_wheel_url(
                    ray_commit=ray.__commit__,
                    sys_platform=sys_platform,
                    ray_version=ray.__version__,
                    py_version=py_version)
            else:
                url = get_release_wheel_url(
                    ray_commit=ray.__commit__,
                    sys_platform=sys_platform,
                    ray_version=ray.__version__,
                    py_version=py_version)
            assert requests.head(url).status_code == 200, url
            print("Successfully tested URL: ", url)
            update_progress({"url": url})

    print("PASSED")
