import re
import sys
import time

import pytest
import requests

import ray


@pytest.mark.skipif(
    sys.version_info < (3, 5, 3), reason="requires python3.5.3 or higher")
def test_dashboard(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    dashboard_url = addresses["webui_url"]
    assert ray.get_dashboard_url() == dashboard_url

    assert re.match(r"^(localhost|\d+\.\d+\.\d+\.\d+):\d+$", dashboard_url)

    start_time = time.time()
    while True:
        try:
            node_info_url = f"http://{dashboard_url}/nodes"
            resp = requests.get(node_info_url, params={"view": "summary"})
            resp.raise_for_status()
            summaries = resp.json()
            assert summaries["result"] is True
            assert "msg" in summaries
            break
        except (requests.exceptions.ConnectionError, AssertionError):
            if time.time() > start_time + 30:
                out_log = None
                with open(
                        "{}/logs/dashboard.log".format(
                            addresses["session_dir"]), "r") as f:
                    out_log = f.read()
                raise Exception(
                    "Timed out while waiting for dashboard to start. "
                    f"Dashboard output log: {out_log}\n")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
