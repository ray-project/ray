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
            node_info_url = f"http://{dashboard_url}/api/"
            resp = requests.get(node_info_url)
            resp.raise_for_status()
            summaries = resp.json()
            break
        except requests.exceptions.ConnectionError:
            if time.time() > start_time + 30:
                error_log = None
                out_log = None
                with open(
                        "{}/logs/dashboard.out".format(
                            addresses["session_dir"]), "r") as f:
                    out_log = f.read()
                with open(
                        "{}/logs/dashboard.err".format(
                            addresses["session_dir"]), "r") as f:
                    error_log = f.read()
                raise Exception(
                    "Timed out while waiting for dashboard to start. "
                    "Dashboard output log: {}\n"
                    "Dashboard error log: {}\n".format(out_log, error_log))
    assert summaries["error"] is None
    assert summaries["result"] is True
    assert summaries["data"][0]["state"] == "ALIVE"


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
