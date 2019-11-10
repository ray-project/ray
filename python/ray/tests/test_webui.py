from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import sys
import time

import pytest
import requests

import ray


@pytest.mark.skipif(
    sys.version_info < (3, 5, 3), reason="requires python3.5.3 or higher")
def test_get_webui(shutdown_only):
    addresses = ray.init(include_webui=True, num_cpus=1)
    webui_url = addresses["webui_url"]
    assert ray.get_webui_url() == webui_url

    assert re.match(r"^(localhost|\d+\.\d+\.\d+\.\d+):8080$", webui_url)

    start_time = time.time()
    while True:
        try:
            node_info = requests.get("http://" + webui_url +
                                     "/api/node_info").json()
            break
        except requests.exceptions.ConnectionError:
            if time.time() > start_time + 30:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")
    assert node_info["error"] is None
    assert node_info["result"] is not None
    assert isinstance(node_info["timestamp"], float)
