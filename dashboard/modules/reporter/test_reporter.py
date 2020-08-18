import os
import logging
import requests
import time

import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    RayTestTimeoutException,
    wait_until_server_available,
)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def test_profiling(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    @ray.remote(num_cpus=2)
    class Actor:
        def getpid(self):
            return os.getpid()

    c = Actor.remote()
    actor_pid = ray.get(c.getpid.remote())

    webui_url = addresses["webui_url"]
    assert (wait_until_server_available(webui_url) is True)
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")

    start_time = time.time()
    launch_profiling = None
    while True:
        # Sometimes some startup time is required
        if time.time() - start_time > 30:
            raise RayTestTimeoutException(
                "Timed out while collecting profiling stats, "
                "launch_profiling: {}".format(launch_profiling))
        launch_profiling = requests.get(
            webui_url + "/api/launch_profiling",
            params={
                "ip": ray.nodes()[0]["NodeManagerAddress"],
                "pid": actor_pid,
                "duration": 5
            }).json()
        if launch_profiling["result"]:
            profiling_info = launch_profiling["data"]["profilingInfo"]
            break
        time.sleep(1)
    logger.info(profiling_info)
