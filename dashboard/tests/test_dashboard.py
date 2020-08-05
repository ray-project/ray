import os
import json
import time
import logging

import ray
import psutil
import pytest

from ray import ray_constants
from ray.test_utils import wait_for_condition

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_internal_config": json.dumps({
            "agent_register_timeout_ms": 5000
        })
    }],
    indirect=True)
def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    all_processes = ray.worker._global_node.all_processes
    assert ray_constants.PROCESS_TYPE_DASHBOARD in all_processes
    assert ray_constants.PROCESS_TYPE_REPORTER not in all_processes
    dashboard_proc_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][
        0]
    dashboard_proc = psutil.Process(dashboard_proc_info.process.pid)
    assert dashboard_proc.status() == psutil.STATUS_RUNNING
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    def _search_agent(processes):
        for p in processes:
            for c in p.cmdline():
                if "new_dashboard/agent.py" in c:
                    return p

    # Test agent restart after dead.
    logger.info("Test agent restart after dead.")
    agent_proc = _search_agent(raylet_proc.children())
    assert agent_proc is not None
    agent_proc.kill()
    agent_proc.wait()
    assert _search_agent(raylet_proc.children()) is None

    logger.info("Test agent register is OK.")
    wait_for_condition(lambda: _search_agent(raylet_proc.children()))
    assert dashboard_proc.status() == psutil.STATUS_RUNNING
    agent_proc = _search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    # Check if agent register OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = _search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)
