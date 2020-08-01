import os
import json
import time

import ray
import psutil
import pytest

from ray import ray_constants
from ray.test_utils import wait_for_condition, relevant_errors

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "_internal_config": json.dumps({
            "agent_register_timeout_ms": 2000
        })
    }],
    indirect=True)
def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    all_processes = ray.worker._global_node.all_processes
    assert ray_constants.PROCESS_TYPE_DASHBOARD in all_processes
    assert ray_constants.PROCESS_TYPE_REPORTER not in all_processes
    dashboard_proc_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
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
    agent_proc = _search_agent(raylet_proc.children())
    assert agent_proc is not None
    agent_proc.kill()
    agent_proc.wait()
    assert _search_agent(raylet_proc.children()) is None

    wait_for_condition(lambda: _search_agent(raylet_proc.children()))
    dashboard_died_error = relevant_errors(ray_constants.DASHBOARD_DIED_ERROR)
    assert not dashboard_died_error
    assert dashboard_proc.status() == psutil.STATUS_RUNNING
    agent_proc = _search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    # Check if agent register OK.
    time.sleep(3)
    agent_proc = _search_agent(raylet_proc.children())
    assert agent_proc.pid == agent_pid
