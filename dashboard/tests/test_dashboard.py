import os
import json
import time
import logging

import ray
import psutil
import pytest
import redis
import requests

from ray import ray_constants
from ray.test_utils import wait_for_condition, wait_until_server_available
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.modules

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def cleanup_test_files():
    module_path = ray.new_dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Remove test file: %s", filename)
    try:
        os.remove(filename)
    except Exception:
        pass


def prepare_test_files():
    module_path = ray.new_dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Prepare test file: %s", filename)
    with open(filename, "w") as f:
        f.write(">>>")


cleanup_test_files()


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
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    address_info = ray_start_with_dashboard
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    client = redis.StrictRedis(
        host=address[0],
        port=int(address[1]),
        password=ray_constants.REDIS_DEFAULT_PASSWORD)

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
            try:
                for c in p.cmdline():
                    if "new_dashboard/agent.py" in c:
                        return p
            except Exception:
                pass

    # Test for bad imports, the agent should be restarted.
    logger.info("Test for bad imports.")
    agent_proc = _search_agent(raylet_proc.children())
    prepare_test_files()
    agent_pids = set()
    try:
        assert agent_proc is not None
        agent_proc.kill()
        agent_proc.wait()
        # The agent will be restarted for imports failure.
        for x in range(40):
            agent_proc = _search_agent(raylet_proc.children())
            if agent_proc:
                agent_pids.add(agent_proc.pid)
            time.sleep(0.1)
    finally:
        cleanup_test_files()
    assert len(agent_pids) > 1, agent_pids

    agent_proc = _search_agent(raylet_proc.children())
    if agent_proc:
        agent_proc.kill()
        agent_proc.wait()

    logger.info("Test agent register is OK.")
    wait_for_condition(lambda: _search_agent(raylet_proc.children()))
    assert dashboard_proc.status() == psutil.STATUS_RUNNING
    agent_proc = _search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = _search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)

    # Check redis keys are set.
    logger.info("Check redis keys are set.")
    dashboard_address = client.get(dashboard_consts.REDIS_KEY_DASHBOARD)
    assert dashboard_address is not None
    dashboard_rpc_address = client.get(
        dashboard_consts.REDIS_KEY_DASHBOARD_RPC)
    assert dashboard_rpc_address is not None
    key = "{}{}".format(dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX,
                        address[0])
    agent_ports = client.get(key)
    assert agent_ports is not None


def test_nodes_update(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")

    timeout_seconds = 20
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/test/dump")
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: {}".format(response.text))
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["nodes"]) == 1
            assert len(dump_data["agents"]) == 1
            assert len(dump_data["hostnameToIp"]) == 1
            assert len(dump_data["ipToHostname"]) == 1
            assert dump_data["nodes"].keys() == dump_data[
                "ipToHostname"].keys()

            response = requests.get(webui_url + "/test/notified_agents")
            response.raise_for_status()
            try:
                notified_agents = response.json()
            except Exception as ex:
                logger.info("failed response: {}".format(response.text))
                raise ex
            assert notified_agents["result"] is True
            notified_agents = notified_agents["data"]
            assert len(notified_agents) == 1
            assert notified_agents == dump_data["agents"]
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")


def test_http_get(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")

    target_url = webui_url + "/test/dump"

    timeout_seconds = 20
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/test/http_get?url=" +
                                    target_url)
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: {}".format(response.text))
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["agents"]) == 1
            ip, ports = next(iter(dump_data["agents"].items()))
            http_port, grpc_port = ports

            response = requests.get(
                "http://{}:{}/test/http_get_from_agent?url={}".format(
                    ip, http_port, target_url))
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: {}".format(response.text))
                raise ex
            assert dump_info["result"] is True
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")
