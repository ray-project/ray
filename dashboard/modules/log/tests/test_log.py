import sys
import logging
import requests
import time
import traceback
import html.parser
import urllib.parse
import json
import os

from ray.dashboard.tests.conftest import *  # noqa
import pytest
import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray.dashboard.modules.log.log_agent import tail as tail_file

logger = logging.getLogger(__name__)


class LogUrlParser(html.parser.HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._urls = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._urls.append(dict(attrs)["href"])

    def error(self, message):
        logger.error(message)

    def get_urls(self):
        return self._urls


def test_log(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    def write_log(s):
        print(s)

    test_log_text = "test_log_text"
    ray.get(write_log.remote(test_log_text))
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/log_index")
            response.raise_for_status()
            parser = LogUrlParser()
            parser.feed(response.text)
            all_nodes_log_urls = parser.get_urls()
            assert len(all_nodes_log_urls) == 1

            response = requests.get(all_nodes_log_urls[0])
            response.raise_for_status()
            parser = LogUrlParser()
            parser.feed(response.text)

            # Search test_log_text from all worker logs.
            parsed_url = urllib.parse.urlparse(all_nodes_log_urls[0])
            paths = parser.get_urls()
            urls = []
            for p in paths:
                if "worker" in p:
                    urls.append(parsed_url._replace(path=p).geturl())

            for u in urls:
                response = requests.get(u)
                response.raise_for_status()
                if test_log_text in response.text:
                    break
            else:
                raise Exception(f"Can't find {test_log_text} from {urls}")

            # Test range request.
            response = requests.get(
                webui_url + "/logs/dashboard.log", headers={"Range": "bytes=44-52"}
            )
            response.raise_for_status()
            assert response.text == "Dashboard"

            # Test logUrl in node info.
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            node_info = response.json()
            assert node_info["result"] is True
            node_info = node_info["data"]["detail"]
            assert "logUrl" in node_info
            assert node_info["logUrl"] in all_nodes_log_urls
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_log_proxy(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            # Test range request.
            response = requests.get(
                f"{webui_url}/log_proxy?url={webui_url}/logs/dashboard.log",
                headers={"Range": "bytes=44-52"},
            )
            response.raise_for_status()
            assert response.text == "Dashboard"
            # Test 404.
            response = requests.get(
                f"{webui_url}/log_proxy?" f"url={webui_url}/logs/not_exist_file.log"
            )
            assert response.status_code == 404
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_logs_experimental_list(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            # Test that logs/list is comprehensive
            response = requests.get(webui_url + "/api/experimental/logs/list")
            response.raise_for_status()
            logs = json.loads(response.text)
            assert len(logs) == 1
            node_id = next(iter(logs))

            # test worker logs
            outs = logs[node_id]["worker_outs"]
            errs = logs[node_id]["worker_outs"]
            core_worker_logs = logs[node_id]["python_core_worker_logs"]

            assert len(outs) == len(errs) == len(core_worker_logs)
            assert len(outs) > 0

            for file in ["debug_state_gcs.txt", "gcs_server.out", "gcs_server.err"]:
                assert file in logs[node_id]["gcs_server"]
            for file in ["raylet.out", "raylet.err"]:
                assert file in logs[node_id]["raylet"]
            for file in ["dashboard_agent.log", "dashboard.log"]:
                assert file in logs[node_id]["dashboard"]
            break

            # Test that logs/list can be filtered
            response = requests.get(
                webui_url + "/api/experimental/logs/list?filters=gcs"
            )
            response.raise_for_status()
            logs = json.loads(response.text)
            assert len(logs) == 1
            node_id = next(iter(logs))
            assert "gcs_server" in logs[node_id] and len(logs[node_id]) == 1

            response = requests.get(
                webui_url + "/api/experimental/logs/list?filters=worker"
            )
            response.raise_for_status()
            logs = json.loads(response.text)
            assert len(logs) == 1
            node_id = next(iter(logs))
            worker_log_categories = [
                "python_core_worker_logs",
                "worker_outs",
                "worker_errors",
            ]
            assert (
                all([cat in logs[node_id] for cat in worker_log_categories])
                and len(logs[node_id]) == 3
            )
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_logs_tail():
    """
    Unit test for tail
    """
    TOTAL_LINES = 1000
    FILE_NAME = "test_file.txt"
    try:
        with open(FILE_NAME, "w") as f:
            for i in range(TOTAL_LINES):
                f.write(f"Message {i:4}\n")
        file = open(FILE_NAME, "rb")
        text, byte_pos = tail_file(file, 100)
        assert byte_pos == TOTAL_LINES * len(b"Message 1000\n")
        lines = text.decode("utf-8").split("\n")
        assert len(lines) == 100
        assert lines[0] == "Message  900"
        assert lines[99] == "Message  999"
    except Exception as e:
        raise e
    finally:
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)


def test_logs_experimental_write(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def write_log(self, strings):
            for s in strings:
                print(s)

    test_log_text = "test_log_text{}"
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None

    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/api/experimental/logs/list")
            response.raise_for_status()
            logs = json.loads(response.text)
            assert len(logs) == 1
            node_id = next(iter(logs))

            actor = Actor.remote()
            ray.get(actor.write_log.remote([test_log_text.format("XXXXXX")]))

            # Test stream and fetching by actor id
            stream_response = requests.get(
                webui_url
                + f"/api/experimental/logs/stream?node_id={node_id}&lines=2"
                + "&actor_id="
                + actor._ray_actor_id.hex(),
                stream=True,
            )
            if stream_response.status_code != 200:
                raise ValueError(stream_response.text)
            stream_iterator = stream_response.iter_content(chunk_size=None)
            assert (
                next(stream_iterator).decode("utf-8")
                == ":actor_name:Actor\n" + test_log_text.format("XXXXXX") + "\n"
            )

            streamed_string = ""
            for i in range(5):
                strings = []
                for j in range(100):
                    strings.append(test_log_text.format(f"{100*i + j:06d}"))

                ray.get(actor.write_log.remote(strings))

                string = ""
                for s in strings:
                    string += s + "\n"
                assert next(stream_iterator).decode("utf-8") == string
                streamed_string += string
            del stream_response

            # Test tailing log by actor id
            LINES = 150
            file_response = requests.get(
                webui_url
                + f"/api/experimental/logs/file?node_id={node_id}&lines={LINES}"
                + "&actor_id="
                + actor._ray_actor_id.hex(),
            ).text
            assert file_response == "\n".join(
                streamed_string.split("\n")[-(LINES + 1) :]
            )
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_logs_grpc_client_termination(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    time.sleep(1)
    # Get dashboard agent log
    RAYLET_FILE_NAME = "raylet.out"
    DASHBOARD_AGENT_FILE_NAME = "dashboard_agent.log"
    stream_response = requests.get(
        webui_url
        + f"/api/experimental/logs/stream?node_id={node_id}"
        + f"&lines=0&log_file_name={RAYLET_FILE_NAME}",
        stream=True,
    )
    if stream_response.status_code != 200:
        raise ValueError(stream_response.text)
    # give enough time for the initiation message to be written to the log
    time.sleep(1)

    file_response = requests.get(
        webui_url
        + f"/api/experimental/logs/file?node_id={node_id}"
        + f"&lines=10&log_file_name={DASHBOARD_AGENT_FILE_NAME}",
    )

    # Check that gRPC stream initiated as a result of starting the stream
    assert f'initiated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
    "\nkeep_alive: true" in file_response.text
    # Check that gRPC stream has not terminated (is kept alive)
    assert f'terminated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
    "\nkeep_alive: true" not in file_response.text

    del stream_response
    # give enough time for the termination message to be written to the log
    time.sleep(1)

    file_response = requests.get(
        webui_url
        + f"/api/experimental/logs/file?node_id={node_id}"
        + f"&lines=10&log_file_name={DASHBOARD_AGENT_FILE_NAME}",
    )

    # Check that gRPC terminated as a result of closing the stream
    assert f'terminated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
    "\nkeep_alive: true" in file_response.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
