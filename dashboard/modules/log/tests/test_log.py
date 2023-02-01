import html.parser
import logging
import sys
import time
import traceback
import urllib.parse

import pytest
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


class LogUrlParser(html.parser.HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._urls = []
        self._texts = set()
        self._capture_text = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._urls.append(dict(attrs)["href"])
        if tag == "li":
            self._capture_text = True

    def handle_endtag(self, tag):
        if tag == "li":
            self._capture_text = False

    def handle_data(self, data: str) -> None:
        if self._capture_text:
            self._texts.add(data)

    def error(self, message):
        logger.error(message)

    def get_urls(self):
        return self._urls

    def get_texts(self):
        return self._texts


def test_log(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    def write_log(s):
        print(s)

    # Make sure that this works with unicode
    test_log_text = "test_log_text"
    ray.get(write_log.remote(test_log_text))

    test_file = "test.log"
    with open(
        f"{ray._private.worker.global_worker.node.get_logs_dir_path()}/{test_file}", "w"
    ) as f:
        f.write(test_log_text)
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
                if test_log_text in response.content.decode(encoding="utf-8"):
                    break
            else:
                raise Exception(f"Can't find {test_log_text} from {urls}")

            # Test range request.
            response = requests.get(
                webui_url + f"/logs/{test_file}", headers={"Range": "bytes=2-5"}
            )
            response.raise_for_status()
            assert response.text == test_log_text[2:6]

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


@pytest.mark.parametrize(
    "test_file",
    ["test.log", "test#1234.log"],
)
def test_log_proxy(ray_start_with_dashboard, test_file):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    test_log_text = "test_log_text"
    with open(
        f"{ray._private.worker.global_worker.node.get_logs_dir_path()}/{test_file}", "w"
    ) as f:
        f.write(test_log_text)
    while True:
        time.sleep(1)
        try:
            url = urllib.parse.quote(f"{webui_url}/logs/{test_file}")
            # Test range request.
            response = requests.get(
                f"{webui_url}/log_proxy?url={url}",
                headers={"Range": "bytes=2-5"},
            )
            response.raise_for_status()
            assert response.text == test_log_text[2:6]
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


@pytest.mark.parametrize(
    "ray_start_cluster", [{"include_dashboard": True, "num_nodes": 2}], indirect=True
)
def test_log_index_texts(disable_aiohttp_cache, ray_start_cluster):
    cluster = ray_start_cluster
    webui_url = cluster.head_node.webui_url
    assert wait_until_server_available(webui_url) is True
    webui_url = format_web_url(webui_url)

    # Check nodes ready
    def _check_two_nodes_ready():
        try:
            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status
            result = response.json()
            nodes = result["data"]["summary"]
            assert len(nodes) == 2
            return True
        except Exception:
            logger.exception("Node number check failed:")

    wait_for_condition(_check_two_nodes_ready)

    def _get_node_id_ip_pairs():
        result = requests.get(webui_url + "/nodes?view=summary").json()
        nodes = result["data"]["summary"]
        node_id_ip_pairs = []
        for node in nodes:
            node_id_ip_pairs.append(
                (node["raylet"]["nodeId"], node["raylet"]["nodeManagerAddress"])
            )
        return node_id_ip_pairs

    node_id_ip_pairs = _get_node_id_ip_pairs()
    expected_texts = set()
    for node_id, node_ip in node_id_ip_pairs:
        expected_texts.add("Node ID: {} (IP: {})".format(node_id, node_ip))

    # Check log index format
    def check_log_index_format():
        response = requests.get(webui_url + "/log_index")
        response.raise_for_status()
        text = response.text
        parser = LogUrlParser()
        parser.feed(text)
        texts = parser.get_texts()
        assert expected_texts == texts

    check_log_index_format()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
