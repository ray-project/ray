import os
import logging
import requests
import socket
import time
import traceback
import html.parser
import urllib.parse

import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    wait_until_server_available, )

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

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


def test_log(disable_test_module, ray_start_with_dashboard):
    @ray.remote
    def write_log(s):
        print(s)

    test_log_text = "test_log_text"
    ray.get(write_log.remote(test_log_text))
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")

    timeout_seconds = 20
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
                raise Exception("Can't find {} from {}".format(
                    test_log_text, urls))

            # Test range request.
            response = requests.get(
                webui_url + "/logs/dashboard.log",
                headers={"Range": "bytes=43-51"})
            response.raise_for_status()
            assert response.text == "Dashboard"

            # Test logUrl in node info.
            response = requests.get(webui_url +
                                    "/nodes/{}".format(socket.gethostname()))
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
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                raise Exception(
                    "Timed out while waiting for dashboard to start, {}".
                    format("".join(ex_stack)))
