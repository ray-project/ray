import requests
import json
from typing import List, Dict

import ray._private.services as services
from ray.experimental.log.common import (
    NodeIdentifiers,
    FileIdentifiers,
)
from ray.experimental.log.consts import (
    RAY_LOG_CATEGORIES,
    RAY_WORKER_LOG_CATEGORIES,
)
from dataclasses import fields

SESSION_DASHBOARD_URL = None


def _get_dashboard_url():
    global SESSION_DASHBOARD_URL

    if SESSION_DASHBOARD_URL is None:
        gcs_addr = services.get_ray_address_from_environment()
        dashboard_addr = services.get_dashboard_url(gcs_addr)
        if not dashboard_addr:
            return f"Cannot find the dashboard of the cluster of address {gcs_addr}."

        def format_web_url(url):
            """Format web url."""
            url = url.replace("localhost", "http://127.0.0.1")
            if not url.startswith("http://"):
                return "http://" + url
            return url

        SESSION_DASHBOARD_URL = format_web_url(dashboard_addr)
    return SESSION_DASHBOARD_URL


def get_log(
    file_identifiers: FileIdentifiers,
    node_identifiers: NodeIdentifiers,
    stream: bool = False,
    lines: int = 100,
    interval: float = 0.5,
    api_server_url: str = None,
):
    """
    Gets a log file from the cluster using the provided identifiers.

    Streams the log in real time starting from `lines` number of lines from
    the end of the file if `stream == True`. Else, it terminates the
    stream once there are no more bytes to read from the log file.

    Raises a ValueError if the identifying input params provided cannot match
    a single log file in the cluster.

    Args:
        file_identifiers: FileIdentifiers, a dataclass with the optional fields:
            log_file_name, pid, actor_id, task_id
        node_identifiers: NodeIdentifiers, a dataclass with the optional fields:
            node_id, node_ip
        stream (optional): whether to stream the log file in real time.
        lines (optional): number of lines to tail from the file. Indicates the initial
            number of lines tailed if streaming.
        interval (optional): how frequently to read new lines from the file if streaming
        api_server_url (optional): the URL for the desired cluster's API server
    Returns:
        A generator yielding strings. Note that this API is blocking.

    """
    api_server_url = api_server_url or _get_dashboard_url()
    if file_identifiers.task_id is not None:
        raise NotImplementedError(
            "querying for logs by`task_id` is not yet implemented"
        )

    query_strings = []
    for field in fields(NodeIdentifiers):
        value = getattr(node_identifiers, field.name)
        if value:
            query_strings.append(f"{field.name}={value}")
    for field in fields(FileIdentifiers):
        value = getattr(file_identifiers, field.name)
        if value:
            query_strings.append(f"{field.name}={value}")

    if stream:
        media_type = "stream"
    else:
        media_type = "file"
    with requests.get(
        f"{api_server_url}/api/experimental/logs/{media_type}?"
        f"{'&'.join(query_strings)}&lines={lines}&interval={interval}",
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise ValueError(r.text)
        for bytes in r.iter_content(chunk_size=None):
            yield bytes.decode("utf-8")


def list_logs(
    node_identifiers: NodeIdentifiers,
    filters: List[str],
    api_server_url: str = None,
):
    """
    Lists the logs in the Ray cluster or the given node.

    Args:
        node_identifiers: NodeIdentifiers, a dataclass with the optional fields:
            node_id, node_ip
        filters: a list of strings to match against each filename.
    Returns:
        JSON {node_id: str -> {category: str -> List[filename]}}

    If the given node_identifiers cannot be resolved, it will throw a
    ValueError.
    """
    api_server_url = api_server_url or _get_dashboard_url()

    query_strings = []
    for field in fields(NodeIdentifiers):
        value = getattr(node_identifiers, field.name)
        if value:
            query_strings.append(f"{field.name}={value}")

    response = requests.get(
        f"{api_server_url}/api/experimental/logs/list?{'&'.join(query_strings)}"
        f"&filters={','.join(filters)}"
    )
    if response.status_code != 200:
        raise ValueError(response.text)
    logs_dict = json.loads(response.text)
    return logs_dict


def pretty_print_logs_index(node_id: str, files: Dict[str, List[str]]):
    """
    Pretty prints the logs by category.

    Args:
        node_id: the node_id for the files
        files: {category: str -> filenames: List[str]}
    """

    def print_section(key):
        if len(files[key]) > 0:
            print(f"\n{key}")
            print("----------------------------")
            [
                print(
                    f"{_get_dashboard_url()}/api/experimental/logs/file"
                    f"?node_id={node_id}&log_file_name={log}"
                )
                for log in files[key]
            ]

    for category in RAY_LOG_CATEGORIES:
        print_section(category)
    for category in RAY_WORKER_LOG_CATEGORIES:
        print_section(category)
