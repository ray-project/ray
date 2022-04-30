import requests
import json
from typing import List

import ray._private.services as services
import ray.ray_constants as ray_constants
from ray.experimental.log.common import (
    NodeIdentifiers,
    FileIdentifiers,
    LogStreamOptions
)

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
    lines: int = 100,
    interval: float = 0.5,
    api_server_url: str = None,
):
    """
    Streams the log in real time starting from `lines` number of lines from
    the end of the file if `stream == True`. Else, it terminates the
    stream once there are no more bytes to read from the log file.

    Raises a ValueError if the identifying input params provided cannot match
    a single log file in the cluster.
    Returns a generator yielding strings. Note that this API is blocking.

    Args:
        file_identifiers: FileIdentifiers, a dataclass with the fields:
            log_file_name, pid, actor_id, task_id
        node_identifiers: NodeIdentifiers, a dataclass with the fields:
            node_id, node_ip
        stream: whether to continue to stream the log file as it is updated.
        lines (optional): number of lines to tail from the file. Indicates the initial
            number of lines tailed if streaming.
        interval (optional): how frequently to read new lines from the file if streaming
        api_server_url (optional): the URL for the desired cluster's API server

    """
    api_server_url = api_server_url or _get_dashboard_url()
    file_identifiers.task_id is not None:
        raise NotImplementedError(
            "querying for logs by`task_id` is not yet implemented"
        )

    query_strings = []
    for field in fields(NodeIdentifiers):
        query_strings.append(f"{field.name}={getattr(node_identifiers, field.name)}")
    for field in fields(FileIdentifiers):
        query_strings.append(f"{field.name}={getattr(file_identifiers, field.name)}")

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
    Returns a JSON file containing, for each node in the cluster,
    a dict mapping a category of log component to a list of filenames.
    If a node_ip or node_id is provided, only that node's logs will be
    returned.

    Args:
        node_identifiers: NodeIdentifiers, a dataclass with the fields:
            node_id, node_ip
        filters: a list of strings to match against each filename.

    If the given node_identifiers cannot be resolved, it will throw a
    ValueError.
    """
    api_server_url = api_server_url or _get_dashboard_url()

    query_strings = []
    for field in fields(NodeIdentifiers):
        query_strings.append(f"{field.name}={getattr(node_identifiers, field.name)}")

    response = requests.get(
        f"{api_server_url}/api/experimental/logs/list?{'&'.join(query_strings)}"
        f"filters={','.join(filters)}"
    )
    if response.status_code != 200:
        raise ValueError(response.text)
    logs_dict = json.loads(response.text)
    return logs_dict


def pretty_print_logs_index(node_id: str, files: List[str]):
    def print_section(name, key):
        if len(files[key]) > 0:
            print(f"\n{name}")
            print("----------------------------")
            [
                print(
                    f"{_get_dashboard_url()}/api/experimental/logs/file"
                    f"?node_id={node_id}&log_file_name={log}"
                )
                for log in files[key]
            ]

    for lang in ray_constants.LANGUAGE_WORKER_TYPES:
        print_section(f"{lang.capitalize()} Core Driver Logs", f"{lang}_driver_logs")
        print_section(
            f"{lang.capitalize()} Core Worker Logs", f"{lang}_core_worker_logs"
        )
    print_section("Worker Errors", "worker_errors")
    print_section("Worker Stdout", "worker_outs")
    print_section("Raylet Logs", "raylet")
    print_section("GCS Logs", "gcs_server")
    print_section("Miscellaneous Logs", "misc")
    print_section("Autoscaler Monitor Logs", "autoscaler")
    print_section("Runtime Environment Logs", "runtime_env")
    print_section("Dashboard Logs", "dashboard")
    print_section("Ray Client Logs", "ray_client")
