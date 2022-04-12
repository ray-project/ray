import requests
import json
import ray._private.services as services

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
    node_ip: str = None,
    pid: str = None,
    node_id: str = None,
    actor_id: str = None,
    task_id: str = None,
    log_file_name: str = None,
    stream: bool = False,
    lines: int = 1000,
    interval: float = 0.5,
    api_server_url: str = None,
):
    """
    Streams the log in real time starting from `lines` number of lines from
    the end of the file if `stream == True`. Else, it terminates the
    stream once there are no more bytes to read from the log file.

    Returns a generator yielding strings. Note that this API is blocking.

    Raises a ValueError if the identifying input params provided cannot match
    a single log file in the cluster.
    """
    if api_server_url is None:
        api_server_url = _get_dashboard_url()
    if task_id is not None:
        raise NotImplementedError(
            "querying for logs by`task_id` is not yet implemented")

    query_string = ""
    args = {
        "node_id": node_id,
        "node_ip": node_ip,
        "actor_id": actor_id,
        "pid": pid,
        "log_file_name": log_file_name,
    }
    for arg in args:
        if args[arg] is not None:
            query_string += f"{arg}={args[arg]}&"

    if stream:
        media_type = "stream"
    else:
        media_type = "file"
    with requests.get(
        f"{api_server_url}/api/experimental/logs/{media_type}?"
        f"{query_string}lines={lines}",
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise ValueError(r.text)
        for bytes in r.iter_content(chunk_size=None):
            yield bytes.decode("utf-8")


def list_logs(
    node_id: str,
    node_ip: str,
    filters: str,
    api_server_url: str = None,
):
    """
    Returns a JSON file containing, for each node in the cluster,
    a dict mapping a category of log component to a list of filenames.
    If a node_ip or node_id is provided, only that node's index will be
    returned.

    filters: a list of strings to match against each filename.

    If the given node_ip or node_ip cannot be found, it will throw a
    ValueError.
    """

    if api_server_url is None:
        api_server_url = _get_dashboard_url()
    query_string = ""
    args = {
        "node_id": node_id,
        "node_ip": node_ip,
    }
    for arg in args:
        if args[arg] is not None:
            query_string += f"{arg}={args[arg]}&"

    response = requests.get(
        f"{api_server_url}/api/experimental/logs/index?{query_string}"
        f"filters={filters}"
    )
    if response.status_code != 200:
        raise ValueError(response.text)
    logs_dict = json.loads(response.text)
    return api_server_url, logs_dict
