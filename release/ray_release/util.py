import collections
import hashlib
import json
import os
import threading
import time
from typing import Callable, Dict, Any

import requests
from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

ANYSCALE_HOST = os.environ.get("ANYSCALE_HOST", "https://console.anyscale.com")


def deep_update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def dict_hash(dt: Dict[Any, Any]) -> str:
    json_str = json.dumps(dt, sort_keys=True, ensure_ascii=True)
    sha = hashlib.sha256()
    sha.update(json_str.encode())
    return sha.hexdigest()


def url_exists(url: str):
    return requests.head(url).status_code == 200


def format_link(link: str):
    # Use ANSI escape code to allow link to be clickable
    # https://buildkite.com/docs/pipelines/links-and-images
    # -in-log-output
    if os.environ.get("BUILDKITE_COMMIT"):
        return "\033]1339;url='" + link + "'\a\n"
    # Else, no buildkite:
    return link


def anyscale_project_url(project_id: str):
    return f"{ANYSCALE_HOST}" \
           f"/o/anyscale-internal/projects/{project_id}" \
           f"/?tab=session-list"


def anyscale_session_url(project_id: str, session_id: str):
    return f"{ANYSCALE_HOST}" \
           f"/o/anyscale-internal/projects/{project_id}" \
           f"/clusters/{session_id}"


def anyscale_compute_tpl_url(compute_tpl_id: str):
    return f"{ANYSCALE_HOST}" \
           f"/o/anyscale-internal/configurations/cluster-computes" \
           f"/{compute_tpl_id}"


def anyscale_app_config_build_url(build_id: str):
    return f"{ANYSCALE_HOST}" \
           f"/o/anyscale-internal/configurations/app-config-details" \
           f"/{build_id}"


_anyscale_sdk = None


def get_anyscale_sdk() -> AnyscaleSDK:
    global _anyscale_sdk
    if _anyscale_sdk:
        return _anyscale_sdk

    _anyscale_sdk = AnyscaleSDK()
    return _anyscale_sdk


# Todo: remove to get rid of threading
def run_with_timeout(fn: Callable[[], None],
                     timeout: float,
                     status_fn: Callable[[float], None],
                     error_fn: Callable[[], None],
                     status_interval: float = 30.):
    start_time = time.monotonic()
    next_status = start_time + status_interval
    thread = threading.Thread(target=fn)
    thread.start()

    while thread.is_alive() and time.monotonic() < start_time + timeout:
        if time.monotonic() > next_status:
            next_status += status_interval
            status_fn(time.monotonic() - start_time)
        time.sleep(1)

    if thread.is_alive():
        error_fn()
