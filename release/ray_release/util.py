import collections
import hashlib
import json
import os
import subprocess
import time
from typing import Dict, Any

import requests
from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

from ray_release.logger import logger

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
    return requests.head(url, allow_redirects=True).status_code == 200


def format_link(link: str):
    # Use ANSI escape code to allow link to be clickable
    # https://buildkite.com/docs/pipelines/links-and-images
    # -in-log-output
    if os.environ.get("BUILDKITE_COMMIT"):
        return "\033]1339;url='" + link + "'\a\n"
    # Else, no buildkite:
    return link


def anyscale_project_url(project_id: str):
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/projects/{project_id}"
        f"/?tab=session-list"
    )


def anyscale_cluster_url(project_id: str, session_id: str):
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/projects/{project_id}"
        f"/clusters/{session_id}"
    )


def anyscale_cluster_compute_url(compute_tpl_id: str):
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/configurations/cluster-computes"
        f"/{compute_tpl_id}"
    )


def anyscale_cluster_env_build_url(build_id: str):
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/configurations/app-config-details"
        f"/{build_id}"
    )


_anyscale_sdk = None


def get_anyscale_sdk() -> AnyscaleSDK:
    global _anyscale_sdk
    if _anyscale_sdk:
        return _anyscale_sdk

    _anyscale_sdk = AnyscaleSDK()
    return _anyscale_sdk


def exponential_backoff_retry(f, retry_exceptions, initial_retry_delay_s, max_retries):
    retry_cnt = 0
    retry_delay_s = initial_retry_delay_s
    while True:
        try:
            return f()
        except retry_exceptions as e:
            retry_cnt += 1
            if retry_cnt > max_retries:
                raise
            logger.info(
                f"Retry function call failed due to {e} "
                f"in {retry_delay_s} seconds..."
            )
            time.sleep(retry_delay_s)
            retry_delay_s *= 2


def run_bash_script(bash_script: str):
    subprocess.run(f"bash {bash_script}", shell=True, check=True)
