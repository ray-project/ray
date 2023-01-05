import collections
import hashlib
import gzip
import json
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import requests
from ray_release.logger import logger

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class DeferredEnvVar:
    def __init__(self, var: str, default: Optional[str] = None):
        self._var = var
        self._default = default

    def __str__(self):
        return os.environ.get(self._var, self._default)


ANYSCALE_HOST = DeferredEnvVar("ANYSCALE_HOST", "https://console.anyscale.com")


def deep_update(d, u) -> Dict:
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


def url_exists(url: str) -> bool:
    try:
        return requests.head(url, allow_redirects=True).status_code == 200
    except requests.exceptions.RequestException:
        logger.exception(f"Failed to check url exists: {url}")
        return False


def resolve_url(url: str) -> str:
    return requests.head(url, allow_redirects=True).url


def format_link(link: str) -> str:
    # Use ANSI escape code to allow link to be clickable
    # https://buildkite.com/docs/pipelines/links-and-images
    # -in-log-output
    if os.environ.get("BUILDKITE_COMMIT"):
        return "\033]1339;url='" + link + "'\a\n"
    # Else, no buildkite:
    return link


def anyscale_project_url(project_id: str) -> str:
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/projects/{project_id}"
        f"/?tab=session-list"
    )


def anyscale_cluster_url(project_id: str, cluster_id: str) -> str:
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/projects/{project_id}"
        f"/clusters/{cluster_id}"
    )


def anyscale_cluster_compute_url(compute_tpl_id: str) -> str:
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/configurations/cluster-computes"
        f"/{compute_tpl_id}"
    )


def anyscale_cluster_env_build_url(build_id: str) -> str:
    return (
        f"{ANYSCALE_HOST}"
        f"/o/anyscale-internal/configurations/app-config-details"
        f"/{build_id}"
    )


_anyscale_sdk = None


def get_anyscale_sdk(use_cache: bool = True) -> "AnyscaleSDK":
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

    global _anyscale_sdk
    if use_cache and _anyscale_sdk:
        return _anyscale_sdk

    _anyscale_sdk = AnyscaleSDK(host=str(ANYSCALE_HOST))
    return _anyscale_sdk


def exponential_backoff_retry(
    f, retry_exceptions, initial_retry_delay_s, max_retries
) -> None:
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


def run_bash_script(bash_script: str) -> None:
    subprocess.run(f"bash {bash_script}", shell=True, check=True)


def reinstall_anyscale_dependencies() -> None:
    logger.info("Re-installing `anyscale` package")

    # Copy anyscale pin to requirements.txt and requirements_buildkite.txt
    subprocess.check_output(
        "pip install -U anyscale==0.5.51",
        shell=True,
        text=True,
    )


def get_pip_packages() -> List[str]:
    from pip._internal.operations import freeze

    return list(freeze.freeze())


def python_version_str(python_version: Tuple[int, int]) -> str:
    """From (X, Y) to XY"""
    return "".join([str(x) for x in python_version])


# Duplicated in ray_release.command_runner._prometheus_metrics
def write_json(
    data: Dict[str, Any], path: Union[str, Path], **json_dump_kwargs
) -> Path:
    """Supports both .gz and .json files."""
    pathlib_path = Path(path)
    if pathlib_path.suffix == ".gz":
        with gzip.open(pathlib_path, "w") as f:
            f.write(json.dumps(data, **json_dump_kwargs).encode("utf-8"))
    else:
        with open(pathlib_path, "wt") as f:
            json.dump(data, f, **json_dump_kwargs)
    return pathlib_path


def _read_gzip_json(path: str, **json_load_kwargs) -> Dict[str, Any]:
    with gzip.open(path, "r") as f:
        return json.loads(f.read().decode("utf-8"), **json_load_kwargs)


def read_json(path: Union[str, Path], **json_load_kwargs) -> Dict[str, Any]:
    """Supports both .gz and .json files."""
    pathlib_path = Path(path)
    if pathlib_path.suffix == ".gz":
        data = _read_gzip_json(pathlib_path, **json_load_kwargs)
    else:
        try:
            with open(pathlib_path, "rt") as f:
                data = json.load(f, **json_load_kwargs)
        except UnicodeDecodeError:
            # If we fail due to UnicodeDecodeError, maybe this is a
            # gzip
            data = _read_gzip_json(pathlib_path, **json_load_kwargs)
    return data
