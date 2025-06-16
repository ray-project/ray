import collections
import hashlib
import json
import os
import random
import string
import subprocess
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from google.cloud import storage
import requests
import shutil

from ray_release.logger import logger
from ray_release.configs.global_config import get_global_config
from ray_release.exception import ClusterEnvCreateError

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class DeferredEnvVar:
    def __init__(self, var: str, default: Optional[str] = None):
        self._var = var
        self._default = default

    def __str__(self):
        return os.environ.get(self._var, self._default)


ANYSCALE_HOST = DeferredEnvVar("ANYSCALE_HOST", "https://console.anyscale.com")
S3_CLOUD_STORAGE = "s3"
GS_CLOUD_STORAGE = "gs"
GS_BUCKET = "anyscale-oss-dev-bucket"
ERROR_LOG_PATTERNS = [
    "ERROR",
    "Traceback (most recent call last)",
]
KUBERAY_SERVER_URL = "https://kuberaytest.anyscale.dev"
DEFAULT_KUBERAY_NAMESPACE = "kuberayportal-kevin"


def get_read_state_machine_aws_bucket(allow_pr_bucket: bool = False) -> str:
    # We support by default reading from the branch bucket only, since most of the use
    # cases are on branch pipelines. Changing the default flag to read from the bucket
    # according to the current pipeline
    if allow_pr_bucket:
        return get_write_state_machine_aws_bucket()
    return get_global_config()["state_machine_branch_aws_bucket"]


def get_write_state_machine_aws_bucket() -> str:
    # We support different buckets for writing test result data; one for pr and one for
    # branch. This is because pr and branch pipeline have different permissions, and we
    # want data on branch pipeline being protected.
    pipeline_id = os.environ.get("BUILDKITE_PIPELINE_ID")
    pr_pipelines = get_global_config()["ci_pipeline_premerge"]
    branch_pipelines = get_global_config()["ci_pipeline_postmerge"]
    assert pipeline_id in pr_pipelines + branch_pipelines, (
        "Test state machine is only supported for branch or pr pipeline, "
        f"{pipeline_id} is given"
    )
    if pipeline_id in pr_pipelines:
        return get_global_config()["state_machine_pr_aws_bucket"]
    return get_global_config()["state_machine_branch_aws_bucket"]


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
    if os.environ.get("BUILDKITE_COMMIT") and link:
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


def anyscale_job_url(job_id: str) -> str:
    return f"{ANYSCALE_HOST}/o/anyscale-internal/jobs/{job_id}"


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
            logger.exception(
                f"Retry function call failed due to {e} "
                f"in {retry_delay_s} seconds..."
            )
            time.sleep(retry_delay_s)
            retry_delay_s *= 2


def run_bash_script(bash_script: str) -> None:
    subprocess.run(f"bash {bash_script}", shell=True, check=True)


def reinstall_anyscale_dependencies() -> None:
    logger.info("Re-installing `anyscale` package")
    subprocess.check_output(
        "pip install -U anyscale",
        shell=True,
        text=True,
    )


def get_pip_packages() -> List[str]:
    from pip._internal.operations import freeze

    return list(freeze.freeze())


def python_version_str(python_version: Tuple[int, int]) -> str:
    """From (X, Y) to XY"""
    return "".join([str(x) for x in python_version])


def generate_tmp_cloud_storage_path() -> str:
    return "".join(random.choice(string.ascii_lowercase) for i in range(10))


def join_cloud_storage_paths(*paths: str):
    paths = list(paths)
    if len(paths) > 1:
        for i in range(1, len(paths)):
            while paths[i][0] == "/":
                paths[i] = paths[i][1:]
    joined_path = os.path.join(*paths)
    while joined_path[-1] == "/":
        joined_path = joined_path[:-1]
    return joined_path


def upload_working_dir(working_dir: str) -> str:
    """Upload working directory to GCS bucket.

    Args:
        working_dir: Path to directory to upload.
    Returns:
        GCS path where directory was uploaded.
    """
    # Create archive of working dir
    timestamp = str(int(time.time()))
    archived_filename = f"ray_release_{timestamp}.zip"
    output_path = os.path.abspath(archived_filename)

    logger.info(f"Archiving working directory: {working_dir}")
    shutil.make_archive(output_path[:-4], "zip", working_dir)

    # Upload to GCS
    gcs_client = storage.Client()
    bucket = gcs_client.bucket("ray-release-working-dir")
    blob = bucket.blob(archived_filename)
    blob.upload_from_filename(archived_filename)

    return f"gs://ray-release-working-dir/{blob.name}"


def get_custom_cluster_env_name(image: str, test_name: str) -> str:
    image_normalized = image.replace("/", "_").replace(":", "_").replace(".", "_")
    return f"test_env_{image_normalized}_{test_name}"


def create_cluster_env_from_image(
    image: str,
    test_name: str,
    runtime_env: Dict[str, Any],
    sdk: Optional["AnyscaleSDK"] = None,
    cluster_env_id: Optional[str] = None,
    cluster_env_name: Optional[str] = None,
) -> str:
    anyscale_sdk = sdk or get_anyscale_sdk()
    if not cluster_env_name:
        cluster_env_name = get_custom_cluster_env_name(image, test_name)

    # Find whether there is identical cluster env
    paging_token = None
    while not cluster_env_id:
        result = anyscale_sdk.search_cluster_environments(
            dict(
                name=dict(equals=cluster_env_name),
                paging=dict(count=50, paging_token=paging_token),
                project_id=None,
            )
        )
        paging_token = result.metadata.next_paging_token

        for res in result.results:
            if res.name == cluster_env_name:
                cluster_env_id = res.id
                logger.info(f"Cluster env already exists with ID " f"{cluster_env_id}")
                break

        if not paging_token or cluster_env_id:
            break

    if not cluster_env_id:
        logger.info("Cluster env not found. Creating new one.")
        try:
            result = anyscale_sdk.create_byod_cluster_environment(
                dict(
                    name=cluster_env_name,
                    config_json=dict(
                        docker_image=image,
                        ray_version="nightly",
                        env_vars=runtime_env,
                    ),
                )
            )
            cluster_env_id = result.result.id
        except Exception as e:
            logger.warning(
                f"Got exception when trying to create cluster "
                f"env: {e}. Sleeping for 10 seconds with jitter and then "
                f"try again..."
            )
            raise ClusterEnvCreateError("Could not create cluster env.") from e

        logger.info(f"Cluster env created with ID {cluster_env_id}")

    return cluster_env_id
