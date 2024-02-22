import subprocess
from datetime import datetime
from typing import List, Optional, Callable
import os
import requests
from dateutil import parser

from ci.ray_ci.utils import logger

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


class DockerHubRateLimitException(Exception):
    """
    Exception for Docker Hub rate limit exceeded.
    """

    def __init__(self):
        super().__init__("429: Rate limit exceeded for Docker Hub.")


class RetrieveImageConfigException(Exception):
    """
    Exception for failing to retrieve image config.
    """

    def __init__(self, message: str):
        super().__init__(f"Failed to retrieve {message}")


class AuthTokenException(Exception):
    """
    Exception for failing to retrieve auth token.
    """

    def __init__(self, message: str):
        super().__init__(f"Failed to retrieve auth token from {message}.")


def get_docker_auth_token(namespace: str, repository: str) -> Optional[str]:
    """
    Retrieve Docker Registry token.

    Args:
        namespace: Docker namespace
        repository: Docker repository

    Returns:
        Auth token for Docker.
    """
    service, scope = (
        "registry.docker.io",
        f"repository:{namespace}/{repository}:pull",
    )
    auth_url = f"https://auth.docker.io/token?service={service}&scope={scope}"
    response = requests.get(auth_url)
    if response.status_code != 200:
        raise AuthTokenException(f"Docker. Error code: {response.status_code}")
    token = response.json().get("token", None)
    return token


def get_docker_hub_auth_token():
    """
    Retrieve Docker Hub auth token.
    """
    username = os.environ["DOCKER_HUB_USERNAME"]
    password = os.environ["DOCKER_HUB_PASSWORD"]

    url = "https://hub.docker.com/v2/users/login"
    json_body = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=json_body)
    if response.status_code != 200:
        raise AuthTokenException(f"Docker Hub. Error code: {response.status_code}")
    return response.json().get("token", None)


def _get_git_log(n_days: int = 30):
    return subprocess.check_output(
        [
            "git",
            "log",
            f"--until='{n_days} days ago'",
            "--pretty=format:%H",
        ],
        text=True,
    )


def list_recent_commit_short_shas(n_days: int = 30) -> List[str]:
    """
    Get list of recent commit SHAs (short version, first 6 char) on ray master branch.

    Args:
        n_days: Number of days to go back in git log.

    Returns:
        List of recent commit SHAs (6 char).
    """
    commit_shas = _get_git_log(n_days=n_days)
    short_commit_shas = [
        commit_sha[:6] for commit_sha in commit_shas.split("\n") if commit_sha
    ]
    return short_commit_shas

def get_config_docker_oci(tag: str, namespace: str, repository: str):
    """Get Docker image config from tag using OCI API."""
    token = get_docker_auth_token(namespace=namespace, repository=repository)

    # Pull image manifest to get config digest
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json"
    }
    image_manifest_url = f"https://registry-1.docker.io/v2/{namespace}/{repository}/manifests/{tag}"
    response = requests.get(image_manifest_url, headers=headers)
    if response.status_code != 200:
        raise RetrieveImageConfigException("image manifest.")
    config_blob_digest = response.json()["config"]["digest"]

    # Pull image config
    config_blob_url = f"https://registry-1.docker.io/v2/{namespace}/{repository}/blobs/{config_blob_digest}"
    config_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.container.image.v1+json"
    }
    response = requests.get(config_blob_url, headers=config_headers)
    if response.status_code != 200:
        raise RetrieveImageConfigException("image config.")
    return response.json()


def get_image_creation_time(tag: str) -> datetime:
    """
    Get Docker image creation time from tag image config.

    Args:
        tag: Docker tag name

    Returns:
        Datetime object of image creation time.
    """
    namespace, repo_tag = tag.split("/")
    repository, tag = repo_tag.split(":")
    config = get_config_docker_oci(tag=tag, namespace=namespace, repository=repository)
    if "created" not in config:
        raise RetrieveImageConfigException("image creation time.")
    return parser.isoparse(config["created"])


def delete_tag(tag: str) -> bool:
    """
    Delete tag from Docker Hub repo.

    Args:
        tag: Docker tag name
    Returns:
        True if tag was deleted successfully, False otherwise.
    """
    token = get_docker_hub_auth_token()
    headers = {
        "Authorization": f"Bearer {token}",
    }
    namespace, repo_tag = tag.split("/")
    repository, tag_name = repo_tag.split(":")

    url = f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag_name}"  # noqa E501
    response = requests.delete(url, headers=headers)
    if response.status_code == 429:
        raise DockerHubRateLimitException()
    if response.status_code != 204:
        logger.info(f"Failed to delete {tag}, status code: {response.json()}")
        return False
    else:
        logger.info(f"Deleted tag {tag}")
        return True


def query_tags_from_docker_hub(
    filter_func: Callable[[str], bool],
    namespace: str,
    repository: str,
    num_tags: Optional[int] = 1000,
) -> List[str]:
    """
    Query tags from Docker Hub repository with filter.
    If Docker Hub API returns an error, the function will:
        - Stop querying
        - Return the current list of tags.

    Args:
        filter_func: Function to return whether tag should be included.
        namespace: Docker namespace
        repository: Docker repository
        num_tags: Max number of tags to query

    Returns:
        Sorted list of tags from Docker Hub repository
        with format namespace/repository:tag.
    """
    filtered_tags = []
    headers = {
        "Authorization": f"Bearer {get_docker_hub_auth_token()}",
    }

    page_count = 1
    while page_count:
        logger.info(f"Querying page {page_count}")
        url = f"https://hub.docker.com/v2/namespaces/{namespace}/repositories/{repository}/tags?page={page_count}&page_size=100"  # noqa E501

        response = requests.get(url, headers=headers)
        response_json = response.json()

        # Stop querying if Docker Hub API returns an error
        if response.status_code != 200:
            logger.info(f"Failed to query tags from Docker Hub: Error: {response_json}")
            return sorted([f"{namespace}/{repository}:{t}" for t in filtered_tags])

        result = response_json["results"]
        tags = [tag["name"] for tag in result]
        filtered_tags_page = list(filter(filter_func, tags))  # Filter tags

        # Add enough tags to not exceed num_tags if num_tags is specified
        if num_tags:
            if len(filtered_tags) + len(filtered_tags_page) > num_tags:
                filtered_tags.extend(
                    filtered_tags_page[: num_tags - len(filtered_tags)]
                )
                break
        filtered_tags.extend(filtered_tags_page)

        logger.info(f"Tag count: {len(filtered_tags)}")
        if not response_json["next"]:
            break
        page_count += 1
    return sorted([f"{namespace}/{repository}:{t}" for t in filtered_tags])


def _call_crane_ls(namespace: str, repository: str):
    try:
        return subprocess.check_output(
            [
                "crane",
                "ls",
                f"{namespace}/{repository}",
            ],
            text=True,
        )
    except subprocess.CalledProcessError as e:
        return f"Error: {e.output}"


def query_tags_from_docker_with_crane(namespace: str, repository: str) -> List[str]:
    """
    Query all repo tags from Docker using Crane.

    Args:
        namespace: Docker namespace
        repository: Docker repository

    Returns:
        List of tags from Docker Registry in format namespace/repository:tag.
    """
    get_docker_auth_token(namespace=namespace, repository=repository)
    result = _call_crane_ls(namespace=namespace, repository=repository)
    if "Error" in result:
        raise Exception(f"Failed to query tags from Docker Registry: {result}")
    return [f"{namespace}/{repository}:{t}" for t in result.split("\n")]
