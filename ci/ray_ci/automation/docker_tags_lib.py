import subprocess
import docker
import re
from datetime import datetime
from typing import List, Optional, Callable, Tuple
import os
import sys
from dateutil import parser
import runfiles
import platform

import requests

from ci.ray_ci.utils import logger
from ci.ray_ci.builder_container import DEFAULT_ARCHITECTURE, DEFAULT_PYTHON_VERSION
from ci.ray_ci.docker_container import (
    GPU_PLATFORM,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_ML,
    PLATFORMS_RAY,
    PLATFORMS_RAY_ML,
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_ML,
    RayType,
)

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
SHA_LENGTH = 6


def _check_python_version(python_version: str, ray_type: str) -> None:
    if ray_type == RayType.RAY and python_version not in PYTHON_VERSIONS_RAY:
        raise ValueError(
            f"Python version {python_version} not supported for ray image."
        )
    if ray_type == RayType.RAY_ML and python_version not in PYTHON_VERSIONS_RAY_ML:
        raise ValueError(
            f"Python version {python_version} not supported for ray-ml image."
        )


def _check_platform(platform: str, ray_type: str) -> None:
    if ray_type == RayType.RAY and platform not in PLATFORMS_RAY:
        raise ValueError(f"Platform {platform} not supported for ray image.")
    if ray_type == RayType.RAY_ML and platform not in PLATFORMS_RAY_ML:
        raise ValueError(f"Platform {platform} not supported for ray-ml image.")


def _check_architecture(architecture: str, ray_type: str) -> None:
    if ray_type == RayType.RAY and architecture not in ARCHITECTURES_RAY:
        raise ValueError(f"Architecture {architecture} not supported for ray image.")
    if ray_type == RayType.RAY_ML and architecture not in ARCHITECTURES_RAY_ML:
        raise ValueError(f"Architecture {architecture} not supported for ray-ml image.")


def _get_python_version_tag(python_version: str) -> str:
    return f"-py{python_version.replace('.', '')}"  # 3.x -> py3x


def _get_platform_tag(platform: str) -> str:
    if platform == "cpu":
        return "-cpu"
    versions = platform.split(".")
    return f"-{versions[0]}{versions[1]}"  # cu11.8.0-cudnn8 -> cu118


def _get_architecture_tag(architecture: str) -> str:
    return f"-{architecture}" if architecture != DEFAULT_ARCHITECTURE else ""


def list_image_tag_suffixes(
    ray_type: str, python_version: str, platform: str, architecture: str
) -> List[str]:
    """
    Get image tags & alias suffixes for the container image.
    """
    _check_python_version(python_version, ray_type)
    _check_platform(platform, ray_type)
    _check_architecture(architecture, ray_type)

    python_version_tags = [_get_python_version_tag(python_version)]
    platform_tags = [_get_platform_tag(platform)]
    architecture_tags = [_get_architecture_tag(architecture)]

    if python_version == DEFAULT_PYTHON_VERSION:
        python_version_tags.append("")
    if platform == "cpu" and ray_type == RayType.RAY:
        platform_tags.append("")  # no tag is alias to cpu for ray image
    if platform == GPU_PLATFORM:
        platform_tags.append("-gpu")  # gpu is alias for cu11.8.0
        if ray_type == RayType.RAY_ML:
            platform_tags.append("")  # no tag is alias to gpu for ray-ml image

    tag_suffixes = []
    for platform in platform_tags:
        for py_version in python_version_tags:
            for architecture in architecture_tags:
                tag_suffix = f"{py_version}{platform}{architecture}"
                tag_suffixes.append(tag_suffix)
    return tag_suffixes


def pull_image(image_name: str) -> None:
    logger.info(f"Pulling image {image_name}")
    client = docker.from_env()
    client.images.pull(image_name)


def remove_image(image_name: str) -> None:
    logger.info(f"Removing image {image_name}")
    client = docker.from_env()
    client.images.remove(image_name)


def get_ray_commit(image_name: str) -> str:
    """
    Get the commit hash of Ray in the image.
    """
    client = docker.from_env()

    # Command to grab commit hash from ray image
    command = "python -u -c 'import ray; print(ray.__commit__)'"

    container = client.containers.run(
        image=image_name, command=command, remove=True, stdout=True, stderr=True
    )
    output = container.decode("utf-8").strip()
    match = re.search(
        r"^[a-f0-9]{40}$", output, re.MULTILINE
    )  # Grab commit hash from output

    if not match:
        raise Exception(
            f"Failed to get commit hash from image {image_name}. Output: {output}"
        )
    return match.group(0)


def check_image_ray_commit(prefix: str, ray_type: str, expected_commit: str) -> None:
    if ray_type == RayType.RAY:
        tags = list_image_tags(
            prefix, ray_type, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
        )
    elif ray_type == RayType.RAY_ML:
        tags = list_image_tags(
            prefix,
            ray_type,
            PYTHON_VERSIONS_RAY_ML,
            PLATFORMS_RAY_ML,
            ARCHITECTURES_RAY_ML,
        )
    tags = [f"rayproject/{ray_type}:{tag}" for tag in tags]

    for i, tag in enumerate(tags):
        logger.info(f"{i+1}/{len(tags)} Checking commit for tag {tag} ....")

        pull_image(tag)
        commit = get_ray_commit(tag)

        if commit != expected_commit:
            print(f"Ray commit mismatch for tag {tag}!")
            print("Expected:", expected_commit)
            print("Actual:", commit)
            sys.exit(42)  # Not retrying the check on Buildkite jobs
        else:
            print(f"Commit {commit} match for tag {tag}!")

        if i != 0:  # Only save first pulled image for caching
            remove_image(tag)


def list_image_tags(
    prefix: str,
    ray_type: str,
    python_versions: List[str],
    platforms: List[str],
    architectures: List[str],
) -> List[str]:
    """
    List all tags for a Docker build version.

    An image tag is composed by ray version tag, python version and platform.
    See https://docs.ray.io/en/latest/ray-overview/installation.html for
    more information on the image tags.

    Args:
        prefix: The build version of the image tag (e.g. nightly, abc123).
        ray_type: The type of the ray image. It can be "ray" or "ray-ml".
    """
    if ray_type not in RayType.__members__.values():
        raise ValueError(f"Ray type {ray_type} not supported.")

    tag_suffixes = []
    for python_version in python_versions:
        for platf in platforms:
            for architecture in architectures:
                tag_suffixes += list_image_tag_suffixes(
                    ray_type, python_version, platf, architecture
                )
    return sorted([f"{prefix}{tag_suffix}" for tag_suffix in tag_suffixes])


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


def _get_docker_auth_token(namespace: str, repository: str) -> Optional[str]:
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


def _get_docker_hub_auth_token(username: str, password: str) -> Optional[str]:
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


def _get_git_log(n_days: int = 30) -> str:
    return subprocess.check_output(
        [
            "git",
            "log",
            f"--until='{n_days} days ago'",
            "--pretty=format:%H",
        ],
        text=True,
    )


def _list_recent_commit_short_shas(n_days: int = 30) -> List[str]:
    """
    Get list of recent commit SHAs (short version) on ray master branch.

    Args:
        n_days: Number of days to go back in git log.

    Returns:
        List of recent commit SHAs (short version).
    """
    commit_shas = _get_git_log(n_days=n_days)
    short_commit_shas = [
        commit_sha[:SHA_LENGTH] for commit_sha in commit_shas.split("\n") if commit_sha
    ]
    return short_commit_shas


def _get_config_docker_oci(tag: str, namespace: str, repository: str):
    """Get Docker image config from tag using OCI API."""
    token = _get_docker_auth_token(namespace, repository)

    # Pull image manifest to get config digest
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
    }
    image_manifest_url = (
        f"https://registry-1.docker.io/v2/{namespace}/{repository}/manifests/{tag}"
    )
    response = requests.get(image_manifest_url, headers=headers)
    if response.status_code != 200:
        raise RetrieveImageConfigException("image manifest.")
    config_blob_digest = response.json()["config"]["digest"]

    # Pull image config
    config_blob_url = f"https://registry-1.docker.io/v2/{namespace}/{repository}/blobs/{config_blob_digest}"  # noqa E501
    config_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.container.image.v1+json",
    }
    response = requests.get(config_blob_url, headers=config_headers)
    if response.status_code != 200:
        raise RetrieveImageConfigException("image config.")
    return response.json()


def _get_image_creation_time(tag: str) -> datetime:
    namespace, repo_tag = tag.split("/")
    repository, tag = repo_tag.split(":")
    config = _get_config_docker_oci(tag=tag, namespace=namespace, repository=repository)
    if "created" not in config:
        raise RetrieveImageConfigException("image creation time.")
    return parser.isoparse(config["created"])


def delete_tag(tag: str, docker_hub_token: str) -> bool:
    """Delete tag from Docker Hub repo."""
    headers = {
        "Authorization": f"Bearer {docker_hub_token}",
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
    logger.info(f"Deleted tag {tag}")
    return True


def query_tags_from_docker_hub(
    filter_func: Callable[[str], bool],
    namespace: str,
    repository: str,
    docker_hub_token: str,
    num_tags: Optional[int] = None,
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
        "Authorization": f"Bearer {docker_hub_token}",
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


def query_tags_from_docker_with_oci(namespace: str, repository: str) -> List[str]:
    """
    Query all repo tags from Docker using OCI API.

    Args:
        namespace: Docker namespace
        repository: Docker repository

    Returns:
        List of tags from Docker Registry in format namespace/repository:tag.
    """
    token = _get_docker_auth_token(namespace, repository)
    headers = {
        "Authorization": f"Bearer {token}",
    }
    url = f"https://registry-1.docker.io/v2/{namespace}/{repository}/tags/list"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to query tags from Docker: {response.json()}")

    return [f"{namespace}/{repository}:{t}" for t in response.json()["tags"]]


def _is_release_tag(
    tag: str,
    release_versions: Optional[List[str]] = None,
) -> bool:
    """
    Check if tag is a release tag & is in the list of release versions.
    Tag input format should be just the tag name, without namespace/repository.
    Tag input can be in any format queried from Docker Hub: "x.y.z-...", "a1s2d3-..."
    Args:
        tag: Docker tag name
        release_versions: List of release versions.
            If None, don't filter by release version.
    Returns:
        True if tag is a release tag and is in the list of release versions.
            False otherwise.
    """
    versions = tag.split(".")
    if len(versions) != 3 and "post1" not in tag:
        return False
    # Parse variables into major, minor, patch version
    major, minor, patch = versions[0], versions[1], versions[2]
    extra = versions[3] if len(versions) > 3 else None
    if not major.isnumeric() or not minor.isnumeric():
        return False
    if not patch.isnumeric() and "rc" not in patch and "-" not in patch:
        return False

    if "-" in patch:
        patch = patch.split("-")[0]
    release_version = ".".join([major, minor, patch])
    if extra:
        release_version += f".{extra}"
    if release_versions and release_version not in release_versions:
        return False

    return True


def _crane_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise ValueError(f"Unsupported platform: {system}")
    return r.Rlocation("crane_linux_x86_64/crane")


def _call_crane_cp(tag: str, source: str, aws_ecr_repo: str) -> Tuple[int, str]:
    try:
        with subprocess.Popen(
            [
                _crane_binary(),
                "cp",
                source,
                f"{aws_ecr_repo}:{tag}",
            ],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc:
            output = ""
            for line in proc.stdout:
                logger.info(line + "\n")
                output += line
            return_code = proc.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, proc.args)
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def copy_tag_to_aws_ecr(tag: str, aws_ecr_repo: str) -> bool:
    """
    Copy tag from Docker Hub to AWS ECR.
    Args:
        tag: Docker tag name in format "namespace/repository:tag"
    Returns:
        True if tag was copied successfully, False otherwise.
    """
    _, repo_tag = tag.split("/")
    tag_name = repo_tag.split(":")[1]
    logger.info(f"Copying from {tag} to {aws_ecr_repo}:{tag_name}......")
    return_code, output = _call_crane_cp(
        tag=tag_name,
        source=tag,
        aws_ecr_repo=aws_ecr_repo,
    )
    if return_code:
        logger.info(f"Failed to copy {tag} to {aws_ecr_repo}:{tag_name}......")
        logger.info(f"Error: {output}")
        return False
    logger.info(f"Copied {tag} to {aws_ecr_repo}:{tag_name} successfully")
    return True


def backup_release_tags(
    namespace: str,
    repository: str,
    aws_ecr_repo: str,
    docker_username: str,
    docker_password: str,
    release_versions: Optional[List[str]] = None,
) -> None:
    """
    Backup release tags to AWS ECR.
    Args:
        release_versions: List of release versions to backup
        aws_ecr_repo: AWS ECR repository
    """
    docker_hub_token = _get_docker_hub_auth_token(docker_username, docker_password)
    docker_hub_tags = query_tags_from_docker_hub(
        filter_func=lambda t: _is_release_tag(t, release_versions),
        namespace=namespace,
        repository=repository,
        docker_hub_token=docker_hub_token,
    )
    _write_to_file("release_tags.txt", docker_hub_tags)
    for t in docker_hub_tags:
        copy_tag_to_aws_ecr(tag=t, aws_ecr_repo=aws_ecr_repo)


def _write_to_file(file_path: str, content: List[str]) -> None:
    file_path = os.path.join(bazel_workspace_dir, file_path)
    logger.info(f"Writing to {file_path}......")
    with open(file_path, "w") as f:
        f.write("\n".join(content))
