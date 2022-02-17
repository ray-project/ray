import enum
import os
import subprocess
from typing import Optional, Dict, Tuple

from ray_release.exception import ReleaseTestConfigError
from ray_release.logger import logger
from ray_release.wheels import DEFAULT_BRANCH


class Frequency(enum.Enum):
    DISABLED = enum.auto()
    ANY = enum.auto()
    MULTI = enum.auto()
    NIGHTLY = enum.auto()
    WEEKLY = enum.auto()


frequency_str_to_enum = {
    "disabled": Frequency.DISABLED,
    "any": Frequency.ANY,
    "multi": Frequency.MULTI,
    "nightly": Frequency.NIGHTLY,
    "weekly": Frequency.WEEKLY,
}


def get_frequency(frequency_str: str) -> Frequency:
    frequency_str = frequency_str.lower()
    if frequency_str not in frequency_str_to_enum:
        raise ReleaseTestConfigError(
            f"Frequency not found: {frequency_str}. Must be one of "
            f"{list(frequency_str_to_enum.keys())}."
        )
    return frequency_str_to_enum[frequency_str]


def split_ray_repo_str(repo_str: str) -> Tuple[str, str]:
    if "https://" in repo_str:
        if "/tree/" in repo_str:
            url, branch = repo_str.split("/tree/", maxsplit=2)
            return f"{url}.git", branch.rstrip("/")
        return repo_str, DEFAULT_BRANCH  # Default branch

    if ":" in repo_str:
        owner_or_url, commit_or_branch = repo_str.split(":")
    else:
        owner_or_url = repo_str
        commit_or_branch = DEFAULT_BRANCH

    # Else, construct URL
    url = f"https://github.com/{owner_or_url}/ray.git"
    return url, commit_or_branch


def get_buildkite_prompt_value(key: str) -> Optional[str]:
    try:
        value = subprocess.check_output(
            ["buildkite-agent", "meta-data", "get", key], text=True
        )
    except Exception as e:
        logger.warning(f"Could not fetch metadata for {key}: {e}")
        return None
    logger.debug(f"Got Buildkite prompt value for {key}: {value}")
    return value


def get_pipeline_settings() -> Dict:
    """Get pipeline settings.

    Retrieves settings from the buildkite agent, environment variables,
    and default values (in that order of preference)."""
    settings = get_default_settings()
    settings = update_settings_from_environment(settings)
    settings = update_settings_from_buildkite(settings)
    return settings


def get_default_settings() -> Dict:
    settings = {
        "frequency": Frequency.ANY,
        "test_name_filter": None,
        "ray_wheels": None,
        "ray_test_repo": None,
        "ray_test_branch": None,
    }
    return settings


def update_settings_from_environment(settings: Dict) -> Dict:
    if "RELEASE_FREQUENCY" in os.environ:
        settings["frequency"] = get_frequency(os.environ["RELEASE_FREQUENCY"])

    if "RAY_TEST_REPO" in os.environ:
        settings["ray_test_repo"] = os.environ["RAY_TEST_REPO"]
        settings["ray_test_branch"] = os.environ.get("RAY_TEST_BRANCH", DEFAULT_BRANCH)
    elif "BUILDKITE_BRANCH" in os.environ:
        settings["ray_test_repo"] = os.environ["BUILDKITE_REPO"]
        settings["ray_test_branch"] = os.environ["BUILDKITE_BRANCH"]

    if "RAY_WHEELS" in os.environ:
        settings["ray_wheels"] = os.environ["RAY_WHEELS"]

    if "TEST_NAME" in os.environ:
        settings["test_name_filter"] = os.environ["TEST_NAME"]

    return settings


def update_settings_from_buildkite(settings: Dict):
    release_frequency = get_buildkite_prompt_value("release-frequency")
    if release_frequency:
        settings["frequency"] = get_frequency(release_frequency)

    ray_test_repo_branch = get_buildkite_prompt_value("release-ray-test-repo-branch")
    if ray_test_repo_branch:
        repo, branch = split_ray_repo_str(ray_test_repo_branch)
        settings["ray_test_repo"] = repo
        settings["ray_test_branch"] = branch

    ray_wheels = get_buildkite_prompt_value("release-ray-wheels")
    if ray_wheels:
        settings["ray_wheels"] = ray_wheels

    test_name_filter = get_buildkite_prompt_value("release-test-name")
    if ray_wheels:
        settings["test_name_filter"] = test_name_filter

    return settings
