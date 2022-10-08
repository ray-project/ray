import enum
import os
import subprocess
from typing import Optional, Dict, Tuple

from ray_release.exception import ReleaseTestConfigError
from ray_release.logger import logger
from ray_release.wheels import DEFAULT_BRANCH, get_buildkite_repo_branch


class Frequency(enum.Enum):
    DISABLED = enum.auto()
    ANY = enum.auto()
    MULTI = enum.auto()
    NIGHTLY = enum.auto()
    NIGHTLY_3x = enum.auto()
    WEEKLY = enum.auto()


frequency_str_to_enum = {
    "disabled": Frequency.DISABLED,
    "any": Frequency.ANY,
    "any-smoke": Frequency.ANY,
    "multi": Frequency.MULTI,
    "nightly": Frequency.NIGHTLY,
    "nightly-3x": Frequency.NIGHTLY_3x,
    "weekly": Frequency.WEEKLY,
}


class Priority(enum.Enum):
    DEFAULT = 0
    MANUAL = 10
    HIGH = 50
    HIGHEST = 100


priority_str_to_enum = {
    "default": Priority.DEFAULT,
    "manual": Priority.MANUAL,
    "high": Priority.HIGH,
    "highest": Priority.HIGHEST,
}


def get_frequency(frequency_str: str) -> Frequency:
    frequency_str = frequency_str.lower()
    if frequency_str not in frequency_str_to_enum:
        raise ReleaseTestConfigError(
            f"Frequency not found: {frequency_str}. Must be one of "
            f"{list(frequency_str_to_enum.keys())}."
        )
    return frequency_str_to_enum[frequency_str]


def get_priority(priority_str: str) -> Priority:
    priority_str = priority_str.lower()
    if priority_str not in priority_str_to_enum:
        raise ReleaseTestConfigError(
            f"Priority not found: {priority_str}. Must be one of "
            f"{list(priority_str_to_enum.keys())}."
        )
    return priority_str_to_enum[priority_str]


def get_test_attr_regex_filters(filters_str: str) -> Dict[str, str]:
    if not filters_str:
        return {}

    test_attr_regex_filters = {}
    for line in filters_str.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":", maxsplit=1)
        if len(parts) != 2:
            raise ReleaseTestConfigError(
                f"Invalid test attr regex filter: {line}. "
                "Should be of the form attr:regex"
            )
        test_attr_regex_filters[parts[0]] = parts[1]
    return test_attr_regex_filters


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
        "prefer_smoke_tests": False,
        "test_attr_regex_filters": None,
        "ray_wheels": None,
        "ray_test_repo": None,
        "ray_test_branch": None,
        "priority": Priority.DEFAULT,
        "no_concurrency_limit": False,
    }
    return settings


def update_settings_from_environment(settings: Dict) -> Dict:
    if "RELEASE_FREQUENCY" in os.environ:
        settings["frequency"] = get_frequency(os.environ["RELEASE_FREQUENCY"])

    if "RELEASE_PREFER_SMOKE_TESTS" in os.environ:
        settings["prefer_smoke_tests"] = bool(
            int(os.environ["RELEASE_PREFER_SMOKE_TESTS"])
        )
    elif os.environ.get("RELEASE_FREQUENCY", "").endswith("-smoke"):
        settings["prefer_smoke_tests"] = True

    if "RAY_TEST_REPO" in os.environ:
        settings["ray_test_repo"] = os.environ["RAY_TEST_REPO"]
        settings["ray_test_branch"] = os.environ.get("RAY_TEST_BRANCH", DEFAULT_BRANCH)
    elif "BUILDKITE_BRANCH" in os.environ:
        repo_url, branch = get_buildkite_repo_branch()

        settings["ray_test_repo"] = repo_url
        settings["ray_test_branch"] = branch

    if "RAY_WHEELS" in os.environ:
        settings["ray_wheels"] = os.environ["RAY_WHEELS"]

    if "TEST_NAME" in os.environ:
        # This is for backward compatibility.
        settings["test_attr_regex_filters"] = get_test_attr_regex_filters(
            "name:" + os.environ["TEST_NAME"]
        )

    if "TEST_ATTR_REGEX_FILTERS" in os.environ:
        settings["test_attr_regex_filters"] = get_test_attr_regex_filters(
            os.environ["TEST_ATTR_REGEX_FILTERS"]
        )

    if "RELEASE_PRIORITY" in os.environ:
        settings["priority"] = get_priority(os.environ["RELEASE_PRIORITY"])

    if "NO_CONCURRENCY_LIMIT" in os.environ:
        settings["no_concurrency_limit"] = bool(int(os.environ["NO_CONCURRENCY_LIMIT"]))

    return settings


def update_settings_from_buildkite(settings: Dict):
    release_frequency = get_buildkite_prompt_value("release-frequency")
    if release_frequency:
        settings["frequency"] = get_frequency(release_frequency)
        if release_frequency.endswith("-smoke"):
            settings["prefer_smoke_tests"] = True

    ray_test_repo_branch = get_buildkite_prompt_value("release-ray-test-repo-branch")
    if ray_test_repo_branch:
        repo, branch = split_ray_repo_str(ray_test_repo_branch)
        settings["ray_test_repo"] = repo
        settings["ray_test_branch"] = branch

    ray_wheels = get_buildkite_prompt_value("release-ray-wheels")
    if ray_wheels:
        settings["ray_wheels"] = ray_wheels

    test_name_filter = get_buildkite_prompt_value("release-test-name")
    if test_name_filter:
        settings["test_attr_regex_filters"] = get_test_attr_regex_filters(
            "name:" + test_name_filter
        )

    test_attr_regex_filters = get_buildkite_prompt_value(
        "release-test-attr-regex-filters"
    )
    if test_attr_regex_filters:
        settings["test_attr_regex_filters"] = get_test_attr_regex_filters(
            test_attr_regex_filters
        )

    test_priority = get_buildkite_prompt_value("release-priority")
    if test_priority:
        settings["priority"] = get_priority(test_priority)

    no_concurrency_limit = get_buildkite_prompt_value("release-no-concurrency-limit")
    if no_concurrency_limit == "yes":
        settings["no_concurrency_limit"] = True

    return settings
