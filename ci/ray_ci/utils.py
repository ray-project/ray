import base64
import logging
import subprocess
import sys
import tempfile

import boto3
from typing import List
from math import ceil
from ray_release.test import Test, TestState

import ci.ray_ci.bazel_sharding as bazel_sharding


POSTMERGE_PIPELINE = "0189e759-8c96-4302-b6b5-b4274406bf89"
RAY_VERSION = "3.0.0.dev0"


def chunk_into_n(list: List[str], n: int) -> List[List[str]]:
    """
    Chunk a list into n chunks
    """
    size = ceil(len(list) / n)
    return [list[x * size : x * size + size] for x in range(n)]


def shard_tests(
    test_targets: List[str],
    shard_count: int,
    shard_id: int,
) -> List[str]:
    """
    Shard tests into N shards and return the shard corresponding to shard_id
    """
    return bazel_sharding.main(test_targets, index=shard_id, count=shard_count)


def docker_login(docker_ecr: str) -> None:
    """
    Login to docker with AWS credentials
    """
    token = boto3.client("ecr", region_name="us-west-2").get_authorization_token()
    user, password = (
        base64.b64decode(token["authorizationData"][0]["authorizationToken"])
        .decode("utf-8")
        .split(":")
    )
    with tempfile.TemporaryFile() as f:
        f.write(bytes(password, "utf-8"))
        f.flush()
        f.seek(0)

        subprocess.run(
            [
                "docker",
                "login",
                "--username",
                user,
                "--password-stdin",
                docker_ecr,
            ],
            stdin=f,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )


def docker_pull(image: str) -> None:
    """
    Pull docker image
    """
    subprocess.run(
        ["docker", "pull", image],
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=True,
    )


def query_all_test_names_by_state(test_state: str = "flaky", prefix_on: bool = False):
    """
    Query all existing test names by the test state.

    Args:
        test_state: Test state to filter by.
            Use string representation from ray_release.test.TestState class.
        prefix_on: Whether to include test prefix in test name.

    Returns:
        List[str]: List of test names.
    """
    TEST_PREFIXES = ["darwin:"]

    # Convert test_state string into TestState enum
    test_state_enum = next(
        (state for state in TestState if state.value == test_state), None
    )
    if test_state_enum is None:
        raise ValueError("Invalid test state.")

    # Obtain all existing tests
    tests = Test.get_tests(TEST_PREFIXES)
    # Filter tests by test state
    filtered_tests = Test.filter_tests_by_state(tests, test_state_enum)
    filtered_test_names = [test.get_name() for test in filtered_tests]
    if prefix_on:
        return filtered_test_names
    else:
        no_prefix_filtered_test_names = [
            test.replace(prefix, "")
            for test in filtered_test_names
            for prefix in TEST_PREFIXES
            if test.startswith(prefix)
        ]
        return no_prefix_filtered_test_names


def omit_tests_by_state(
    test_targets: List[str], test_state: str = "flaky"
) -> List[str]:
    """
    Omit tests from list of tests by test state.

    Args:
        test_targets: List of test targets.
        test_state: Test state to filter by.
            Use string representation from ray_release.test.TestState class.

    Returns:
        List[str]: List of test targets with tests of specified state removed.
    """
    # Obtain all existing tests with specified test state
    state_test_names = query_all_test_names_by_state(
        test_state=test_state, prefix_on=False
    )
    # Eliminate these test from list of test targets
    test_targets_filtered = [
        test for test in test_targets if test not in state_test_names
    ]
    return test_targets_filtered


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def add_handlers(logger: logging.Logger):
    """
    Add handlers to logger
    """
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(
        fmt="[%(levelname)s %(asctime)s] %(filename)s: %(lineno)d  %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if not logger.hasHandlers():
    add_handlers(logger)
