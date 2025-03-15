import base64
import io
import logging
import os
import subprocess
import sys
import tempfile

import boto3
from typing import List
from math import ceil

import ci.ray_ci.bazel_sharding as bazel_sharding
from ray_release.bazel import bazel_runfile
from ray_release.test import Test, TestState
from ray_release.configs.global_config import init_global_config

GLOBAL_CONFIG_FILE = (
    os.environ.get("RAYCI_GLOBAL_CONFIG") or "ci/ray_ci/oss_config.yaml"
)
RAY_VERSION = "2.44.0"


def ci_init() -> None:
    """
    Initialize global config
    """
    init_global_config(bazel_runfile(GLOBAL_CONFIG_FILE))


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


def get_flaky_test_names(prefix: str) -> List[str]:
    """
    Query all flaky tests with specified prefix.

    Args:
        prefix: A prefix to filter by.

    Returns:
        List[str]: List of test names.
    """
    tests = Test.gen_from_s3(prefix)
    # Filter tests by test state
    state = TestState.FLAKY
    test_names = [t.get_name() for t in tests if t.get_state() == state]

    # Remove prefixes.
    for i in range(len(test_names)):
        test = test_names[i]
        if test.startswith(prefix):
            test_names[i] = test[len(prefix) :]

    return test_names


def filter_tests(
    input: io.TextIOBase, output: io.TextIOBase, prefix: str, state_filter: str
):
    """
    Filter flaky tests from list of test targets.

    Args:
        input: Input stream, each test name in one line.
        output: Output stream, each test name in one line.
        prefix: Prefix to query tests with.
        state_filter: Options to filter tests: "flaky" or "-flaky" tests.
    """
    # Valid prefix check
    if prefix not in ["darwin:", "linux:", "windows:"]:
        raise ValueError("Prefix must be one of 'darwin:', 'linux:', or 'windows:'.")

    # Valid filter choices check
    if state_filter not in ["flaky", "-flaky"]:
        raise ValueError("Filter option must be one of 'flaky' or '-flaky'.")

    # Obtain all existing tests with specified test state
    flaky_tests = set(get_flaky_test_names(prefix))

    # Filter these test from list of test targets based on user condition.
    for t in input:
        t = t.strip()
        if not t:
            continue

        hit = t in flaky_tests
        if state_filter == "-flaky":
            hit = not hit

        if hit:
            output.write(f"{t}\n")


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
