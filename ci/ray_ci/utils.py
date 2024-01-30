import base64
import io
import logging
import subprocess
import sys
import tempfile

import boto3
from typing import List
from math import ceil

import ci.ray_ci.bazel_sharding as bazel_sharding
from ray_release.test import Test, TestState


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


def filter_out_flaky_tests(input: io.TextIOBase, output: io.TextIOBase, prefix: str):
    """
    Filter out flaky tests from list of test targets.

    Args:
        input: Input stream, each test name in one line.
        output: Output stream, each test name in one line.
        prefix: Prefix to query tests with.
    """
    # Obtain all existing tests with specified test state
    flaky_tests = set(get_flaky_test_names(prefix))

    # Filter out these test from list of test targets
    for t in input:
        t = t.strip()
        if not t:
            continue
        if t in flaky_tests:
            continue
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
