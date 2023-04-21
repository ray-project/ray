import click
import subprocess
import os
import json
import time
from typing import Dict, List, Optional, Set
from ray_release.logger import logger
from ray_release.buildkite.step import get_step
from ray_release.config import (
    read_and_validate_release_test_collection,
    DEFAULT_WHEEL_WAIT_TIMEOUT,
    Test,
)
from ray_release.wheels import find_and_wait_for_ray_wheels_url


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
@click.option(
    "--concurrency",
    default=3,
    type=int,
    help=(
        "Maximum number of concurrent test jobs to run. Higher number uses more "
        "capacity, but reduce the bisect duration"
    ),
)
def main(
    test_name: str,
    passing_commit: str,
    failing_commit: str,
    concurrency: Optional[int] = 1,
) -> None:
    if concurrency <= 0:
        raise ValueError(
            f"Concurrency input need to be a positive number, received: {concurrency}"
        )
    test = _get_test(test_name)
    pre_sanity_check = _sanity_check(test, passing_commit, failing_commit)
    if not pre_sanity_check:
        logger.info(
            "Failed pre-saniy check, the test might be flaky or fail due to"
            " an external (not a code change) factors"
        )
        return
    commit_lists = _get_commit_lists(passing_commit, failing_commit)
    blamed_commit = _bisect(test, commit_lists, concurrency)
    logger.info(f"Blamed commit found for test {test_name}: {blamed_commit}")


def _bisect(test: Test, commit_list: List[str], concurrency: int) -> str:
    while len(commit_list) > 2:
        logger.info(
            f"Bisecting between {len(commit_list)} commits: "
            f"{commit_list[0]} to {commit_list[-1]} with concurrency {concurrency}"
        )
        idx_to_commit = {}
        for i in range(concurrency):
            idx = len(commit_list) * (i + 1) // (concurrency + 1)
            idx_to_commit[idx] = commit_list[idx]
        outcomes = _run_test(test, set(idx_to_commit.values()))
        passing_idx = 0
        failing_idx = len(commit_list) - 1
        for idx, commit in idx_to_commit.items():
            is_passing = outcomes[commit] == "passed"
            if is_passing and idx > passing_idx:
                passing_idx = idx
            if not is_passing and idx < failing_idx:
                failing_idx = idx
        commit_list = commit_list[passing_idx : failing_idx + 1]
    return commit_list[-1]


def _sanity_check(test: Test, passing_revision: str, failing_revision: str) -> bool:
    """
    Sanity check that the test indeed passes on the passing revision, and fails on the
    failing revision
    """
    logger.info(
        f"Sanity check passing revision: {passing_revision}"
        f" and failing revision: {failing_revision}"
    )
    outcomes = _run_test(test, [passing_revision, failing_revision])
    return (
        outcomes[passing_revision] == "passed"
        and outcomes[failing_revision] != "passed"
    )


def _run_test(test: Test, commits: Set[str]) -> Dict[str, str]:
    logger.info(f'Running test {test["name"]} on commits {commits}')
    for commit in commits:
        _trigger_test_run(test, commit)
    return _obtain_test_result(commits)


def _trigger_test_run(test: Test, commit: str) -> None:
    ray_wheels_url = find_and_wait_for_ray_wheels_url(
        commit,
        timeout=DEFAULT_WHEEL_WAIT_TIMEOUT,
    )
    step = get_step(
        test,
        ray_wheels=ray_wheels_url,
        env={
            "RAY_COMMIT_OF_WHEEL": commit,
        },
    )
    step["label"] = f'{test["name"]}:{commit[:7]}'
    step["key"] = commit
    pipeline = subprocess.Popen(
        ["echo", json.dumps({"steps": [step]})], stdout=subprocess.PIPE
    )
    subprocess.check_output(
        ["buildkite-agent", "pipeline", "upload"], stdin=pipeline.stdout
    )
    pipeline.stdout.close()


def _obtain_test_result(buildkite_step_keys: List[str]) -> Dict[str, str]:
    outcomes = {}
    wait = 5
    total_wait = 0
    while True:
        logger.info(f"... waiting for test result ...({total_wait} seconds)")
        for key in buildkite_step_keys:
            if key in outcomes:
                continue
            outcome = subprocess.check_output(
                [
                    "buildkite-agent",
                    "step",
                    "get",
                    "outcome",
                    "--step",
                    key,
                ]
            ).decode("utf-8")
            if outcome:
                outcomes[key] = outcome
        if len(outcomes) == len(buildkite_step_keys):
            break
        time.sleep(wait)
        total_wait = total_wait + wait
    logger.info(f"Final test outcomes: {outcomes}")
    return outcomes


def _get_test(test_name: str) -> Test:
    test_collection = read_and_validate_release_test_collection(
        os.path.join(os.path.dirname(__file__), "..", "..", "release_tests.yaml")
    )
    return [test for test in test_collection if test["name"] == test_name][0]


def _get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
    # This command obtains all commits between inclusively
    return (
        subprocess.check_output(
            f"git rev-list --reverse ^{passing_commit}~ {failing_commit}",
            shell=True,
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )


if __name__ == "__main__":
    main()
