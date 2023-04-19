import click
import subprocess
import os
import json
import time
from typing import List
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
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    commit_lists = _get_commit_lists(passing_commit, failing_commit)
    blamed_commit = _bisect(test_name, commit_lists)
    logger.info(f"Blamed commit found for test {test_name}: {blamed_commit}")


def _bisect(test_name: str, commit_list: List[str]) -> str:
    test = _get_test(test_name)
    while len(commit_list) > 2:
        logger.info(
            f"Bisecting between {len(commit_list)} commits: "
            f"{commit_list[0]} to {commit_list[-1]}"
        )
        middle_commit_idx = len(commit_list) // 2
        middle_commit = commit_list[middle_commit_idx]
        is_passing = _run_test(test, middle_commit)
        if is_passing:
            commit_list = commit_list[middle_commit_idx:]
        else:
            commit_list = commit_list[: middle_commit_idx + 1]
    return commit_list[-1]


def _run_test(test: Test, commit: str) -> bool:
    logger.info(f'Running test {test["name"]} on commit {commit}')
    _trigger_test_run(test, commit)
    return _obtain_test_result(commit)


def _trigger_test_run(test: Test, commit: str) -> None:
    ray_wheels_url = find_and_wait_for_ray_wheels_url(
        commit,
        timeout=DEFAULT_WHEEL_WAIT_TIMEOUT,
    )
    step = get_step(test, ray_wheels=ray_wheels_url)
    step["label"] = f'{test["name"]}:{commit[:6]}'
    step["key"] = commit
    pipeline = json.dumps({"steps": [step]})
    subprocess.check_output(
        f'echo "{pipeline}" | buildkite-agent pipeline upload',
        shell=True,
    )


def _obtain_test_result(buildkite_step_key: str) -> bool:
    outcome = None
    wait = 30
    total_wait = 0
    while outcome not in ["passed", "hard_failed", "soft_failed"]:
        logger.info(f"... waiting for test result ...({total_wait} seconds)")
        outcome = subprocess.check_output(
            f'buildkite-agent step get "outcome" --step "{buildkite_step_key}"',
            shell=True,
        ).decode("utf-8")
        time.sleep(wait)
        total_wait = total_wait + wait
    logger.info(f"Final test outcome: {outcome}")
    return outcome == "passed"


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
