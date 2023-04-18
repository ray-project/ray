import click
import subprocess
import os
import json
import time
from typing import List, Dict
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
    test = _get_test(test_name)
    pre_sanity_check = _sanity_check(test, passing_commit, failing_commit)
    if not pre_sanity_check:
        logger.info(
            "Failed pre-saniy check, the test might be flaky or fail due to"
            " an external (not a code change) factors"
        )
        return
    commit_lists = _get_commit_lists(passing_commit, failing_commit)
    blamed_commit = _bisect(test, commit_lists)
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
        is_passing = _run_test(test, [middle_commit])[middle_commit] == "passed"
        if is_passing:
            commit_list = commit_list[middle_commit_idx:]
        else:
            commit_list = commit_list[: middle_commit_idx + 1]
    return commit_list[-1]


def _sanity_check(test: Test, passing_revision: str, failing_revision: str) -> bool:
    logger.info(
        f"Sanity check passing revision: {passing_revision}"
        f" and failing revision: {failing_revision})"
    )
    outcomes = _run_test(test, [passing_revision, failing_revision])
    return (
        outcomes[passing_revision] == "passed"
        and outcomes[failing_revision] != "passed"
    )


def _run_test(test: Test, commits: List[str]) -> Dict[str, str]:
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
            "RAY_TEST_COMMIT": commit,
        },
    )
    step["label"] = f'{test["name"]}:{commit[:6]}'
    step["key"] = commit
    pipeline = json.dumps({"steps": [step]})
    subprocess.check_output(
        f'echo "{pipeline}" | buildkite-agent pipeline upload',
        shell=True,
    )


def _obtain_test_result(buildkite_step_keys: List[str]) -> dict[str, str]:
    outcomes = {}
    wait = 30
    total_wait = 0
    while len(outcomes) != len(buildkite_step_keys):
        logger.info(f"... waiting for test result ...({total_wait} seconds)")
        for key in buildkite_step_keys:
            if key in outcomes:
                continue
            outcome = subprocess.check_output(
                f'buildkite-agent step get "outcome" --step "{key}"',
                shell=True,
            ).decode("utf-8")
            if outcome:
                outcomes[key] = outcome
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
