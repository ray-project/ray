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
    Test
)
from ray_release.wheels import find_and_wait_for_ray_wheels_url

@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    commit_lists = _get_commit_lists(passing_commit, failing_commit)
    blamed_commit = _bisect(test_name, commit_lists)
    print(f"Blamed commit found: {blamed_commit}")


def _bisect(test_name: str, commit_list: List[str]) -> str:
    while len(commit_list) > 1:
        middle_commit_idx = len(commit_list) // 2
        logger.info(
            f'Bisecting between {len(lists)} commits: '
            f'{passing_commit} to {failing_commit}'
        )
        middle_commit = commit_list[middle_commit_idx]
        is_passing = _run_test(test_name, middle_commit)
        if is_passing:
            commit_list = commit_list[middle_commit_idx + 1 :]
        else:
            commit_list = commit_list[:middle_commit_idx]
    return commit_list[-1]

def _run_test(test: Test, commit: str) -> bool:
    logger.info(f'Running test {test["name"]} on commit {commit}')
    ray_wheels_url = find_and_wait_for_ray_wheels_url(
        commit, timeout=DEFAULT_WHEEL_WAIT_TIMEOUT
    )
    step = get_step(
        test,
        ray_wheels=ray_wheels_url,
    )
    step['label'] = f'{test["name"]}:{commit[:6]}'
    step['key'] = commit
    pipeline = json.dumps({'steps': [step]})
    subprocess.check_output(
        f'echo "{pipeline}" | buildkite-agent pipeline upload',
        shell=True,
    )
    outcome = None
    wait = 30
    total_wait = 0
    while outcome not in ['passed', 'hard_failed', 'soft_failed']:
        logger.info(f'... waiting for test result ...({total_wait} seconds)')
        outcome = subprocess.check_output(
            f'buildkite-agent step get "outcome" --step "{commit}"',
            shell=True,
        ).decode('utf-8')
        time.sleep(wait)
        total_wait = total_wait + wait
    logger.info(f'Final test outcome: {outcome}')
    return outcome == 'passed'

def _get_test(test_name: str) -> Test:
    test_collection = read_and_validate_release_test_collection(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "release_tests.yaml"
        )
    )
    return [test for test in test_collection if test['name'] == test_name][0]

def _get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
    commit_lists = subprocess.check_output(
        f"git rev-list --ancestry-path {passing_commit}..{failing_commit}",
        shell=True,
    )
    return commit_lists.decode("utf-8").split("\n")


if __name__ == "__main__":
    main()
