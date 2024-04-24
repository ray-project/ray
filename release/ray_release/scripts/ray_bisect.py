import click
import copy
import subprocess
import os
import json
import time
from typing import Dict, List, Set, Tuple
from pathlib import Path

from ray_release.bazel import bazel_runfile
from ray_release.logger import logger
from ray_release.buildkite.step import get_step
from ray_release.byod.build import (
    build_anyscale_base_byod_images,
    build_anyscale_custom_byod_image,
)
from ray_release.config import read_and_validate_release_test_collection
from ray_release.configs.global_config import init_global_config
from ray_release.test import Test
from ray_release.test_automation.release_state_machine import ReleaseTestStateMachine


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
@click.option(
    "--test-collection-file",
    type=str,
    multiple=True,
    help="Test collection file, relative path to ray repo.",
)
@click.option(
    "--concurrency",
    default=3,
    type=int,
    help=(
        "Maximum number of concurrent test jobs to run. Higher number uses more "
        "capacity, but reduce the bisect duration"
    ),
)
@click.option(
    "--run-per-commit",
    default=1,
    type=int,
    help=(
        "The number of time we run test on the same commit, to account for test "
        "flakiness. Commit passes only when it passes on all runs"
    ),
)
@click.option(
    "--is-full-test",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Use the full, non-smoke version of the test"),
)
@click.option(
    "--global-config",
    default="oss_config.yaml",
    type=click.Choice(
        [x.name for x in (Path(__file__).parent.parent / "configs").glob("*.yaml")]
    ),
    help="Global config to use for test execution.",
)
def main(
    test_name: str,
    passing_commit: str,
    failing_commit: str,
    test_collection_file: Tuple[str],
    concurrency: int = 1,
    run_per_commit: int = 1,
    is_full_test: bool = False,
    global_config: str = "oss_config.yaml",
) -> None:
    init_global_config(
        bazel_runfile("release/ray_release/configs", global_config),
    )
    if concurrency <= 0:
        raise ValueError(
            f"Concurrency input need to be a positive number, received: {concurrency}"
        )
    test = _get_test(test_name, test_collection_file)
    pre_sanity_check = _sanity_check(
        test, passing_commit, failing_commit, run_per_commit, is_full_test
    )
    if not pre_sanity_check:
        logger.info(
            "Failed pre-saniy check, the test might be flaky or fail due to"
            " an external (not a code change) factors"
        )
        return
    commit_lists = _get_commit_lists(passing_commit, failing_commit)
    blamed_commit = _bisect(
        test, commit_lists, concurrency, run_per_commit, is_full_test
    )
    logger.info(f"Blamed commit found for test {test_name}: {blamed_commit}")
    # TODO(can): this env var is used as a feature flag, in case we need to turn this
    # off quickly. We should remove this when the new db reporter is stable.
    if os.environ.get("UPDATE_TEST_STATE_MACHINE", False):
        logger.info(f"Updating test state for test {test_name} to CONSISTENTLY_FAILING")
        _update_test_state(test, blamed_commit)


def _bisect(
    test: Test,
    commit_list: List[str],
    concurrency: int,
    run_per_commit: int,
    is_full_test: bool = False,
) -> str:
    while len(commit_list) > 2:
        logger.info(
            f"Bisecting between {len(commit_list)} commits: "
            f"{commit_list[0]} to {commit_list[-1]} with concurrency {concurrency}"
        )
        idx_to_commit = {}
        for i in range(concurrency):
            idx = len(commit_list) * (i + 1) // (concurrency + 1)
            # make sure that idx is not at the boundary; this avoids rerun bisect
            # on the previously run revision
            idx = min(max(idx, 1), len(commit_list) - 2)
            idx_to_commit[idx] = commit_list[idx]
        outcomes = _run_test(
            test, set(idx_to_commit.values()), run_per_commit, is_full_test
        )
        passing_idx = 0
        failing_idx = len(commit_list) - 1
        for idx, commit in idx_to_commit.items():
            is_passing = all(
                outcome == "passed" for outcome in outcomes[commit].values()
            )
            if is_passing and idx > passing_idx:
                passing_idx = idx
            if not is_passing and idx < failing_idx:
                failing_idx = idx
        commit_list = commit_list[passing_idx : failing_idx + 1]
    return commit_list[-1]


def _sanity_check(
    test: Test,
    passing_revision: str,
    failing_revision: str,
    run_per_commit: int,
    is_full_test: bool = False,
) -> bool:
    """
    Sanity check that the test indeed passes on the passing revision, and fails on the
    failing revision
    """
    logger.info(
        f"Sanity check passing revision: {passing_revision}"
        f" and failing revision: {failing_revision}"
    )
    outcomes = _run_test(
        test, [passing_revision, failing_revision], run_per_commit, is_full_test
    )
    if any(map(lambda x: x != "passed", outcomes[passing_revision].values())):
        return False
    return any(map(lambda x: x != "passed", outcomes[failing_revision].values()))


def _run_test(
    test: Test, commits: Set[str], run_per_commit: int, is_full_test: bool
) -> Dict[str, Dict[int, str]]:
    logger.info(f'Running test {test["name"]} on commits {commits}')
    for commit in commits:
        _trigger_test_run(test, commit, run_per_commit, is_full_test)
    return _obtain_test_result(commits, run_per_commit)


def _trigger_test_run(
    test: Test, commit: str, run_per_commit: int, is_full_test: bool
) -> None:
    os.environ["COMMIT_TO_TEST"] = commit
    build_anyscale_base_byod_images([test])
    build_anyscale_custom_byod_image(test)
    for run in range(run_per_commit):
        step = get_step(
            copy.deepcopy(test),  # avoid mutating the original test
            smoke_test=test.get("smoke_test", False) and not is_full_test,
            env={
                "RAY_COMMIT_OF_WHEEL": commit,
                "COMMIT_TO_TEST": commit,
            },
        )
        step["label"] = f'{test["name"]}:{commit[:7]}-{run}'
        step["key"] = f"{commit}-{run}"
        pipeline = subprocess.Popen(
            ["echo", json.dumps({"steps": [step]})], stdout=subprocess.PIPE
        )
        subprocess.check_output(
            ["buildkite-agent", "pipeline", "upload"], stdin=pipeline.stdout
        )
        pipeline.stdout.close()


def _obtain_test_result(
    commits: Set[str], run_per_commit: int
) -> Dict[str, Dict[int, str]]:
    outcomes = {}
    wait = 5
    total_wait = 0
    while True:
        logger.info(f"... waiting for test result ...({total_wait} seconds)")
        for commit in commits:
            if commit in outcomes and len(outcomes[commit]) == run_per_commit:
                continue
            for run in range(run_per_commit):
                outcome = (
                    subprocess.check_output(
                        [
                            "buildkite-agent",
                            "step",
                            "get",
                            "outcome",
                            "--step",
                            f"{commit}-{run}",
                        ]
                    )
                    .decode("utf-8")
                    .strip()
                )
                if not outcome:
                    continue
                if commit not in outcomes:
                    outcomes[commit] = {}
                outcomes[commit][run] = outcome
        all_commit_finished = len(outcomes) == len(commits)
        per_commit_finished = all(
            len(outcome) == run_per_commit for outcome in outcomes.values()
        )
        if all_commit_finished and per_commit_finished:
            break
        time.sleep(wait)
        total_wait = total_wait + wait
    logger.info(f"Final test outcomes: {outcomes}")
    return outcomes


def _get_test(test_name: str, test_collection_file: Tuple[str]) -> Test:
    test_collection = read_and_validate_release_test_collection(
        test_collection_file or ["release/release_tests.yaml"],
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


def _update_test_state(test: Test, blamed_commit: str) -> None:
    test.update_from_s3()
    logger.info(f"Test object: {json.dumps(test)}")
    test[Test.KEY_BISECT_BLAMED_COMMIT] = blamed_commit

    # Compute and update the next test state, then comment blamed commit on github issue
    sm = ReleaseTestStateMachine(test)
    sm.move()
    sm.comment_blamed_commit_on_github_issue()

    logger.info(f"Test object: {json.dumps(test)}")
    test.persist_to_s3()


if __name__ == "__main__":
    main()
