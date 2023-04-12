import click
import math
import subprocess
from typing import List

@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    blamed_commit = _bisect(test_name, passing_commit, failing_commit)
    print(f'Blamed commit found: {blamed_commit}')

def _bisect(test_name: str, passing_commit: str, failing_commit: str) -> str:
    lists = _get_commit_lists(passing_commit, failing_commit)
    while len(lists) > 1:
        middle_commit = lists[math.floor(len(lists)/2)]
        is_passing = _run_test(test_name, middle_commit)
        if is_passing:
            passing_commit = middle_commit
        else:
            failing_commit = middle_commit
        lists = _get_commit_lists(passing_commit, failing_commit)
    return failing_commit

def _run_test(test_name: str, commit: str) -> bool:
    return True

def _get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
    commit_lists = subprocess.check_output(
        f'git rev-list --ancestry-path {passing_commit}..{failing_commit}',
        shell=True,
    )
    return commit_lists.decode('utf-8').split("\n")

if __name__ == "__main__":
    main()