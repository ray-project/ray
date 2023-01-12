import os
from github import Github
from datetime import date, timedelta, datetime
from run_release_test import run
from typing import Optional

# First create a Github instance:

# using an access token
token = os.getenv("GITHUB_ACCESS_TOKEN")
if not token:
    raise ValueError("You should provide a github access token via `GITHUB_ACCESS_TOKEN` env variable.")
g = Github(token)

today = date.today()
week_ago = today - timedelta(days=14)
repo = g.get_repo("ray-project/ray")

last_success = "5c7826f55a0776bcea42b019dd9aec452e729342"
first_failure = "2182af14b147d4e3c973e61ac0238c71ee686667"
commits = list(repo.get_commits(since=datetime.fromordinal(week_ago.toordinal())))
print(commits)
first = None
last = None

for i, commit in enumerate(commits):
    if commit.sha == last_success:
        last_success_idx = i
    if commit.sha == first_failure:
        first_failure_idx = i

assert last_success_idx
assert first_failure_idx
commits = commits[first_failure_idx:last_success_idx+1]

test_name = "many_tasks"
def run_test(commit) -> bool:
    wheel = f"https://s3-us-west-2.amazonaws.com/ray-wheels/master/{commit}/ray-3.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
    result = run(
        test_name,
        test_collection_file=None,
        smoke_test=False,
        report=False,
        ray_wheels=wheel,
        cluster_id=None,
        cluster_env_id=None,
        env=None,
        no_terminate=False,
    )
    return result.return_code == 0

last_success_idx = len(commits) - 1
first_failure_idx = 0

from dataclasses import dataclass

@dataclass
class Commit:
    sha: str
    success: Optional[bool] = None


commits = [Commit(sha=commit.sha) for commit in commits]
commits[-1].success = True
commits[0].success = False

while True:
    if last_success_idx - first_failure_idx <= 1:
        break

    i = first_failure_idx + ((last_success_idx - first_failure_idx) // 2)
    commit = commits[i]
    print(f"Running a test with a commit {commit}")
    try:
        success = run_test(commit.sha)
    except Exception as e:
        print(e)
        print("Failed to run a test. Consider the test has failed.")
        success = False

    if success:
        print(f"{commit} succeed to run.")
        last_success_idx = i
        commit.success = True
    else:
        print(f"{commit} failed to run.")
        first_failure_idx = i
        commit.success = False

print("\n")
for commit in commits:
    print(commit)

print(f"\nFirst commit that failed is {commits[first_failure_idx].sha}.")
