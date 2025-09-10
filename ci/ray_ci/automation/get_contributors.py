import os
import sys
from collections import defaultdict
from subprocess import check_output

import click
from github import Github
from tqdm import tqdm


def _find_pr_number(line: str) -> str:
    start = line.find("(#")
    if start < 0:
        return ""
    end = line.find(")", start + 2)
    if end < 0:
        return ""
    return line[start + 2 : end]


@click.command()
@click.option(
    "--access-token",
    required=True,
    help="""
Github Access token that has repo:public_repo and user:read:user permission.

Create them at https://github.com/settings/tokens/new
""",
)
@click.option(
    "--prev-release-commit",
    required=True,
    help="Last commit SHA of the previous release.",
)
@click.option(
    "--curr-release-commit",
    required=True,
    help="Last commit SHA of the current release.",
)
def run(access_token, prev_release_commit, curr_release_commit):
    repo_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
    if not repo_dir:
        raise ValueError(
            "BUILD_WORKSPACE_DIRECTORY not set; please run with bazel run."
        )

    print("Writing commit descriptions to 'commits.txt'...")
    commits = check_output(
        [
            "git",
            "log",
            f"{prev_release_commit}..{curr_release_commit}",
            "--pretty=format:%s",
        ],
        cwd=repo_dir,
        stderr=sys.stderr,
    ).decode()

    lines = commits.split("\n")

    # Organize commits
    NO_CATEGORY = "[NO_CATEGORY]"

    def get_category(line):
        if line[0] == "[":
            return (line.split("]")[0].strip(" ") + "]").upper()
        return NO_CATEGORY

    commits_by_team = defaultdict(list)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        commits_by_team[get_category(line)].append(line)

    team_output_file = "/tmp/commits.txt"
    print(f"Writing team's commits in '{team_output_file}'...")

    with open(team_output_file, "w") as file:
        for category, commit_msgs in commits_by_team.items():
            file.write("\n{}\n".format(category))
            for commit_msg in commit_msgs:
                file.write("{}\n".format(commit_msg))

    # Query Github API to get the list of contributors
    pr_numbers = []
    for line in lines:
        pr_number = _find_pr_number(line)
        if pr_number:
            pr_numbers.append(int(pr_number))

    # Sort the PR numbers
    print("PR numbers", pr_numbers)

    # Use Github API to fetch the
    g = Github(access_token)
    ray_repo = g.get_repo("ray-project/ray")
    logins = set()
    for num in tqdm(pr_numbers):
        try:
            logins.add(ray_repo.get_pull(num).user.login)
        except Exception as e:
            print(e)

    print()
    print("Here's the list of contributors")
    print("=" * 10)
    print()
    print("@" + ", @".join(logins))
    print()
    print("=" * 10)


if __name__ == "__main__":
    run()
