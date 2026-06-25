import os
import re
import sys
from collections import defaultdict
from subprocess import check_output
from typing import List

import click
from tqdm import tqdm

from ray_release.github_client import GitHubClient, GitHubException

# Matches a well-formed "(#<digits>)" reference. Compiled once at module scope
# since the same pattern is reused across every invocation of the script.
_PR_NUMBER_RE = re.compile(r"\(#(\d+)\)")


def _find_pr_numbers(text: str) -> List[int]:
    # A single commit title may carry several "(#<digits>)" references: a fixed
    # issue, a reverted PR, or an original PR plus its cherry-pick/backport PR
    # (e.g. "...in MiB (#63932) (#64042)"). The text alone cannot tell an issue
    # from a PR, nor an original from a backport, so we return every well-formed
    # candidate in order and let the caller resolve which are real PRs via the
    # GitHub API. Truncated text like "(#6..." is ignored because it has no
    # closing ")". `text` may be a single title or the whole multi-line git-log
    # blob; re.findall scans across newlines either way.
    return [int(n) for n in _PR_NUMBER_RE.findall(text)]


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

    # Query Github API to get the list of contributors. A commit may reference
    # several numbers (e.g. an original PR plus its backport); we collect every
    # candidate so that both the original author and the backporter are credited
    # and a cherry-pick never silently drops the original author. Numbers that
    # are actually issues (or deleted PRs) resolve to a 404 below and are
    # skipped, so we cannot pre-filter them from the text.
    #
    # Trade-off: because we keep every reference, a commit that mentions another
    # PR in prose -- most commonly a revert like 'Revert "[X] foo (#500)" (#600)'
    # -- also credits the author of that referenced PR (#500), even though their
    # change is not in this release. We accept this over-crediting in exchange
    # for never missing a real contributor.
    pr_numbers = sorted(set(_find_pr_numbers(commits)))

    # Sort the PR numbers
    print("PR numbers", pr_numbers)

    # Use Github API to fetch PR authors
    client = GitHubClient(access_token)
    ray_repo = client.get_repo("ray-project/ray")
    logins = set()
    # A 404 is expected for numbers that are actually issues rather than PRs.
    # We still collect them so the script acknowledges every number it could
    # not resolve, in case GitHub's behavior (issue numbers 404, PR numbers
    # never do) changes and a real PR silently stops being credited.
    not_found_numbers = []
    # Any other error (rate limit 403, 5xx, network) means we could not look up
    # that PR's author. Dropping it silently would omit a real contributor from
    # a release announcement, so we collect these and fail loudly at the end
    # instead of printing a complete-looking list and exiting 0.
    errored_numbers = []
    for num in tqdm(pr_numbers):
        try:
            logins.add(ray_repo.get_pull(num).user.login)
        except GitHubException as e:
            if getattr(e, "status", None) == 404:
                not_found_numbers.append(num)
            else:
                print(e)
                errored_numbers.append(num)

    if not_found_numbers:
        print()
        print(
            "The following numbers did not resolve to a PR (expected for issue "
            "references; investigate if any of these are known PRs):"
        )
        print(not_found_numbers)

    print()
    print("Here's the list of contributors")
    print("=" * 10)
    print()
    print("@" + ", @".join(logins))
    print()
    print("=" * 10)

    if errored_numbers:
        raise click.ClickException(
            "Could not fetch the author for the following PR numbers due to "
            f"GitHub API errors (see logs above): {errored_numbers}. The "
            "contributor list above is INCOMPLETE; re-run after resolving the "
            "errors (e.g. wait out rate limits) before using it."
        )


if __name__ == "__main__":
    run()
