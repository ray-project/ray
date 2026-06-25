import sys
from unittest import mock

import pytest
import responses
from click.testing import CliRunner

from ci.ray_ci.automation.get_contributors import _find_pr_numbers, run

BASE = "https://api.github.com"
REPO = "ray-project/ray"

_COMMIT_LOG = "\n".join(
    [
        "[core] Fix scheduler bug (#100)",
        "[serve] Add new endpoint (#101)",
        "No category commit (#102)",
        "",
    ]
)

# A cherry-pick carries the original PR followed by the backport PR. Both
# authors should be credited.
_COMMIT_LOG_CHERRY_PICK = "\n".join(
    [
        "[Core] Compute per component memory usage in MiB (#63932) (#64042)",
        "",
    ]
)

_COMMIT_LOG_NO_PRS = "\n".join(
    [
        "[core] Fix scheduler bug",
        "[serve] Add new endpoint",
        "No category commit",
        "",
    ]
)


# ---------------------------------------------------------------------------
# _find_pr_numbers
# ---------------------------------------------------------------------------


def test_find_pr_numbers_standard_format():
    assert _find_pr_numbers("[fix] Fix crash in serve (#12345)") == [12345]


def test_find_pr_numbers_no_pr():
    assert _find_pr_numbers("[fix] Fix crash in serve") == []


def test_find_pr_numbers_empty_string():
    assert _find_pr_numbers("") == []


def test_find_pr_numbers_unclosed_paren():
    assert _find_pr_numbers("[fix] Fix crash (#12345") == []


def test_find_pr_numbers_at_start_of_line():
    assert _find_pr_numbers("(#99)") == [99]


def test_find_pr_numbers_multiple_titles_in_one_blob():
    # `run` passes the entire multi-line `git log` output at once; every
    # reference across all lines must be found, in order, including both
    # references of a cherry-pick and ignoring titles with no PR.
    blob = "\n".join(
        [
            "[core] Fix scheduler bug (#100)",
            "[data] No PR reference here",
            "[Core] Compute per component memory usage in MiB (#63932) (#64042)",
            "[serve] Add new endpoint (#101)",
        ]
    )
    assert _find_pr_numbers(blob) == [100, 63932, 64042, 101]


@pytest.mark.parametrize(
    "line,expected",
    [
        # Reverted-PR title where the inner reference is truncated by git's
        # subject elision: only the trailing well-formed token is captured.
        (
            'Revert "[Data] Remove safe_round from ExecutionResources hot path '
            "(#6... (#64309)",
            [64309],
        ),
        # Two references: a fixed GitHub issue (#63729) followed by the merging
        # PR (#63733). Both candidates are returned in title order; the API
        # resolves which are real PRs (the issue number 404s).
        (
            "[dashboard] Guard against zero num_cpus in k8s_utils.cpu_percent "
            "(#63729) (#63733)",
            [63729, 63733],
        ),
        # Cherry-picked commit: original PR first, then the backport PR. Both
        # are returned so both authors can be credited.
        (
            "[Core] Compute per component memory usage in MiB (#63932) (#64042)",
            [63932, 64042],
        ),
        # Truncated reference only — no well-formed "(#<digits>)" token.
        ("[Data] Some very long title that got cut off (#6...", []),
        # Non-digit content between "(#" and ")" must not match.
        ("[fix] Mention the (#hashtag) in the title", []),
    ],
)
def test_find_pr_numbers_edge_cases(line, expected):
    assert _find_pr_numbers(line) == expected


# ---------------------------------------------------------------------------
# run (CLI) — uses `responses` for HTTP, one mock for subprocess
# ---------------------------------------------------------------------------


@responses.activate
def test_run_collects_contributor_logins():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/100",
        json={"number": 100, "user": {"login": "alice"}},
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/101",
        json={"number": 101, "user": {"login": "bob"}},
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/102",
        json={"number": 102, "user": {"login": "carol"}},
    )

    with (
        mock.patch(
            "ci.ray_ci.automation.get_contributors.check_output",
            return_value=_COMMIT_LOG.encode(),
        ),
        mock.patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/fake/repo"}),
    ):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "alice" in result.output
    assert "bob" in result.output
    assert "carol" in result.output


@responses.activate
def test_run_skips_failed_pr_lookups():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/100",
        json={"message": "Not Found"},
        status=404,
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/101",
        json={"number": 101, "user": {"login": "bob"}},
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/102",
        json={"number": 102, "user": {"login": "carol"}},
    )

    with (
        mock.patch(
            "ci.ray_ci.automation.get_contributors.check_output",
            return_value=_COMMIT_LOG.encode(),
        ),
        mock.patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/fake/repo"}),
    ):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "bob" in result.output
    assert "carol" in result.output


@responses.activate
def test_run_credits_both_authors_of_cherry_pick():
    # Original PR author and backport PR author must both be credited.
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/63932",
        json={"number": 63932, "user": {"login": "original_author"}},
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/64042",
        json={"number": 64042, "user": {"login": "backporter"}},
    )

    with (
        mock.patch(
            "ci.ray_ci.automation.get_contributors.check_output",
            return_value=_COMMIT_LOG_CHERRY_PICK.encode(),
        ),
        mock.patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/fake/repo"}),
    ):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "original_author" in result.output
    assert "backporter" in result.output


@responses.activate
def test_run_skips_issue_reference_but_keeps_pr():
    # "(#63729) (#63733)": 63729 is a fixed issue (404), 63733 is the PR.
    # The issue 404 must not prevent the PR author from being credited.
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/63729",
        json={"message": "Not Found"},
        status=404,
    )
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/63733",
        json={"number": 63733, "user": {"login": "dave"}},
    )

    commit_log = (
        "[dashboard] Guard against zero num_cpus in k8s_utils.cpu_percent "
        "(#63729) (#63733)\n"
    )
    with (
        mock.patch(
            "ci.ray_ci.automation.get_contributors.check_output",
            return_value=commit_log.encode(),
        ),
        mock.patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/fake/repo"}),
    ):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "dave" in result.output
    # The unresolved issue number is acknowledged in the output rather than
    # silently dropped.
    assert "63729" in result.output


def test_run_writes_commits_file_with_categories():
    with (
        mock.patch(
            "ci.ray_ci.automation.get_contributors.check_output",
            return_value=_COMMIT_LOG_NO_PRS.encode(),
        ),
        mock.patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/fake/repo"}),
    ):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )

    assert result.exit_code == 0, result.output
    with open("/tmp/commits.txt") as f:
        content = f.read()
    assert "[CORE]" in content
    assert "[SERVE]" in content
    assert "[NO_CATEGORY]" in content
    assert "No category commit" in content


def test_run_fails_without_workspace_dir():
    with mock.patch.dict("os.environ", {}, clear=True):
        result = CliRunner().invoke(
            run,
            [
                "--access-token",
                "tok",
                "--prev-release-commit",
                "abc",
                "--curr-release-commit",
                "def",
            ],
        )
    assert result.exit_code != 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
