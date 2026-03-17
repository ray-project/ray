import sys
from unittest import mock

import pytest
import responses
from click.testing import CliRunner

from ci.ray_ci.automation.get_contributors import _find_pr_number, run

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

_COMMIT_LOG_NO_PRS = "\n".join(
    [
        "[core] Fix scheduler bug",
        "[serve] Add new endpoint",
        "No category commit",
        "",
    ]
)


# ---------------------------------------------------------------------------
# _find_pr_number
# ---------------------------------------------------------------------------


def test_find_pr_number_standard_format():
    assert _find_pr_number("[fix] Fix crash in serve (#12345)") == "12345"


def test_find_pr_number_no_pr():
    assert _find_pr_number("[fix] Fix crash in serve") == ""


def test_find_pr_number_empty_string():
    assert _find_pr_number("") == ""


def test_find_pr_number_unclosed_paren():
    assert _find_pr_number("[fix] Fix crash (#12345") == ""


def test_find_pr_number_at_start_of_line():
    assert _find_pr_number("(#99)") == "99"


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
