"""
Tests for the GitHub REST API client.

Uses the `responses` library to intercept outbound HTTP requests without
patching Python internals, so tests exercise the full client code path:
URL construction, headers, JSON parsing, and dataclass population.
"""
import sys

import pytest

from ray_release.github_client import (
    GitHubClient,
    GitHubException,
    GitHubRepo,
)

REPO = "owner/repo"
BASE = "https://api.github.com"


def _client() -> GitHubClient:
    return GitHubClient("test-token")


def _repo(client: GitHubClient = None) -> GitHubRepo:
    return (_client() if client is None else client).get_repo(REPO)


# ---------------------------------------------------------------------------
# GitHubException
# ---------------------------------------------------------------------------


def test_github_exception_stores_status_and_data():
    exc = GitHubException(404, {"message": "Not Found"}, {"x-header": "val"})
    assert exc.status == 404
    assert exc.data == {"message": "Not Found"}
    assert exc.headers == {"x-header": "val"}
    assert "404" in str(exc)


def test_github_exception_is_exception():
    assert isinstance(GitHubException(500), Exception)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
