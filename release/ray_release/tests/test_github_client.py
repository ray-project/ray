"""
Tests for the GitHub REST API client.

Uses the `responses` library to intercept outbound HTTP requests without
patching Python internals, so tests exercise the full client code path:
URL construction, headers, JSON parsing, and dataclass population.
"""
import sys

import pytest
import responses

from ray_release.github_client import (
    GitHubClient,
    GitHubException,
    GitHubPull,
    GitHubRepo,
)

REPO = "owner/repo"
BASE = "https://api.github.com"


def _client() -> GitHubClient:
    return GitHubClient("test-token")


def _repo(client: GitHubClient = None) -> GitHubRepo:
    return (_client() if client is None else client).get_repo(REPO)


# ---------------------------------------------------------------------------
# GitHubClient — auth headers
# ---------------------------------------------------------------------------


@responses.activate
def test_auth_header_is_sent():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/1",
        json={"number": 1, "user": {"login": "octocat"}},
    )
    client = GitHubClient("my-secret-token")
    client.get_repo(REPO).get_pull(1)

    sent_headers = responses.calls[0].request.headers
    assert sent_headers["Authorization"] == "Bearer my-secret-token"
    assert sent_headers["Accept"] == "application/vnd.github+json"
    assert "X-GitHub-Api-Version" in sent_headers


# ---------------------------------------------------------------------------
# GitHubRepo.get_pull
# ---------------------------------------------------------------------------


@responses.activate
def test_get_pull_returns_number_and_user_login():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/99",
        json={"number": 99, "user": {"login": "octocat"}},
    )
    pull = _repo().get_pull(99)

    assert isinstance(pull, GitHubPull)
    assert pull.number == 99
    assert pull.user.login == "octocat"


@responses.activate
def test_get_pull_raises_on_error():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/pulls/9999",
        json={"message": "Not Found"},
        status=404,
    )
    with pytest.raises(GitHubException) as exc_info:
        _repo().get_pull(9999)
    assert exc_info.value.status == 404


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
