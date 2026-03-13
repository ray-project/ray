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
    GitHubIssue,
    GitHubLabel,
    GitHubPull,
    GitHubRepo,
)

REPO = "owner/repo"
BASE = "https://api.github.com"


def _client() -> GitHubClient:
    return GitHubClient("test-token")


def _repo(client: GitHubClient = None) -> GitHubRepo:
    return (_client() if client is None else client).get_repo(REPO)


def _issue_json(**overrides) -> dict:
    base = {
        "number": 42,
        "state": "open",
        "title": "Flaky test in CI",
        "html_url": f"https://github.com/{REPO}/issues/42",
        "labels": [{"name": "bug"}, {"name": "core"}],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# GitHubClient — auth headers
# ---------------------------------------------------------------------------


@responses.activate
def test_auth_header_is_sent():
    responses.add(
        responses.GET, f"{BASE}/repos/{REPO}/issues/1", json=_issue_json(number=1)
    )
    client = GitHubClient("my-secret-token")
    client.get_repo(REPO).get_issue(1)

    sent_headers = responses.calls[0].request.headers
    assert sent_headers["Authorization"] == "Bearer my-secret-token"
    assert sent_headers["Accept"] == "application/vnd.github+json"
    assert "X-GitHub-Api-Version" in sent_headers


# ---------------------------------------------------------------------------
# GitHubRepo.get_issue
# ---------------------------------------------------------------------------


@responses.activate
def test_get_issue_returns_populated_issue():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(),
    )
    issue = _repo().get_issue(42)

    assert isinstance(issue, GitHubIssue)
    assert issue.number == 42
    assert issue.state == "open"
    assert issue.title == "Flaky test in CI"
    assert issue.html_url == f"https://github.com/{REPO}/issues/42"


@responses.activate
def test_get_issue_populates_labels():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(
            labels=[{"name": "weekly-release-blocker"}, {"name": "serve"}]
        ),
    )
    issue = _repo().get_issue(42)
    labels = issue.get_labels()

    assert len(labels) == 2
    assert labels[0].name == "weekly-release-blocker"
    assert labels[1].name == "serve"


@responses.activate
def test_get_issue_with_no_labels():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(labels=[]),
    )
    issue = _repo().get_issue(42)
    assert issue.get_labels() == []


@responses.activate
def test_get_issue_raises_github_exception_on_404():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/9999",
        json={"message": "Not Found"},
        status=404,
    )
    with pytest.raises(GitHubException) as exc_info:
        _repo().get_issue(9999)

    assert exc_info.value.status == 404
    assert exc_info.value.data == {"message": "Not Found"}


# ---------------------------------------------------------------------------
# GitHubRepo.get_label + get_issues
# ---------------------------------------------------------------------------


def test_get_label_returns_label_with_name():
    label = _repo().get_label("weekly-release-blocker")
    assert isinstance(label, GitHubLabel)
    assert label.name == "weekly-release-blocker"


@responses.activate
def test_get_issues_passes_state_and_label_params():
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=[])

    repo = _repo()
    label = repo.get_label("weekly-release-blocker")
    repo.get_issues(state="open", labels=[label])

    qs = responses.calls[0].request.url
    assert "state=open" in qs
    assert "labels=weekly-release-blocker" in qs
    assert "per_page=100" in qs


@responses.activate
def test_get_issues_returns_list_of_issues():
    page1 = [_issue_json(number=1, title="A"), _issue_json(number=2, title="B")]
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=page1)
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=[])

    issues = _repo().get_issues(state="open")

    assert len(issues) == 2
    assert issues[0].number == 1
    assert issues[1].number == 2


@responses.activate
def test_get_issues_paginates():
    page1 = [_issue_json(number=i) for i in range(100)]
    page2 = [_issue_json(number=i) for i in range(100, 115)]
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=page1)
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=page2)
    responses.add(responses.GET, f"{BASE}/repos/{REPO}/issues", json=[])

    issues = _repo().get_issues()
    assert len(issues) == 115


@responses.activate
def test_get_issues_raises_on_error():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues",
        json={"message": "Forbidden"},
        status=403,
    )
    with pytest.raises(GitHubException) as exc_info:
        _repo().get_issues()
    assert exc_info.value.status == 403


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
# GitHubIssue.create_comment
# ---------------------------------------------------------------------------


@responses.activate
def test_create_comment_posts_to_correct_url():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(),
    )
    responses.add(
        responses.POST,
        f"{BASE}/repos/{REPO}/issues/42/comments",
        json={"id": 1},
        status=201,
    )

    issue = _repo().get_issue(42)
    issue.create_comment("Test has been failing. Jailing.")

    post_body = responses.calls[1].request.body
    assert b"Test has been failing. Jailing." in post_body


@responses.activate
def test_create_comment_raises_on_error():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(),
    )
    responses.add(
        responses.POST,
        f"{BASE}/repos/{REPO}/issues/42/comments",
        json={"message": "Unprocessable"},
        status=422,
    )

    issue = _repo().get_issue(42)
    with pytest.raises(GitHubException) as exc_info:
        issue.create_comment("hi")
    assert exc_info.value.status == 422


# ---------------------------------------------------------------------------
# GitHubIssue.edit
# ---------------------------------------------------------------------------


@responses.activate
def test_edit_state_sends_patch_and_updates_local_state():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(state="open"),
    )
    responses.add(
        responses.PATCH,
        f"{BASE}/repos/{REPO}/issues/42",
        json={"state": "closed"},
        status=200,
    )

    issue = _repo().get_issue(42)
    assert issue.state == "open"

    issue.edit(state="closed")

    assert issue.state == "closed"
    patch_body = responses.calls[1].request.body
    assert b'"closed"' in patch_body


@responses.activate
def test_edit_labels_sends_patch_without_state():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(),
    )
    responses.add(
        responses.PATCH,
        f"{BASE}/repos/{REPO}/issues/42",
        json={},
        status=200,
    )

    issue = _repo().get_issue(42)
    issue.edit(labels=["jailed-test", "core"])

    patch_body = responses.calls[1].request.body
    assert b"jailed-test" in patch_body
    assert b"state" not in patch_body


@responses.activate
def test_edit_raises_on_error():
    responses.add(
        responses.GET,
        f"{BASE}/repos/{REPO}/issues/42",
        json=_issue_json(),
    )
    responses.add(
        responses.PATCH,
        f"{BASE}/repos/{REPO}/issues/42",
        json={"message": "Not Found"},
        status=404,
    )

    issue = _repo().get_issue(42)
    with pytest.raises(GitHubException) as exc_info:
        issue.edit(state="closed")
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
