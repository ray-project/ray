"""
Minimal GitHub REST API client using requests, replacing PyGithub.

Supports the operations used by ray_release: reading/creating/updating issues,
reading labels, and reading pull requests.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests


class GitHubException(Exception):
    """Raised when the GitHub API returns a non-2xx response."""

    def __init__(self, status: int, data: Any = None, headers: Any = None) -> None:
        self.status = status
        self.data = data
        self.headers = headers
        super().__init__(f"GitHub API error {status}: {data}")


@dataclass
class GitHubLabel:
    """A GitHub label with a name."""

    name: str


@dataclass
class GitHubPullUser:
    """The author of a pull request."""

    login: str

    @classmethod
    def from_dict(cls, data: dict) -> "GitHubPullUser":
        return cls(login=data["login"])


@dataclass
class GitHubPull:
    """A GitHub pull request."""

    number: int
    user: GitHubPullUser

    @classmethod
    def from_dict(cls, data: dict) -> "GitHubPull":
        return cls(number=data["number"], user=GitHubPullUser.from_dict(data["user"]))


@dataclass
class GitHubIssue:
    """A GitHub issue."""

    number: int
    state: str
    title: str
    html_url: str
    _repo: "GitHubRepo"
    _labels: List[GitHubLabel] = field(default_factory=list)

    def get_labels(self) -> List[GitHubLabel]:
        """Return the labels attached to this issue."""
        return self._labels

    def create_comment(self, body: str) -> None:
        """Post a comment on this issue."""
        self._repo._client._post(
            f"/repos/{self._repo.full_name}/issues/{self.number}/comments",
            {"body": body},
        )

    def edit(
        self,
        title: Optional[str] = None,
        state: Optional[str] = None,
        labels: Optional[List[str]] = None,
    ) -> None:
        """Update the issue title, state, and/or labels."""
        data: Dict[str, Any] = {}
        if title is not None:
            data["title"] = title
        if state is not None:
            data["state"] = state
        if labels is not None:
            data["labels"] = labels
        resp = self._repo._client._patch(
            f"/repos/{self._repo.full_name}/issues/{self.number}",
            data,
        )
        updated = self._repo._issue_from_data(resp)
        self.title = updated.title
        self.state = updated.state
        self._labels = updated._labels


@dataclass
class GitHubRepo:
    """A GitHub repository."""

    full_name: str
    _client: "GitHubClient"

    def get_label(self, name: str) -> GitHubLabel:
        """Return a label object for the given name."""
        return GitHubLabel(name=name)

    def create_issue(
        self,
        title: str,
        body: str = "",
        labels: Optional[List[str]] = None,
    ) -> "GitHubIssue":
        """Create a new issue and return it."""
        data: Dict[str, Any] = {"title": title, "body": body}
        if labels is not None:
            data["labels"] = labels
        resp = self._client._post(f"/repos/{self.full_name}/issues", data)
        return self._issue_from_data(resp)

    def get_issues(
        self,
        state: str = "open",
        labels: Optional[List[GitHubLabel]] = None,
    ) -> List[GitHubIssue]:
        """Return all issues matching the given state and labels."""
        params: Dict[str, Any] = {"state": state}
        if labels:
            params["labels"] = ",".join(label.name for label in labels)
        data = self._client._get_paginated(f"/repos/{self.full_name}/issues", params)
        return [self._issue_from_data(item) for item in data]

    def get_issue(self, number: int) -> GitHubIssue:
        """Return a single issue by number."""
        data = self._client._get(f"/repos/{self.full_name}/issues/{number}")
        return self._issue_from_data(data)

    def get_pull(self, number: int) -> GitHubPull:
        """Return a single pull request by number."""
        data = self._client._get(f"/repos/{self.full_name}/pulls/{number}")
        return GitHubPull.from_dict(data)

    def _issue_from_data(self, data: dict) -> GitHubIssue:
        return GitHubIssue(
            number=data["number"],
            state=data["state"],
            title=data["title"],
            html_url=data["html_url"],
            _repo=self,
            _labels=[GitHubLabel(name=lbl["name"]) for lbl in data.get("labels", [])],
        )


class GitHubClient:
    """Authenticated client for the GitHub REST API."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str) -> None:
        """Initialize the client with a personal access token or app token."""
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def get_repo(self, full_name: str) -> "GitHubRepo":
        """Return a repo handle for owner/name (e.g. 'ray-project/ray')."""
        return GitHubRepo(full_name=full_name, _client=self)

    def _raise_for_response(self, resp: requests.Response) -> None:
        if not resp.ok:
            try:
                data = resp.json()
            except requests.exceptions.JSONDecodeError:
                data = resp.text
            raise GitHubException(resp.status_code, data, resp.headers)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        resp = self._session.get(f"{self.BASE_URL}{path}", params=params)
        self._raise_for_response(resp)
        return resp.json()

    def _get_paginated(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> list:
        params = dict(params or {})
        params["per_page"] = 100
        results = []
        url: Optional[str] = f"{self.BASE_URL}{path}"
        while url:
            resp = self._session.get(url, params=params)
            self._raise_for_response(resp)
            results.extend(resp.json())
            url = resp.links.get("next", {}).get("url")
            params = None  # subsequent URLs from Link header already include all params
        return results

    def _post(self, path: str, data: dict) -> dict:
        resp = self._session.post(f"{self.BASE_URL}{path}", json=data)
        self._raise_for_response(resp)
        return resp.json()

    def _patch(self, path: str, data: dict) -> dict:
        resp = self._session.patch(f"{self.BASE_URL}{path}", json=data)
        self._raise_for_response(resp)
        return resp.json()
