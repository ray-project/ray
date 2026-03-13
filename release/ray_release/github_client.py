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
    name: str


@dataclass
class GitHubPullUser:
    login: str


@dataclass
class GitHubPull:
    number: int
    user: GitHubPullUser


@dataclass
class GitHubIssue:
    number: int
    state: str
    title: str
    html_url: str
    _repo: "GitHubRepo"
    _labels: List[GitHubLabel] = field(default_factory=list)

    def get_labels(self) -> List[GitHubLabel]:
        return self._labels

    def create_comment(self, body: str) -> None:
        self._repo._client._post(
            f"/repos/{self._repo.full_name}/issues/{self.number}/comments",
            {"body": body},
        )

    def edit(
        self,
        state: Optional[str] = None,
        labels: Optional[List[str]] = None,
    ) -> None:
        data: Dict[str, Any] = {}
        if state is not None:
            data["state"] = state
        if labels is not None:
            data["labels"] = labels
        self._repo._client._patch(
            f"/repos/{self._repo.full_name}/issues/{self.number}",
            data,
        )
        if state is not None:
            self.state = state


@dataclass
class GitHubRepo:
    full_name: str
    _client: "GitHubClient"

    def get_label(self, name: str) -> GitHubLabel:
        return GitHubLabel(name=name)

    def get_issues(
        self,
        state: str = "open",
        labels: Optional[List[GitHubLabel]] = None,
    ) -> List[GitHubIssue]:
        params: Dict[str, Any] = {"state": state}
        if labels:
            params["labels"] = ",".join(label.name for label in labels)
        data = self._client._get_paginated(f"/repos/{self.full_name}/issues", params)
        return [self._issue_from_data(item) for item in data]

    def get_issue(self, number: int) -> GitHubIssue:
        data = self._client._get(f"/repos/{self.full_name}/issues/{number}")
        return self._issue_from_data(data)

    def get_pull(self, number: int) -> GitHubPull:
        data = self._client._get(f"/repos/{self.full_name}/pulls/{number}")
        return GitHubPull(
            number=data["number"],
            user=GitHubPullUser(login=data["user"]["login"]),
        )

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
    BASE_URL = "https://api.github.com"

    def __init__(self, token: str) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def get_repo(self, full_name: str) -> GitHubRepo:
        return GitHubRepo(full_name=full_name, _client=self)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        resp = self._session.get(f"{self.BASE_URL}{path}", params=params)
        if not resp.ok:
            raise GitHubException(resp.status_code, resp.json())
        return resp.json()

    def _get_paginated(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> list:
        params = dict(params or {})
        params["per_page"] = 100
        results = []
        page = 1
        while True:
            params["page"] = page
            resp = self._session.get(f"{self.BASE_URL}{path}", params=params)
            if not resp.ok:
                raise GitHubException(resp.status_code, resp.json())
            data = resp.json()
            if not data:
                break
            results.extend(data)
            page += 1
        return results

    def _post(self, path: str, data: dict) -> dict:
        resp = self._session.post(f"{self.BASE_URL}{path}", json=data)
        if not resp.ok:
            raise GitHubException(resp.status_code, resp.json())
        return resp.json()

    def _patch(self, path: str, data: dict) -> dict:
        resp = self._session.patch(f"{self.BASE_URL}{path}", json=data)
        if not resp.ok:
            raise GitHubException(resp.status_code, resp.json())
        return resp.json()
