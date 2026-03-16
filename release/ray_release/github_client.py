"""
Minimal GitHub REST API client using requests, replacing PyGithub.

Supports the operations used by ray_release: reading/creating/updating issues,
reading labels, and reading pull requests.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


class GitHubException(Exception):
    """Raised when the GitHub API returns a non-2xx response."""

    def __init__(self, status: int, data: Any = None, headers: Any = None) -> None:
        self.status = status
        self.data = data
        self.headers = headers
        super().__init__(f"GitHub API error {status}: {data}")


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
class GitHubRepo:
    """A GitHub repository."""

    full_name: str
    _client: "GitHubClient"

    def get_pull(self, number: int) -> GitHubPull:
        """Return a single pull request by number."""
        data = self._client._get(f"/repos/{self.full_name}/pulls/{number}")
        return GitHubPull.from_dict(data)


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
