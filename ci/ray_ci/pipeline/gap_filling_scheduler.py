import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple

from pybuildkite.buildkite import Buildkite


BRANCH = "master"
BLOCK_STEP_KEY = "unblock-me"


class GapFillingScheduler:
    """
    This buildkite pipeline scheduler is responsible for scheduling gap filling builds
    when the latest build is failing.
    """

    def __init__(
        self,
        buildkite_organization: str,
        buildkite_pipeline: str,
        buildkite_access_token: str,
        repo_checkout: str,
        days_ago: int = 1,
    ):
        self.buildkite_organization = buildkite_organization
        self.buildkite_pipeline = buildkite_pipeline
        self.buildkite = Buildkite()
        self.buildkite.set_access_token(buildkite_access_token)
        self.repo_checkout = repo_checkout
        self.days_ago = days_ago

    def run(self) -> Dict[str, Optional[str]]:
        """
        Create gap filling builds for the latest failing build. Return a mapping of
        commit to the triggered build number.
        """
        commits = self.get_gap_commits()

        return {commit: self._trigger_build(commit) for commit in commits}

    def get_gap_commits(self) -> List[str]:
        """
        Return the list of commits between the latest passing and failing builds.
        """
        failing_revision = self._get_latest_commit_for_build_state("failed")
        passing_revision = self._get_latest_commit_for_build_state("passed")
        return (
            subprocess.check_output(
                [
                    "git",
                    "rev-list",
                    "--reverse",
                    f"^{passing_revision}",
                    f"{failing_revision}~",
                ],
                cwd=self.repo_checkout,
            )
            .decode("utf-8")
            .strip()
            .split("\n")
        )

    def _find_blocked_build_and_job(
        self, commit: str
    ) -> Tuple[Optional[str], Optional[str]]:
        for build in self._get_builds():
            if build["commit"] != commit:
                continue
            if build["state"] != "blocked":
                continue
            for job in build["jobs"]:
                if job.get("step_key") != BLOCK_STEP_KEY:
                    continue

                return build["number"], job["id"]

        return None, None

    def _trigger_build(self, commit: str) -> Optional[str]:
        build, job = self._find_blocked_build_and_job(commit)
        if not build or not job:
            return None

        self.buildkite.jobs().unblock_job(
            self.buildkite_organization,
            self.buildkite_pipeline,
            build,
            job,
        )
        return build

    def _get_latest_commit_for_build_state(self, build_state: str) -> Optional[str]:
        latest_commits = self._get_latest_commits()
        commit_to_index = {commit: index for index, commit in enumerate(latest_commits)}
        builds = []
        for build in self._get_builds():
            if build["state"] == build_state and build["commit"] in latest_commits:
                builds.append(build)
        if not builds:
            return None

        builds = sorted(builds, key=lambda build: commit_to_index[build["commit"]])
        return builds[0]["commit"]

    def _get_latest_commits(self) -> List[str]:
        return (
            subprocess.check_output(
                [
                    "git",
                    "log",
                    "--pretty=tformat:%H",
                    f"--since={self.days_ago}.days",
                ],
                cwd=self.repo_checkout,
            )
            .decode("utf-8")
            .strip()
            .split("\n")
        )

    def _get_builds(self) -> List[Dict[str, Any]]:
        return self.buildkite.builds().list_all_for_pipeline(
            self.buildkite_organization,
            self.buildkite_pipeline,
            created_from=datetime.now() - timedelta(days=self.days_ago),
            branch=BRANCH,
        )
