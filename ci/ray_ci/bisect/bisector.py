import subprocess
from typing import List, Optional

from ray_release.test import Test

from ci.ray_ci.bisect.validator import Validator
from ci.ray_ci.utils import logger


class Bisector:
    def __init__(
        self,
        test: Test,
        passing_revision: str,
        failing_revision: str,
        validator: Validator,
        git_dir: str,
    ) -> None:
        self.test = test
        self.passing_revision = passing_revision
        self.failing_revision = failing_revision
        self.validator = validator
        self.git_dir = git_dir

    def run(self) -> Optional[str]:
        """
        Find the blame revision for the test given the range of passing and failing
        revision. If a blame cannot be found, return None
        """
        revisions = self._get_revision_lists()
        if len(revisions) < 2:
            return None
        while len(revisions) > 2:
            logger.info(
                f"Bisecting between {len(revisions)} revisions: "
                f"{revisions[0]} to {revisions[-1]}"
            )
            mid = len(revisions) // 2
            if self._checkout_and_validate(revisions[mid]):
                revisions = revisions[mid:]
            else:
                revisions = revisions[: (mid + 1)]

        return revisions[-1]

    def _get_revision_lists(self) -> List[str]:
        return (
            subprocess.check_output(
                [
                    "git",
                    "rev-list",
                    "--reverse",
                    f"^{self.passing_revision}~",
                    self.failing_revision,
                ],
                cwd=self.git_dir,
            )
            .decode("utf-8")
            .strip()
            .split("\n")
        )

    def _checkout_and_validate(self, revision: str) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        subprocess.check_call(["git", "clean", "-df"], cwd=self.git_dir)
        subprocess.check_call(["git", "checkout", revision], cwd=self.git_dir)
        return self.validator.run(self.test, revision)
