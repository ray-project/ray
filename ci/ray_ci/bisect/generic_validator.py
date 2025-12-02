import time

from pybuildkite.buildkite import Buildkite

from ci.ray_ci.bisect.validator import Validator
from ci.ray_ci.utils import logger

from ray_release.aws import get_secret_token
from ray_release.configs.global_config import get_global_config
from ray_release.test import Test

BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_POSTMERGE_PIPELINE = "postmerge"
BUILDKITE_BUILD_RUNNING_STATE = [
    "creating",
    "scheduled",
    "running",
]
BUILDKITE_BUILD_PASSING_STATE = [
    "passed",
    "skipped",
]
BUILDKITE_BUILD_FAILING_STATE = [
    "failing",
    "failed",
    "blocked",
    "canceled",
    "canceling",
    "not_run",
]
TIMEOUT = 2 * 60 * 60  # 2 hours
WAIT = 10  # 10 seconds


class GenericValidator(Validator):
    def _get_buildkite(self) -> Buildkite:
        buildkite = Buildkite()
        buildkite.set_access_token(
            get_secret_token(get_global_config()["ci_pipeline_buildkite_secret"]),
        )

        return buildkite

    def _get_rayci_select(self, test: Test) -> str:
        return test.get_test_results(limit=1)[0].rayci_step_id

    def run(self, test: Test, revision: str) -> bool:
        buildkite = self._get_buildkite()
        build = buildkite.builds().create_build(
            BUILDKITE_ORGANIZATION,
            BUILDKITE_POSTMERGE_PIPELINE,
            revision,
            "master",
            message=f"[bisection] running single test: {test.get_name()}",
            env={
                "RAYCI_SELECT": self._get_rayci_select(test),
                "RAYCI_BISECT_TEST_TARGET": test.get_target(),
            },
        )
        total_wait = 0
        while True:
            logger.info(f"... waiting for test result ...({total_wait} seconds)")
            time.sleep(WAIT)
            build = buildkite.builds().get_build_by_number(
                BUILDKITE_ORGANIZATION,
                BUILDKITE_POSTMERGE_PIPELINE,
                build["number"],
            )

            # return build status
            if build["state"] in BUILDKITE_BUILD_PASSING_STATE:
                return True
            if build["state"] in BUILDKITE_BUILD_FAILING_STATE:
                return False

            # continue waiting
            total_wait += WAIT
            if total_wait > TIMEOUT:
                logger.error("Timeout")
                return False
