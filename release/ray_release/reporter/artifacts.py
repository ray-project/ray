import json
import os

from ray_release.config import Test
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result

# Write to this directory. run_release_tests.sh will copy the content
# overt to DEFAULT_ARTIFACTS_DIR_HOST
DEFAULT_ARTIFACTS_DIR = "/tmp/artifacts"

ARTIFACT_TEST_CONFIG_FILE = "test_config.json"
ARTIFACT_RESULT_FILE = "result.json"


class ArtifactsReporter(Reporter):
    def __init__(self, artifacts_dir: str = DEFAULT_ARTIFACTS_DIR):
        self.artifacts_dir = artifacts_dir

    def report_result(self, test: Test, result: Result):
        if not os.path.exists(self.artifacts_dir):
            os.makedirs(self.artifacts_dir, 0o755)

        test_config_file = os.path.join(self.artifacts_dir, ARTIFACT_TEST_CONFIG_FILE)
        with open(test_config_file, "wt") as fp:
            json.dump(test, fp, sort_keys=True, indent=4)

        result_file = os.path.join(self.artifacts_dir, ARTIFACT_RESULT_FILE)
        with open(result_file, "wt") as fp:
            json.dump(result.__dict__, fp, sort_keys=True, indent=4)

        logger.info(
            f"Wrote test config and result to artifacts directory: {self.artifacts_dir}"
        )
