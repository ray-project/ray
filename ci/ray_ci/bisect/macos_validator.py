import os
import subprocess

from ray_release.bazel import bazel_runfile
from ray_release.test import Test

from ci.ray_ci.bisect.validator import Validator

TEST_SCRIPT = "ci/ray_ci/bisect/macos_validator.sh"


class MacOSValidator(Validator):
    def run(self, test: Test, revision: str) -> bool:
        env = os.environ.copy()
        # We need to unset PYTHONPATH to avoid conflicts with the Python from the
        # Bazel runfiles.
        env.update({"RAYCI_BISECT_RUN": "1", "PYTHONPATH": ""})
        return (
            subprocess.run(
                [f"{bazel_runfile(TEST_SCRIPT)}", "run_tests", test.get_target()],
                cwd=os.environ["RAYCI_CHECKOUT_DIR"],
                env=env,
            ).returncode
            == 0
        )
