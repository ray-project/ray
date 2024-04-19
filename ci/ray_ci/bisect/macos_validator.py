import os
import subprocess

from ci.ray_ci.bisect.validator import Validator
from ray_release.bazel import bazel_runfile


TEST_SCRIPT = "ci/ray_ci/bisect/macos_validator.sh"


class MacOSValidator(Validator):
    def run(self, test: str) -> bool:
        return (
            subprocess.run(
                [f"{bazel_runfile(TEST_SCRIPT)}", "run_tests", test],
                cwd=os.environ["RAYCI_CHECKOUT_DIR"],
            ).returncode
            == 0
        )
