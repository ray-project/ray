import os
import subprocess

from ci.ray_ci.bisector import Bisector
from ray_release.bazel import bazel_runfile


TEST_SCRIPT = "ci/ray_ci/macos_ci_test.sh"


class MacOSBisector(Bisector):
    def validate(self, revision: str) -> bool:
        return (
            subprocess.run(
                [f"{bazel_runfile(TEST_SCRIPT)}", self.test],
                cwd=os.environ["RAYCI_CHECKOUT_DIR"],
            ).returncode
            == 0
        )
