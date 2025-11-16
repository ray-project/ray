import os
import unittest
from unittest.mock import patch

from ci.ray_ci.configs import DEFAULT_PYTHON_VERSION, PYTHON_VERSIONS
from ci.ray_ci.utils import ci_init


class RayCITestBase(unittest.TestCase):
    def setUp(self) -> None:
        ci_init()
        self.patcher = patch.dict(
            os.environ,
            {
                "RAYCI_CHECKOUT_DIR": "/ray",
                "RAYCI_BUILD_ID": "a1b2c3d4",
                "RAYCI_WORK_REPO": "rayproject/citemp",
                "BUILDKITE_COMMIT": "123456",
                "BUILDKITE_BRANCH": "master",
                "BUILDKITE_PIPELINE_ID": "123456",
            },
        )
        self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()

    def get_non_default_python(self) -> str:
        for version in PYTHON_VERSIONS.keys():
            if version != DEFAULT_PYTHON_VERSION:
                return version

    def get_python_version(self, version: str) -> str:
        return f"py{version.replace('.', '')}"  # 3.x -> py3x

    def get_cpp_version(self, version: str) -> str:
        return f"cp{version.replace('.', '')}"  # 3.x -> cp3x
