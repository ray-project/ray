import os
import unittest
from unittest.mock import patch


class RayCITestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch.dict(
            os.environ,
            {
                "RAYCI_CHECKOUT_DIR": "/ray",
                "RAYCI_BUILD_ID": "123",
                "RAYCI_WORK_REPO": "rayproject/citemp",
                "BUILDKITE_COMMIT": "123456",
                "BUILDKITE_BRANCH": "master",
                "BUILDKITE_PIPELINE_ID": "123456",
            },
        )
        self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()
