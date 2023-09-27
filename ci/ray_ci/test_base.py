import os
import unittest


class RayCITestBase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.update(
            {
                "RAYCI_CHECKOUT_DIR": "/ray",
                "RAYCI_BUILD_ID": "123",
                "RAYCI_WORK_REPO": "rayproject/citemp",
                "BUILDKITE_COMMIT": "123456",
                "BUILDKITE_BRANCH": "master",
                "BUILDKITE_PIPELINE_ID": "123456",
            }
        )
