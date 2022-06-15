import unittest

import numpy as np
import pytest

from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.debug import _logged


@pytest.fixture(autouse=True)
def reset_debug_logs():
    """Resets internal state of ray.util.debug so that warning can be tested
    more than once in the same process"""
    _logged.clear()


class LoggerSuite(unittest.TestCase):
    """Test client warnings are raised when many tasks are scheduled"""

    def testOutboundMessageSizeWarning(self):
        with ray_start_client_server() as ray:
            large_argument = np.random.rand(100, 100, 100)

            @ray.remote
            def f(some_arg):
                return some_arg[0][0][0]

            with self.assertWarns(UserWarning) as cm:
                for _ in range(50):
                    f.remote(large_argument)
            assert (
                "More than 10MB of messages have been created to "
                "schedule tasks on the server." in cm.warning.args[0]
            )


if __name__ == "__main__":
    import os
    import sys

    import pytest

    from ray._private.test_utils import run_pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(run_pytest(__file__))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
