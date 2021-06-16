from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.client.worker import TASK_WARNING_THRESHOLD
from ray.util.debug import _logged

import numpy as np
import pytest

import unittest


@pytest.fixture(autouse=True)
def reset_debug_logs():
    """Resets internal state of ray.util.debug so that warning can be tested
    for more than once in the same process"""
    _logged.clear()


class LoggerSuite(unittest.TestCase):
    """Test client warnings are raised when many tasks are scheduled"""

    def testManyTasksWarning(self):
        with ray_start_client_server() as ray:

            @ray.remote
            def f():
                return 42

            with self.assertWarns(UserWarning) as cm:
                for _ in range(TASK_WARNING_THRESHOLD + 1):
                    f.remote()
            assert f"More than {TASK_WARNING_THRESHOLD} remote tasks have " \
                "been scheduled." in cm.warning.args[0]

    def testNoWarning(self):
        with ray_start_client_server() as ray:

            @ray.remote
            def f():
                return 42

            no_warning_raised = False
            try:
                with self.assertWarns(UserWarning) as cm:
                    for _ in range(TASK_WARNING_THRESHOLD):
                        f.remote()
            except AssertionError:
                # assertion above failed, meaning no warning was raised
                no_warning_raised = True
            if not no_warning_raised:
                raise AssertionError("The following warning was raised:\n "
                                     f"{cm.warning}\n Expected no warnings.")

    def testOutboundMessageSizeWarning(self):
        with ray_start_client_server() as ray:
            large_argument = np.random.rand(100, 100, 100)

            @ray.remote
            def f(some_arg):
                return some_arg[0][0][0]

            with self.assertWarns(UserWarning) as cm:
                for _ in range(50):
                    f.remote(large_argument)
            assert "More than 10MB of messages have been created to " \
                "schedule tasks on the server." in cm.warning.args[0]
