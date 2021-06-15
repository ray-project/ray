from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.client.worker import TASK_WARNING_THRESHOLD

import numpy as np

import logging
import unittest


class LoggerSuite(unittest.TestCase):
    """Test client warnings are raised when many tasks are scheduled"""

    def testManyTasksWarning(self):
        with ray_start_client_server() as ray:

            @ray.remote
            def f():
                return 42

            with self.assertLogs(
                    "ray.util.client.worker", level="WARNING") as cm:
                for _ in range(TASK_WARNING_THRESHOLD + 1):
                    f.remote()
            assert any(f"More than {TASK_WARNING_THRESHOLD} remote tasks have "
                       "been scheduled." in warning for warning in cm.output)

    def testNoWarning(self):
        with ray_start_client_server() as ray:

            @ray.remote
            def f():
                return 42

            with self.assertLogs(
                    "ray.util.client.worker", level="WARNING") as cm:
                # assertLogs requires that at least one log at the specified
                # level is generated, so one dummy warning is raised
                logging.getLogger("ray.util.client.worker").warning(
                    "Dummy Log")
                for _ in range(TASK_WARNING_THRESHOLD):
                    f.remote()
            assert not any(
                f"More than {TASK_WARNING_THRESHOLD} remote tasks have been "
                "scheduled." in warning for warning in cm.output)

    def testOutboundMessageSizeWarning(self):
        with ray_start_client_server() as ray:
            large_argument = np.random.rand(100, 100, 100)

            @ray.remote
            def f(some_arg):
                return some_arg[0][0][0]

            with self.assertLogs(
                    "ray.util.client.worker", level="WARNING") as cm:
                for _ in range(50):
                    f.remote(large_argument)
            assert any(
                "More than 10MB of messages have been created to schedule "
                "tasks on the server." in warning for warning in cm.output)
