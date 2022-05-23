from ray_release.config import Test
from ray_release.result import Result


class Reporter:
    """Reporter interface"""

    def report_result(self, test: Test, result: Result):
        raise NotImplementedError
