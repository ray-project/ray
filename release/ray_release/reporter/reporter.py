from ray_release.logger import logger
from ray_release.result import Result
from ray_release.test import Test


class Reporter:
    """Reporter interface"""

    def report_result(self, test: Test, result: Result):
        try:
            self.report_result(test, result)
        except Exception as e:
            logger.exception(f"Error reporting results via {type(self)}: {e}")

    def report_result_ex(self, test: Test, result: Result):
        raise NotImplementedError
