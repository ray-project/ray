from ray_release.config import Test
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result


class RDSReporter(Reporter):
    def report_result(self, test: Test, result: Result):
        raise NotImplementedError
