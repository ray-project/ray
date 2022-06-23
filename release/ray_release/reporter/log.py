from ray_release.config import Test
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.util import format_link


class LogReporter(Reporter):
    def report_result(self, test: Test, result: Result):
        logger.info(
            f"Test {test['name']} finished after "
            f"{result.runtime:.2f} seconds. Last logs:\n\n"
            f"{result.last_logs}\n"
        )

        logger.info(
            f"Got the following metadata: \n"
            f"  name:    {test['name']}\n"
            f"  status:  {result.status}\n"
            f"  runtime: {result.runtime:.2f}\n"
            f"  stable:  {result.stable}\n"
            f"\n"
            f"  buildkite_url: {format_link(result.buildkite_url)}\n"
            f"  wheels_url:    {format_link(result.wheels_url)}\n"
            f"  cluster_url:   {format_link(result.cluster_url)}\n"
        )

        results = result.results
        if results:
            msg = "Observed the following results:\n\n"

            for key, val in results.items():
                msg += f"  {key} = {val}\n"
        else:
            msg = "Did not find any results."
        logger.info(msg)
