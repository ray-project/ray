import click

from ci.ray_ci.utils import ci_init
from ci.ray_ci.automation.determine_microcheck_tests import LINUX_TEST_PREFIX
from ray_release.test import Test


@click.command()
def main() -> None:
    """
    This script determines the rayci step ids to run microcheck tests.
    """
    ci_init()
    print(",".join(Test.gen_high_impact_tests(LINUX_TEST_PREFIX).keys()))


if __name__ == "__main__":
    main()
