import click

from ci.ray_ci.utils import ci_init
from ray_release.test import (
    Test,
    LINUX_TEST_PREFIX,
    WINDOWS_TEST_PREFIX,
    MACOS_TEST_PREFIX,
)


@click.command()
def main() -> None:
    """
    This script determines the rayci step ids to run microcheck tests.
    """
    ci_init()
    steps = (
        list(Test.gen_high_impact_tests(LINUX_TEST_PREFIX).keys())
        + list(Test.gen_high_impact_tests(WINDOWS_TEST_PREFIX).keys())
        + list(Test.gen_high_impact_tests(MACOS_TEST_PREFIX).keys())
    )

    print(",".join(steps))


if __name__ == "__main__":
    main()
