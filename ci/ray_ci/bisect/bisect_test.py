import click

from ci.ray_ci.utils import logger, ci_init
from ci.ray_ci.bisect.macos_validator import MacOSValidator
from ci.ray_ci.bisect.bisector import Bisector


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    ci_init()
    blame = Bisector(test_name, passing_commit, failing_commit, MacOSValidator()).run()
    logger.info(f"Blame revision: {blame}")


if __name__ == "__main__":
    main()
