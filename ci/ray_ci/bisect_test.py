import click

from ci.ray_ci.utils import logger, ci_init
from ci.ray_ci.macos_bisector import MacOSBisector


@click.command()
@click.argument("test_name", required=True, type=str)
@click.argument("passing_commit", required=True, type=str)
@click.argument("failing_commit", required=True, type=str)
def main(test_name: str, passing_commit: str, failing_commit: str) -> None:
    ci_init()
    blame = MacOSBisector(test_name, passing_commit, failing_commit).run()
    logger.info(f"Blame revision: {blame}")


if __name__ == "__main__":
    main()
