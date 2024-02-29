import click
import sys

from ray_release.aws import get_secret_token

AWS_SECRET_TEST_PYPI = "ray_ci_test_pypi_token"
AWS_SECRET_PYPI = "ray_ci_pypi_token"


@click.command()
@click.option("--env", required=True, type=click.Choice(["test", "prod"]))
def main(env: str) -> None:
    """
    Retrieve the pypi token from AWS secrets manager.
    """
    if env == "test":
        sys.stdout.write(get_secret_token(AWS_SECRET_TEST_PYPI))
    else:
        sys.stdout.write(get_secret_token(AWS_SECRET_PYPI))


if __name__ == "__main__":
    main()
