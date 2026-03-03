from typing import Tuple

import click

from ray_release.config import (
    RELEASE_TEST_CONFIG_FILES,
    read_and_validate_release_test_collection,
)


@click.command()
@click.option(
    "--test-collection-file",
    type=str,
    multiple=True,
    help="Test collection file, relative path to ray repo.",
)
@click.option(
    "--show-disabled",
    is_flag=True,
    default=False,
    help="Show disabled tests.",
)
def main(
    test_collection_file: Tuple[str],
    show_disabled: bool,
):
    if not test_collection_file:
        test_collection_file = tuple(RELEASE_TEST_CONFIG_FILES)

    tests = read_and_validate_release_test_collection(test_collection_file)
    for test in tests:
        name = test["name"]
        python_version = test.get("python", "")
        test_frequency = test.get("frequency", "missing")
        test_team = test.get("team", "missing")
        if not show_disabled and test_frequency == "manual":
            continue

        print(f"{name} python={python_version} team={test_team}")


if __name__ == "__main__":
    main()
