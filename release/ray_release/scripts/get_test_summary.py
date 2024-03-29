import csv
import os
from typing import Optional

import click

from ray_release.buildkite.concurrency import get_test_resources
from ray_release.config import read_and_validate_release_test_collection


@click.command()
@click.option(
    "--test-collection-file",
    multiple=True,
    type=str,
    help="Test collection file, relative path to ray repo.",
)
@click.option(
    "--output",
    default=None,
    type=str,
    help="CSV output file",
)
def main(test_collection_file: Optional[str] = None, output: Optional[str] = None):

    output = output or os.path.join(os.path.dirname(__file__), "test_summary.csv")

    tests = read_and_validate_release_test_collection(
        test_collection_file or ["release/release_tests.yaml"]
    )

    with open(output, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "group", "num_cpus", "num_gpus"])
        writer.writeheader()

        for test in tests:
            name = test["name"]
            cpus, gpus = get_test_resources(test)
            group = test["group"]
            writer.writerow(
                {
                    "name": name,
                    "group": group,
                    "num_cpus": cpus,
                    "num_gpus": gpus,
                }
            )


if __name__ == "__main__":
    main()
