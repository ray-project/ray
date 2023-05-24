import csv
import os
from typing import Optional

import click

from ray_release.buildkite.concurrency import get_test_resources
from ray_release.config import read_and_validate_release_test_collection


@click.command()
@click.option(
    "--test-collection-file",
    default=None,
    type=str,
    help="File containing test configurations",
)
@click.option(
    "--output",
    default=None,
    type=str,
    help="CSV output file",
)
def main(test_collection_file: Optional[str] = None, output: Optional[str] = None):

    test_collection_file = test_collection_file or os.path.join(
        os.path.dirname(__file__), "..", "..", "release_tests.yaml"
    )
    output = output or os.path.join(os.path.dirname(__file__), "test_summary.csv")

    tests = read_and_validate_release_test_collection(test_collection_file)

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
