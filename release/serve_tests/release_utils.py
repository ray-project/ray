#!/usr/bin/env python
import click
import json
import logging
import requests
from typing import Dict, Optional

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


@click.group()
def cli():
    pass


def load_metrics(file_path: str) -> Dict[str, float]:
    with open(file_path) as f:
        report = json.load(f)
        if "perf_metrics" in report:
            # Input file is the TEST_OUTPUT_JSON (e.g. /tmp/release_test_out.json)
            # that is written to directly from running `python workloads/abc.py`
            results = report
        elif "results" in report:
            # Input file is either downloaded from the buildkite job artifacts
            # or from file uploaded to AWS
            results = report["results"]
        else:
            raise RuntimeError(f"Invalid results file {file_path}")

        return {
            perf_metric["perf_metric_name"]: perf_metric["perf_metric_value"]
            for perf_metric in results["perf_metrics"]
        }


@cli.command(
    help=(
        "Print a table that compares the performance metric results from two runs of "
        "the same release test."
    )
)
@click.argument("results_file1")
@click.argument("results_file2")
def compare_perf(results_file1: str, results_file2: str):
    metrics1 = load_metrics(results_file1)
    metrics2 = load_metrics(results_file2)

    print("|metric                        |results 1      |results 2      |%change   |")
    print("|------------------------------|---------------|---------------|----------|")
    for key in metrics1:
        if key in metrics2:
            change = metrics2[key] / metrics1[key] - 1
            percent_change = str(round(100 * change, 2))

            metric1 = str(round(metrics1[key], 2))
            metric2 = str(round(metrics2[key], 2))

            print(f"|{key.ljust(30)}", end="")
            print(f"|{metric1.ljust(15)}", end="")
            print(f"|{metric2.ljust(15)}", end="")
            print(f"|{percent_change.ljust(10)}|")


@cli.command(
    help=(
        "Fetch the results from the most recent nightly run of the specified release "
        "test within the past 30 days."
    )
)
@click.argument("test_name")
@click.option(
    "--output-path",
    "-o",
    type=str,
    default=None,
    help="Output file to write results to",
)
def fetch_nightly(test_name: str, output_path: Optional[str]):
    auth_header = {
        "Authorization": "Bearer bkua_6474b2bfb20dd78d44b83f197d12cb7921583ed7"
    }

    # Get job ID of the most recent run that passed on master
    # Builds returned from buildkite rest api are sorted from newest to
    # oldest already
    builds = requests.get(
        (
            "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release/"
            "builds"
        ),
        headers=auth_header,
        params={"branch": "master"},
    ).json()
    build_number = None
    job_id = None
    for build in builds:
        for job in build["jobs"]:
            job_name = job.get("name")
            if (
                job_name
                and job_name.startswith(test_name)
                and job.get("state") == "passed"
            ):
                build_number = build["number"]
                job_id = job["id"]

        if job_id:
            logger.info(
                f"Found latest release run on master with build #{build_number} and "
                f"job ID {job_id}."
            )
            break

    if job_id is None:
        raise RuntimeError(
            f"Did not find any successful runs of the release test {test_name} on "
            "master in the past 30 days."
        )

    # Get results file from Job ID
    artifacts = requests.get(
        (
            "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release/"
            f"builds/{build_number}/jobs/{job_id}/artifacts"
        ),
        headers=auth_header,
    ).json()
    results = None
    for artifact in artifacts:
        if artifact.get("filename") == "result.json":
            results = requests.get(artifact["download_url"], headers=auth_header).json()
            results = results["results"]

    if results is None:
        raise RuntimeError(f"Did not find results file for buildkite job {job_id}")

    if output_path:
        with open(output_path, "w+") as f:
            json.dump(results, f)
    else:
        print(json.dumps(results, indent=4))


if __name__ == "__main__":
    cli()
