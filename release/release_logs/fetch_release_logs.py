"""
This script will automatically fetch the latest release logs from the
OSS release testing pipeline on buildkite.

Specifically, this will loop through all release test pipeline builds for the
specified Ray version and fetch the latest available results from the respective
tests. It will then write these to the directory in `ray/release/release_logs`.

To use this script, either set the BUILDKITE_TOKEN environment variable to a
valid Buildkite API token with read access, or authenticate in AWS with the
OSS CI account.

Usage:

    python fetch_release_logs.py <version>

Example:

    python fetch_release_logs 1.13.0rc0

Results in:

    Fetched microbenchmark.json for commit 025e4b01822214e03907db0b09f3af17203a6671
    ...
    Writing 1.13.0rc0/microbenchmark.json
    ...

"""
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import click
from pybuildkite.buildkite import Buildkite

BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_PIPELINE = "release-tests-branch"

# Format: job name regex --> filename to save results to
RESULTS_TO_FETCH = {
    r"^microbenchmark \(.+\)$": "microbenchmark.json",
    r"^many_actors \(.+\)$": "benchmarks/many_actors.json",
    r"^many_nodes \(.+\)$": "benchmarks/many_nodes.json",
    r"^many_pgs \(.+\)$": "benchmarks/many_pgs.json",
    r"^many_tasks \(.+\)$": "benchmarks/many_tasks.json",
    r"^object_store \(.+\)$": "scalability/object_store.json",
    r"^single_node \(.+\)$": "scalability/single_node.json",
    r"^stress_test_dead_actors \(.+\)$": "stress_tests/stress_test_dead_actors.json",
    r"^stress_test_many_tasks \(.+\)$": "stress_tests/stress_test_many_tasks.json",
    r"^stress_test_placement_group \(.+\)$": (
        "stress_tests/" "stress_test_placement_group.json"
    ),
}


@dataclass
class Build:
    id: str
    number: int
    commit: str
    job_dict_list: List[Dict]

    pipeline: str = BUILDKITE_PIPELINE
    organization: str = BUILDKITE_ORGANIZATION


@dataclass
class Job:
    build: Build
    id: str
    name: Optional[str]


@dataclass
class Artifact:
    job: Job
    id: str


def get_buildkite_api() -> Buildkite:
    bk = Buildkite()
    buildkite_token = maybe_fetch_buildkite_token()
    bk.set_access_token(buildkite_token)
    return bk


def maybe_fetch_buildkite_token() -> str:
    buildkite_token = os.environ.get("BUILDKITE_TOKEN", None)

    if buildkite_token:
        return buildkite_token

    print("Missing BUILDKITE_TOKEN, retrieving from AWS secrets store")
    buildkite_token = boto3.client(
        "secretsmanager", region_name="us-west-2"
    ).get_secret_value(
        SecretId="arn:aws:secretsmanager:us-west-2:029272617770:secret:"
        "buildkite/ro-token"
    )[
        "SecretString"
    ]
    os.environ["BUILDKITE_TOKEN"] = buildkite_token
    return buildkite_token


def get_results_from_build_collection(
    bk: Buildkite, build_dict_list: List[Dict]
) -> Dict[str, Dict]:
    results_to_fetch = RESULTS_TO_FETCH.copy()
    fetched_results = {}

    for build_dict in sorted(build_dict_list, key=lambda bd: -bd["number"]):
        if not results_to_fetch:
            break

        build = Build(
            id=build_dict["id"],
            number=build_dict["number"],
            commit=build_dict["commit"],
            job_dict_list=build_dict["jobs"],
        )
        build_results = get_results_from_build(bk, build, results_to_fetch)
        fetched_results.update(build_results)

    return fetched_results


def get_results_from_build(bk: Buildkite, build: Build, results_to_fetch: Dict) -> Dict:
    fetched_results = {}

    for job_dict in build.job_dict_list:
        if not results_to_fetch:
            break

        job = Job(build=build, id=job_dict["id"], name=job_dict.get("name", None))

        if not job.name:
            continue

        for job_regex, filename in list(results_to_fetch.items()):
            if re.match(job_regex, job.name):
                result = get_results_artifact_for_job(bk, job=job)

                if not result:
                    continue

                fetched_results[filename] = result
                results_to_fetch.pop(job_regex)
                print(f"Fetched {filename} for commit {job.build.commit}")

    return fetched_results


def get_results_artifact_for_job(bk: Buildkite, job: Job) -> Optional[Dict]:
    artifacts = bk.artifacts().list_artifacts_for_job(
        organization=job.build.organization,
        pipeline=job.build.pipeline,
        build=job.build.number,
        job=job.id,
    )
    for artifact in artifacts:
        if "result.json" in artifact["filename"]:
            artifact = Artifact(job=job, id=artifact["id"])
            return download_results_artifact(bk=bk, artifact=artifact)
    return None


def download_results_artifact(bk: Buildkite, artifact: Artifact) -> Dict:
    blob = bk.artifacts().download_artifact(
        organization=artifact.job.build.organization,
        pipeline=artifact.job.build.pipeline,
        build=artifact.job.build.number,
        job=artifact.job.id,
        artifact=artifact.id,
    )
    data_dict = json.loads(blob)
    return data_dict.get("results", {})


def write_results(log_dir: Path, fetched_results: Dict[str, Any]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)

    for filepath, content in fetched_results.items():
        path = log_dir.joinpath(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Writing {path}")

        with open(path, "w") as fp:
            json.dump(content, fp, sort_keys=True, indent=4)
            fp.write("\n")


@click.command()
@click.argument("version", required=True)
def main(version: str):
    log_dir = Path(__file__).parent.joinpath(version)
    branch = f"releases/{version}"

    bk = get_buildkite_api()
    build_dict_list = bk.builds().list_all_for_pipeline(
        organization=BUILDKITE_ORGANIZATION, pipeline=BUILDKITE_PIPELINE, branch=branch
    )

    fetched_results = get_results_from_build_collection(bk, build_dict_list)
    write_results(log_dir, fetched_results)


if __name__ == "__main__":
    main()
