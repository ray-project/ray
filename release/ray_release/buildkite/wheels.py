import os
from typing import Optional

import boto3
from pybuildkite.buildkite import Buildkite
from ray_release.exception import RayWheelsNotFoundError, RayWheelsNoPRError

BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_PR_CI_PIPELINE = "ray-builders-pr"


def maybe_fetch_buildkite_token():
    if os.environ.get("BUILDKITE_TOKEN", None) is None:
        print("Missing BUILDKITE_TOKEN, retrieving from AWS secrets store")
        os.environ["BUILDKITE_TOKEN"] = boto3.client(
            "secretsmanager", region_name="us-west-2"
        ).get_secret_value(
            SecretId="arn:aws:secretsmanager:us-west-2:029272617770:secret:"
            "buildkite/ro-token"
        )[
            "SecretString"
        ]


def get_buildkite() -> Buildkite:
    buildkite = Buildkite()
    maybe_fetch_buildkite_token()
    buildkite.set_access_token(os.environ.get("BUILDKITE_TOKEN"))
    return buildkite


def find_wheel_for_pr(
    branch_and_repo: str,
    wheel_name: str,
    commit: Optional[str] = None,
    buildkite: Optional[Buildkite] = None,
) -> str:
    buildkite = buildkite or get_buildkite()
    builds = buildkite.builds().list_all_for_pipeline(
        organization=BUILDKITE_ORGANIZATION,
        pipeline=BUILDKITE_PR_CI_PIPELINE,
        branch=branch_and_repo,
    )

    if not builds:
        raise RayWheelsNoPRError(
            f"Buildkite build for branch/repo `{branch_and_repo}` not found. "
            f"Did you create a PR for this branch?"
        )

    use_build = None
    if commit:
        for build in builds:
            if build["commit"] == commit:
                use_build = build
                break
    else:
        use_build = builds[2]

    if not use_build:
        raise RayWheelsNoPRError(
            f"Buildkite commit for branch/repo `{branch_and_repo}` not found: "
            f"{commit} - did you push this commit to your PR, yet?"
        )

    artifacts = buildkite.artifacts().list_artifacts_for_build(
        organization="ray-project",
        pipeline="ray-builders-pr",
        build=use_build["number"],
    )

    wheel_artifact = None
    for artifact in artifacts:
        if artifact["filename"] == wheel_name:
            wheel_artifact = artifact
            break

    if not wheel_artifact:
        raise RayWheelsNotFoundError(
            f"No wheels found for branch/repo `{branch_and_repo}` and "
            f"build {use_build['number']}. Maybe the wheels build failed or "
            f"you just have to wait until they are ready."
        )

    url = wheel_artifact["download_url"]
    return url
