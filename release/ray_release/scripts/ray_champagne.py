import click
import os
from pathlib import Path

import boto3
from anyscale.sdk.anyscale_client.models.create_cluster_environment import (
    CreateClusterEnvironment,
)
from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK
from ray_release.bazel import bazel_runfile
from ray_release.byod.build import build_champagne_image
from ray_release.logger import logger
from ray_release.configs.global_config import init_global_config

CHAMPAGNE_IMAGE_TYPES = [
    # python_version, image_type, cluster_env_name
    ("py38", "cpu", "ray-champagne-cpu"),
    ("py38", "gpu", "ray-champagne-gpu"),
]
ANYSCALE_SECRET_ARM = (
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:release-automation"
)
ANYSCALE_SECRETS = {
    "anyscale-staging-token20221014164754935800000001-pfQunc": (
        "https://console.anyscale-staging.com"
    ),
    "anyscale-demos-FaVACh": "https://console.anyscale.com",
}
CONFIG_CHOICES = click.Choice(
    [x.name for x in (Path(__file__).parent.parent / "configs").glob("*.yaml")]
)


@click.option(
    "--global-config",
    default="oss_config.yaml",
    type=CONFIG_CHOICES,
    help="Global config to use for test execution.",
)
def main(global_config: str = "oss_config.yaml") -> None:
    """
    Builds the Ray champagne image.
    """
    init_global_config(
        bazel_runfile("release/ray_release/configs", global_config),
    )
    branch = os.environ.get("BRANCH_TO_TEST", os.environ["BUILDKITE_BRANCH"])
    commit = os.environ.get("COMMIT_TO_TEST", os.environ["BUILDKITE_COMMIT"])
    assert branch.startswith(
        "releases/"
    ), f"Champagne building only supported on release branch, found {branch}"
    ray_version = f"{branch[len('releases/') :]}.{commit[:6]}"
    for python_version, image_type, cluster_env_name in CHAMPAGNE_IMAGE_TYPES:
        logger.info(f"Building champagne image for {python_version} {image_type}")
        anyscale_image = build_champagne_image(
            ray_version,
            python_version,
            image_type,
        )
        logger.info(f"Updating cluster environment {cluster_env_name}")
        _build_champaign_cluster_environment(anyscale_image, cluster_env_name)


def _build_champaign_cluster_environment(
    anyscale_image: str,
    cluster_env_name: str,
) -> None:
    boto = boto3.client("secretsmanager", region_name="us-west-2")
    for secret_name, host in ANYSCALE_SECRETS.items():
        logger.info(
            f"\tUpdating cluster environment for secret: {secret_name}, host: {host}"
        )
        os.environ["ANYSCALE_CLI_TOKEN"] = boto.get_secret_value(
            SecretId=f"{ANYSCALE_SECRET_ARM}/{secret_name}"
        )["SecretString"]
        AnyscaleSDK(host=host).build_cluster_environment(
            create_cluster_environment=CreateClusterEnvironment(
                name=cluster_env_name,
                config_json=dict(
                    docker_image=anyscale_image,
                    ray_version="nightly",
                    env_vars={},
                ),
            ),
        )


if __name__ == "__main__":
    main()
