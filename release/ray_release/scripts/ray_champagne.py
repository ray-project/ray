import os

from anyscale.sdk.anyscale_client.models.create_cluster_environment import (
    CreateClusterEnvironment,
)

from ray_release.aws import maybe_fetch_api_token
from ray_release.byod.build import build_champagne_image
from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk

CHAMPAGNE_IMAGE_TYPES = [
    # python_version, image_type, cluster_env_name
    ("py38", "cpu", "ray-champagne-cpu"),
    ("py38", "gpu", "ray-champagne-gpu"),
]


def main() -> None:
    """
    Builds the Ray champagne image.
    """
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
    maybe_fetch_api_token()
    get_anyscale_sdk().build_cluster_environment(
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
