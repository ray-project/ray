import os

from ray_release.byod.build import build_anyscale_champagne_image


CHAMPAGNE_IMAGE_TYPES = [
    # python_version, image_type
    ("py38", "cpu"),
    ("py38", "gpu"),
]


def main() -> None:
    """
    Builds the Anyscale champagne image.
    """
    branch = os.environ.get("BRANCH_TO_TEST", os.environ["BUILDKITE_BRANCH"])
    commit = os.environ.get("COMMIT_TO_TEST", os.environ["BUILDKITE_COMMIT"])
    assert branch.startswith(
        "releases/"
    ), f"Champagne building only supported on release branch, found {branch}"
    ray_version = f"{branch[len('releases/') :]}.{commit[:6]}"
    for python_version, image_type in CHAMPAGNE_IMAGE_TYPES:
        build_anyscale_champagne_image(ray_version, python_version, image_type)


if __name__ == "__main__":
    main()
