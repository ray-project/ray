"""
Diff test to ensure ReleaseTestImagePushContext produces identical tags
to the original AnyscaleDockerContainer/DockerContainer implementation.
"""

import os
import sys
from unittest import mock

import pytest

from ci.ray_ci.anyscale_docker_container import AnyscaleDockerContainer
from ci.ray_ci.automation.push_release_test_image import ReleaseTestImagePushContext
from ci.ray_ci.docker_container import GPU_PLATFORM, RayType

# Test matrix covering various scenarios
TEST_CASES = [
    # (python_version, platform, image_type, branch, commit, build_id, pr)
    # Master branch scenarios
    ("3.11", "cpu", "ray", "master", "abc123def456", "build-123", "false"),
    ("3.11", "cpu", "ray-ml", "master", "abc123def456", "build-123", "false"),
    ("3.11", GPU_PLATFORM, "ray", "master", "abc123def456", "build-123", "false"),
    ("3.11", GPU_PLATFORM, "ray-ml", "master", "abc123def456", "build-123", "false"),
    ("3.10", "cpu", "ray", "master", "abc123def456", "build-123", "false"),
    ("3.10", GPU_PLATFORM, "ray-ml", "master", "abc123def456", "build-123", "false"),
    # Release branch scenarios
    ("3.11", "cpu", "ray", "releases/2.44.0", "abc123def456", "build-456", "false"),
    (
        "3.11",
        GPU_PLATFORM,
        "ray-ml",
        "releases/2.44.0",
        "abc123def456",
        "build-456",
        "false",
    ),
    ("3.10", "cpu", "ray", "releases/2.44.0", "abc123def456", "build-456", "false"),
    # PR scenarios
    ("3.11", "cpu", "ray", "feature-branch", "abc123def456", "build-789", "123"),
    (
        "3.11",
        GPU_PLATFORM,
        "ray-ml",
        "feature-branch",
        "abc123def456",
        "build-789",
        "456",
    ),
    # Feature branch (no PR)
    ("3.11", "cpu", "ray", "feature-branch", "abc123def456", "build-789", "false"),
    # Other CUDA versions (not GPU_PLATFORM) - only valid for ray, not ray-ml
    ("3.11", "cu12.3.2-cudnn9", "ray", "master", "abc123def456", "build-123", "false"),
    # Extra image types
    ("3.11", "cpu", "ray-extra", "master", "abc123def456", "build-123", "false"),
    (
        "3.11",
        GPU_PLATFORM,
        "ray-ml-extra",
        "master",
        "abc123def456",
        "build-123",
        "false",
    ),
]


def image_type_to_ray_type(image_type: str) -> RayType:
    """Convert string image type to RayType enum."""
    return RayType(image_type)


class TestAnyscaleImageTagsDiff:
    """Test that new implementation matches original exactly."""

    @pytest.mark.parametrize(
        (
            "python_version",
            "platform",
            "image_type",
            "branch",
            "commit",
            "build_id",
            "pr",
        ),
        TEST_CASES,
    )
    def test_tags_match_original(
        self,
        python_version: str,
        platform: str,
        image_type: str,
        branch: str,
        commit: str,
        build_id: str,
        pr: str,
    ):
        """Compare tags from new ReleaseTestImagePushContext with original DockerContainer."""
        env = {
            "RAYCI_CHECKOUT_DIR": "/ray",
            "RAYCI_BUILD_ID": build_id,
            "RAYCI_WORK_REPO": "rayproject/citemp",
            "BUILDKITE_COMMIT": commit,
            "BUILDKITE_BRANCH": branch,
            "BUILDKITE_PIPELINE_ID": "123456",
            "BUILDKITE_PULL_REQUEST": pr,
        }

        with mock.patch.dict(os.environ, env, clear=False):
            # Create original container (AnyscaleDockerContainer inherits from DockerContainer)
            original = AnyscaleDockerContainer(
                python_version=python_version,
                platform=platform,
                image_type=image_type_to_ray_type(image_type),
                upload=False,
            )
            original_tags = original._get_image_tags()

            # Create new context
            new_ctx = ReleaseTestImagePushContext(
                image_type=image_type,
                python_version=python_version,
                platform=platform,
                branch=branch,
                commit=commit,
                rayci_build_id=build_id,
                pull_request=pr,
            )
            new_tags = new_ctx.destination_tags()

            # Compare - sort both to ignore order differences
            assert sorted(original_tags) == sorted(new_tags), (
                f"Tags mismatch for {image_type} py{python_version} {platform} on {branch}\n"
                f"Original: {sorted(original_tags)}\n"
                f"New:      {sorted(new_tags)}"
            )

    @pytest.mark.parametrize(
        (
            "python_version",
            "platform",
            "image_type",
            "branch",
            "commit",
            "build_id",
            "pr",
        ),
        TEST_CASES,
    )
    def test_wanda_image_name_format(
        self,
        python_version: str,
        platform: str,
        image_type: str,
        branch: str,
        commit: str,
        build_id: str,
        pr: str,
    ):
        """Verify wanda image name follows expected format."""
        new_ctx = ReleaseTestImagePushContext(
            image_type=image_type,
            python_version=python_version,
            platform=platform,
            branch=branch,
            commit=commit,
            rayci_build_id=build_id,
            pull_request=pr,
        )

        wanda_name = new_ctx.wanda_image_name()

        # Wanda image name should be: {image_type}-anyscale-py{version}-{platform}
        assert wanda_name.startswith(f"{image_type}-anyscale-py{python_version}-")
        if platform == "cpu":
            assert wanda_name.endswith("-cpu")
        else:
            assert wanda_name.endswith(f"-{platform}")

    @pytest.mark.parametrize(
        (
            "python_version",
            "platform",
            "image_type",
            "branch",
            "commit",
            "build_id",
            "pr",
        ),
        TEST_CASES,
    )
    def test_canonical_tag_matches(
        self,
        python_version: str,
        platform: str,
        image_type: str,
        branch: str,
        commit: str,
        build_id: str,
        pr: str,
    ):
        """Verify the canonical (first) tag matches between implementations."""
        env = {
            "RAYCI_CHECKOUT_DIR": "/ray",
            "RAYCI_BUILD_ID": build_id,
            "RAYCI_WORK_REPO": "rayproject/citemp",
            "BUILDKITE_COMMIT": commit,
            "BUILDKITE_BRANCH": branch,
            "BUILDKITE_PIPELINE_ID": "123456",
            "BUILDKITE_PULL_REQUEST": pr,
        }

        with mock.patch.dict(os.environ, env, clear=False):
            original = AnyscaleDockerContainer(
                python_version=python_version,
                platform=platform,
                image_type=image_type_to_ray_type(image_type),
                upload=False,
            )
            original_canonical = original._get_canonical_tag()

            new_ctx = ReleaseTestImagePushContext(
                image_type=image_type,
                python_version=python_version,
                platform=platform,
                branch=branch,
                commit=commit,
                rayci_build_id=build_id,
                pull_request=pr,
            )
            new_canonical = new_ctx.destination_tags()[0]

            assert original_canonical == new_canonical, (
                f"Canonical tag mismatch for {image_type} py{python_version} {platform}\n"
                f"Original: {original_canonical}\n"
                f"New:      {new_canonical}"
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
