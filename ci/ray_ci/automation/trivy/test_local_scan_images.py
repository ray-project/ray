import hashlib
import sys
from pathlib import Path
from unittest import mock

import click
import pytest

from ci.ray_ci.automation.trivy import registry, scan_images
from ci.ray_ci.automation.trivy.local_scan_images import local_targets, read_images
from ci.ray_ci.automation.trivy.scan_images import check_target

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"


@pytest.fixture(autouse=True)
def no_real_processes(monkeypatch):
    """Fail any test that would run real crane or docker."""

    def refuse(args, *a, **k):
        raise AssertionError(f"test tried to run {list(args)[:3]}")

    monkeypatch.setattr(scan_images.subprocess, "run", refuse)
    monkeypatch.setattr(registry.subprocess, "run", refuse)


def digest_of(ref: str) -> str:
    return "sha256:" + hashlib.sha256(ref.encode()).hexdigest()


@pytest.mark.parametrize(
    ("image", "name", "repo"),
    [
        (
            "rayproject/ray:2.60.0-py312-cpu",
            "rayproject_ray_2.60.0-py312-cpu",
            "rayproject/ray",
        ),
        ("ubuntu", "ubuntu", "ubuntu"),
        (
            "localhost:5000/team/img",
            "localhost_5000_team_img",
            "localhost:5000/team/img",
        ),
        ("localhost:5000/img:1.0", "localhost_5000_img_1.0", "localhost:5000/img"),
        (
            "ghcr.io/o/i@sha256:" + "a" * 64,
            "ghcr.io_o_i_sha256_" + "a" * 64,
            "ghcr.io/o/i",
        ),
        (
            "ghcr.io/o/i:v1@sha256:" + "b" * 64,
            "ghcr.io_o_i_v1_sha256_" + "b" * 64,
            "ghcr.io/o/i",
        ),
    ],
)
def test_a_local_target_takes_its_name_and_repo_from_the_image(
    image: str, name: str, repo: str
):
    (target,) = local_targets([image], "x86_64")
    assert (target.name, target.repo) == (name, repo)
    target.digest = "sha256:" + "c" * 64
    assert target.pinned_ref == f"{repo}@{target.digest}"


def test_a_local_target_records_no_ray_variant():
    (target,) = local_targets(["ubuntu:22.04"], "aarch64")
    record = target.status_record()
    assert {k: record[k] for k in ("name", "image_type", "python_version")} == {
        "name": "ubuntu_22.04",
        "image_type": "image",
        "python_version": "",
    }
    assert (record["platform"], record["architecture"]) == ("", "aarch64")
    assert (record["ref"], record["published_tags"], record["status"]) == (
        "ubuntu:22.04",
        [],
        "error",
    )


def test_images_come_from_the_options_then_the_file(tmp_path: Path):
    listed = tmp_path / "images.txt"
    listed.write_text("# images for the release\nubuntu:22.04  # base\n\nalpine:3.20\n")
    assert read_images(["rayproject/ray:latest"], listed) == [
        "rayproject/ray:latest",
        "ubuntu:22.04",
        "alpine:3.20",
    ]
    assert read_images(["a"], None) == ["a"]


@pytest.mark.parametrize(
    ("images", "error"),
    [
        (["-flag"], "is not an image name"),
        (["a b"], "is not an image name"),
        (["a|b"], "is not an image name"),
        (["a`b"], "is not an image name"),
        (["ubuntu:22.04", "ubuntu:22.04"], "ubuntu:22.04 is given twice"),
        (["a/b:c", "a_b:c"], "a/b:c and a_b:c would share the result file a_b_c"),
    ],
)
def test_images_it_cannot_scan_safely_are_rejected(images: list, error: str):
    with pytest.raises(click.BadParameter, match=error):
        local_targets(images, "x86_64")


def test_some_image_is_needed():
    with pytest.raises(click.UsageError, match="at least one --image"):
        local_targets([], "x86_64")


@pytest.mark.parametrize(
    ("arch", "platform"), [("x86_64", "linux/amd64"), ("aarch64", "linux/arm64")]
)
def test_check_target_resolves_a_multi_platform_image(arch: str, platform: str):
    """Public tags usually name an index; a local target scans the image for
    its architecture, where CI refuses an index."""
    (target,) = local_targets(["rayproject/ray:2.60.0-py312-cpu"], arch)
    digest = mock.Mock(return_value="sha256:" + "d" * 64)
    with mock.patch.multiple(
        scan_images.registry,
        digest=digest,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": platform.split("/")[1]}),
    ):
        check_target(target, arch)
    digest.assert_called_once_with("rayproject/ray:2.60.0-py312-cpu", platform)
    assert (target.error, target.digest) == ("", "sha256:" + "d" * 64)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
