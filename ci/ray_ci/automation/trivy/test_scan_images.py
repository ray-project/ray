import sys
from datetime import datetime
from unittest import mock

import click
import pytest

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import scan_images
from ci.ray_ci.automation.trivy.scan_images import (
    build_targets,
    check_target,
    parse_image,
)

WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


def targets(*images: tuple, platform: str = "cpu", arch: str = "x86_64") -> list:
    return build_targets(
        list(images),
        platform,
        arch,
        WORK_REPO,
        "abc123",
        "releases/2.61.0",
        "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "",
        "false",
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [("ray:3.12", ("ray", "3.12")), ("ray-llm-extra:3.12", ("ray-llm-extra", "3.12"))],
)
def test_parse_image(spec: str, expected: tuple):
    assert parse_image(spec) == expected


@pytest.mark.parametrize("spec", ["ray", "ray:", ":3.12"])
def test_parse_image_rejects_bad_specs(spec: str):
    with pytest.raises(click.BadParameter):
        parse_image(spec)


def test_build_targets_uses_the_wanda_ref_and_published_tags():
    (target,) = targets(
        ("ray-extra", "3.12"), platform="cu12.8.1-cudnn", arch="aarch64"
    )
    assert target.name == "ray-extra-py3.12-cu12.8.1-cudnn-aarch64"
    assert target.ref == f"{WORK_REPO}:abc123-ray-extra-py3.12-cu12.8.1-cudnn-aarch64"
    assert target.repo == WORK_REPO
    record = target.status_record()
    assert record["published_tags"] == [
        "rayproject/ray:2.61.0.80ceee-extra-py312-cu128-aarch64"
    ]


def test_build_targets_rejects_unpublished_variants():
    with pytest.raises(click.BadParameter):
        targets(("ray-llm", "3.12"), platform="tpu")


@pytest.mark.parametrize(
    ("manifest_type", "arch", "expected_error"),
    [
        (OCI_MANIFEST, "amd64", ""),
        (OCI_INDEX, "amd64", "not a single-platform image manifest"),
        (OCI_MANIFEST, "arm64", "is arm64, expected amd64"),
    ],
)
def test_check_target(manifest_type: str, arch: str, expected_error: str):
    (target,) = targets(("ray", "3.12"))
    with mock.patch.multiple(
        scan_images.registry,
        manifest=mock.Mock(return_value={"mediaType": manifest_type}),
        config=mock.Mock(return_value={"architecture": arch}),
        digest=mock.Mock(return_value="sha256:d1"),
    ):
        check_target(target, "x86_64")
    assert expected_error in target.error
    assert target.digest == ("" if expected_error else "sha256:d1")


def test_check_target_validates_the_digest_it_will_scan():
    """The tag may be re-pushed at any time; the checks must apply to the
    exact digest that gets pulled and scanned."""
    (target,) = targets(("ray", "3.12"))
    seen = []

    def manifest(ref):
        seen.append(("manifest", ref))
        return {"mediaType": OCI_MANIFEST}

    def config(ref):
        seen.append(("config", ref))
        return {"architecture": "amd64"}

    with mock.patch.multiple(
        scan_images.registry,
        manifest=manifest,
        config=config,
        digest=mock.Mock(return_value="sha256:d1"),
    ):
        check_target(target, "x86_64")
    assert target.digest == "sha256:d1"
    assert seen == [
        ("manifest", f"{target.repo}@sha256:d1"),
        ("config", f"{target.repo}@sha256:d1"),
    ]


def test_recorded_tags_do_not_depend_on_when_the_scan_runs():
    """Nightly tags embed the publish date, which a scan after midnight
    can't reproduce; only tags the scan can know for sure are recorded."""
    (target,) = build_targets(
        [("ray", "3.12")],
        "cpu",
        "x86_64",
        WORK_REPO,
        "abc123",
        "master",
        "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "nightly",
        "false",
    )

    def tags_at(when: datetime) -> list:
        with mock.patch.object(push_ray_image, "datetime") as clock:
            clock.now.return_value = when
            return target.status_record()["published_tags"]

    before = tags_at(datetime(2026, 11, 12, 23, 50))
    after = tags_at(datetime(2026, 11, 13, 0, 10))
    assert before == after
    assert "rayproject/ray:nightly-py312-cpu" in before
    assert not any(".2611" in tag for tag in before)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
