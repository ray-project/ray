import sys
from datetime import datetime
from unittest import mock

import click
import pytest

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import registry, scan_images
from ci.ray_ci.automation.trivy.scan_images import (
    build_targets,
    check_target,
    parse_image,
)

WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"


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


@pytest.mark.parametrize(
    ("images", "platform"),
    [
        pytest.param([("ray-llm", "3.12")], "tpu", id="platform-the-type-lacks"),
        pytest.param([("foo", "3.12")], "cpu", id="unknown-type"),
        pytest.param([("ray", "3.12")] * 2, "cpu", id="given-twice"),
    ],
)
def test_build_targets_rejects_images_it_cannot_scan(images: list, platform: str):
    """RayImage.validate checks each field on its own, so a combination like
    ray tpu on aarch64 gets through, and fails later when crane can't find
    the image."""
    with pytest.raises(click.BadParameter):
        targets(*images, platform=platform)


@pytest.mark.parametrize(
    ("branch", "schedule", "published"),
    [
        ("releases/2.61.0", "", True),
        ("master", "nightly", True),
        # The publish step only does a dry run on these.
        ("master", "", False),
        ("someone/feature", "", False),
    ],
)
def test_published_tags_only_when_the_build_publishes(
    branch: str, schedule: str, published: bool
):
    (target,) = build_targets(
        [("ray", "3.12")],
        "cpu",
        "x86_64",
        WORK_REPO,
        "abc123",
        branch,
        COMMIT,
        schedule,
        "false",
    )
    assert bool(target.status_record()["published_tags"]) is published


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
    if expected_error:
        assert expected_error in target.error
    else:
        assert target.error == ""
    assert target.digest == ("" if expected_error else "sha256:d1")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            registry.RegistryError("crane digest x failed: boom"),
            "crane digest x failed: boom",
        ),
        (KeyError("architecture"), "KeyError: 'architecture'"),
    ],
    ids=["registry-error", "unexpected"],
)
def test_check_target_records_why_it_failed(error: Exception, expected: str):
    (target,) = targets(("ray", "3.12"))
    with mock.patch.object(scan_images.registry, "digest", side_effect=error):
        check_target(target, "x86_64")
    assert (target.error, target.digest) == (expected, "")


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
