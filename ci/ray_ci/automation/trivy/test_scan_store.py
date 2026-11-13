import sys
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import (
    BUCKET,
    ScanStore,
    StoreError,
    build_options,
)

COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"


def store(client=None, **overrides) -> ScanStore:
    args = {
        "pipeline": "postmerge",
        "branch": "master",
        "build_number": "20897",
        "commit": COMMIT,
        **overrides,
    }
    return ScanStore.for_build(**args, client=client or FakeS3())


@pytest.mark.parametrize(
    ("branch", "prefix"),
    [
        ("master", "trivy-scans/postmerge/master/20897-80ceee083e49"),
        (
            "releases/2.60.0",
            "trivy-scans/postmerge/releases/2.60.0/20897-80ceee083e49",
        ),
        # Nothing a branch name holds can climb out of, or hide, the folder.
        ("a/../b", "trivy-scans/postmerge/a/b/20897-80ceee083e49"),
        ("/a//b/", "trivy-scans/postmerge/a/b/20897-80ceee083e49"),
        ("me:fix it", "trivy-scans/postmerge/me_fix_it/20897-80ceee083e49"),
    ],
)
def test_the_folder_is_named_after_the_build(branch: str, prefix: str):
    s = store(branch=branch)
    assert s.prefix == prefix
    assert s.url("results") == f"s3://{BUCKET}/{prefix}/results/"


@pytest.mark.parametrize(
    "overrides",
    [
        {"build_number": "12a"},
        {"build_number": ""},
        {"commit": "80ceee"},
        {"commit": "not-a-sha-not-a-sha"},
        {"pipeline": ""},
        {"branch": "/"},
    ],
)
def test_a_build_that_cannot_be_named_is_rejected(overrides: dict):
    with pytest.raises(StoreError):
        store(**overrides)


def test_upload_download_and_list(tmp_path: Path):
    s3 = FakeS3()
    s = store(s3)
    for name in ("b.json", "a.json", "c.json"):
        (tmp_path / name).write_text(name)
        s.upload(tmp_path / name, "results")
    s3.put(BUCKET, f"{s.prefix}/results/nested/x.json", b"")
    s3.put(BUCKET, f"{s.prefix}/db/metadata.json", b"")

    assert s.names("results") == ["a.json", "b.json", "c.json"]
    dest = s.download("results", "b.json", tmp_path / "copy.json")
    assert dest.read_text() == "b.json"
    assert s3.get(BUCKET, f"{s.prefix}/results/a.json") == b"a.json"


def test_s3_errors_become_store_errors(tmp_path: Path):
    s3 = FakeS3()
    s = store(s3)
    with pytest.raises(StoreError, match="results/missing.json"):
        s.download("results", "missing.json", tmp_path / "x")
    s3.denied.add(f"{s.prefix}/results/a.json")
    (tmp_path / "a.json").write_text("{}")
    with pytest.raises(StoreError, match="AccessDenied"):
        s.upload(tmp_path / "a.json", "results")


def test_build_options_read_buildkite_variables():
    @click.command()
    @build_options
    def show(store: ScanStore) -> None:
        click.echo(store.url())

    env = {
        "BUILDKITE_PIPELINE_SLUG": "postmerge",
        "BUILDKITE_BRANCH": "releases/2.60.0",
        "BUILDKITE_BUILD_NUMBER": "7",
        "BUILDKITE_COMMIT": COMMIT,
    }
    result = CliRunner().invoke(show, [], env=env)
    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        f"s3://{BUCKET}/trivy-scans/postmerge/releases/2.60.0/7-80ceee083e49/"
    )

    result = CliRunner().invoke(show, [], env={**env, "BUILDKITE_BUILD_NUMBER": "x"})
    assert result.exit_code == 2
    assert "build number" in result.output


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
