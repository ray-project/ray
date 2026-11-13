import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import click
import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import scan_store
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import (
    BUCKET,
    MAX_POOL_CONNECTIONS,
    ScanStore,
    StoreError,
    build_options,
)

COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"
BUILD = {
    "pipeline": "postmerge",
    "branch": "master",
    "build_number": "20897",
    "commit": COMMIT,
}


def store(client=None, **overrides) -> ScanStore:
    args = {**BUILD, **overrides}
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
        {"commit": ""},
        {"commit": "/.."},
        {"pipeline": ""},
        {"branch": "/"},
    ],
)
def test_a_build_that_cannot_be_named_is_rejected(overrides: dict):
    with pytest.raises(StoreError):
        store(**overrides)
    # Building the store directly checks the name just the same.
    with pytest.raises(StoreError):
        ScanStore(**{**BUILD, **overrides})


@pytest.mark.parametrize(
    ("commit", "folder"),
    [
        ("HEAD", "20897-HEAD"),
        ("80ceee", "20897-80ceee"),
        ("a/b c", "20897-a_b_c"),
    ],
)
def test_a_commit_that_is_not_a_sha_still_names_a_folder(commit: str, folder: str):
    """Only a build's first job can see HEAD, before Buildkite resolves it.
    The Trivy jobs all run after rayci's pipeline upload, so they all name
    the same folder; any other commit text still names one."""
    s = store(commit=commit)
    assert s.prefix == f"trivy-scans/postmerge/master/{folder}"
    assert s.commit == commit


def test_upload_and_download(tmp_path: Path):
    s3 = FakeS3()
    s = store(s3)
    (tmp_path / "a.json").write_text("a")
    assert s.upload(tmp_path / "a.json", "results") == f"{s.prefix}/results/a.json"
    assert s3.get(BUCKET, f"{s.prefix}/results/a.json") == b"a"
    dest = s.download("results", "a.json", tmp_path / "copy.json")
    assert dest.read_text() == "a"


def test_names_lists_only_the_files_directly_in_the_folder():
    s3 = FakeS3()
    s = store(s3)
    for name in ("b.json", "a.json", "c.json"):
        s3.put(BUCKET, f"{s.prefix}/results/{name}", b"")
    s3.put(BUCKET, f"{s.prefix}/results/nested/x.json", b"")
    s3.put(BUCKET, f"{s.prefix}/db/metadata.json", b"")
    assert s.names("results") == ["a.json", "b.json", "c.json"]


def test_s3_errors_become_store_errors(tmp_path: Path):
    s3 = FakeS3()
    s = store(s3)
    with pytest.raises(StoreError, match="results/missing.json"):
        s.download("results", "missing.json", tmp_path / "x")
    s3.denied.add(f"{s.prefix}/results/a.json")
    (tmp_path / "a.json").write_text("{}")
    with pytest.raises(StoreError, match="AccessDenied"):
        s.upload(tmp_path / "a.json", "results")


def test_local_file_errors_become_store_errors(tmp_path: Path):
    s3 = FakeS3()
    s = store(s3)
    s3.put(BUCKET, f"{s.prefix}/results/a.json", b"{}")
    with pytest.raises(StoreError, match="results/a.json"):
        s.download("results", "a.json", tmp_path / "no-such-dir" / "a.json")
    with pytest.raises(StoreError, match="results/b.json"):
        s.upload(tmp_path / "b.json", "results")


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


def test_exists_tells_missing_from_unknown():
    s3 = FakeS3()
    s = store(s3)
    s3.put(BUCKET, f"{s.prefix}/db/metadata.json", b"{}")
    assert s.exists("db", "metadata.json")
    assert not s.exists("db", "other.json")
    # Only a 404 means missing; any other error must not read as "absent".
    s3.broken.add(f"{s.prefix}/db/metadata.json")
    with pytest.raises(StoreError, match="InternalError"):
        s.exists("db", "metadata.json")


def test_the_client_has_a_connection_for_every_download_thread():
    """build_report downloads with DOWNLOAD_THREADS threads on one client;
    a smaller pool logs "Connection pool is full" and reconnects."""
    with mock.patch.object(scan_store.boto3, "client") as client:
        ScanStore.for_build("postmerge", "master", "1", COMMIT)._s3()
    (call,) = client.call_args_list
    assert call.args == ("s3",)
    assert call.kwargs["region_name"] == "us-west-2"
    assert call.kwargs["config"].max_pool_connections == MAX_POOL_CONNECTIONS


def test_threads_share_one_client():
    def slow_client(*args, **kwargs):
        time.sleep(0.05)  # so that every thread asks before the first gets one
        return object()

    s = ScanStore.for_build("postmerge", "master", "1", COMMIT)
    with mock.patch.object(scan_store.boto3, "client", side_effect=slow_client):
        with ThreadPoolExecutor(max_workers=8) as pool:
            clients = list(pool.map(lambda _: s._s3(), range(8)))
        assert scan_store.boto3.client.call_count == 1
    assert all(c is clients[0] for c in clients)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
