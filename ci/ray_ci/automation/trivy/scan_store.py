"""Where one build's Trivy scan files live in S3.

The DB snapshot, each image's results and the report of a build share one
folder, named so people can find it by eye:

    s3://ray-ci-artifact-branch-public/trivy-scans/
        <pipeline>/<branch>/<build number>-<commit[:12]>/
            db/       trivy-db.tar.gz, trivy-java-db.tar.gz, metadata.json
            results/  <image>.status.json, <image>.json.gz
            report/   report.md, summary.json, vulns.csv.gz

Every job of a build derives the same folder from Buildkite's variables, so
jobs hand files to each other through it. CI runner roles can read and write
this bucket but can't push to ECR. The bucket expires objects after 30 days.
"""

import functools
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import boto3
import click
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

BUCKET = "ray-ci-artifact-branch-public"
ROOT = "trivy-scans"
REGION = "us-west-2"
DB_FOLDER = "db"
RESULTS_FOLDER = "results"
REPORT_FOLDER = "report"
# Room for build_report's download threads, which share one client.
MAX_POOL_CONNECTIONS = 32


class StoreError(Exception):
    """The folder can't be named, or an S3 call failed."""


def _clean_path(value: str) -> str:
    """Keep the folder readable and inside its parent, whatever the name holds."""
    parts = [re.sub(r"[^A-Za-z0-9._-]+", "_", p) for p in value.split("/")]
    return "/".join(p for p in parts if p not in ("", ".", ".."))


def _clean_commit(commit: str) -> str:
    """Normally a SHA, but a build created with commit HEAD starts with HEAD."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", commit).strip("_.")


@dataclass
class ScanStore:
    pipeline: str
    branch: str
    build_number: str
    commit: str
    bucket: str = BUCKET
    # A boto3 S3 client, created on first use.
    client: Any = field(default=None, repr=False)

    @classmethod
    def for_build(
        cls,
        pipeline: str,
        branch: str,
        build_number: str,
        commit: str,
        bucket: str = BUCKET,
        client: Any = None,
    ) -> "ScanStore":
        if not re.fullmatch(r"[0-9]+", build_number):
            raise StoreError(f"build number {build_number!r} is not a number")
        if not _clean_commit(commit):
            raise StoreError(f"commit {commit!r} has no usable name")
        if not _clean_path(pipeline.replace("/", "_")):
            raise StoreError(f"pipeline {pipeline!r} has no usable name")
        if not _clean_path(branch):
            raise StoreError(f"branch {branch!r} has no usable name")
        return cls(pipeline, branch, build_number, commit, bucket, client)

    @property
    def prefix(self) -> str:
        pipeline = _clean_path(self.pipeline.replace("/", "_"))
        build = f"{self.build_number}-{_clean_commit(self.commit)[:12]}"
        return f"{ROOT}/{pipeline}/{_clean_path(self.branch)}/{build}"

    def key(self, folder: str, name: str) -> str:
        return f"{self.prefix}/{folder}/{name}"

    def url(self, folder: str = "") -> str:
        return f"s3://{self.bucket}/{self.prefix}/" + (f"{folder}/" if folder else "")

    def _s3(self) -> Any:
        if self.client is None:
            self.client = boto3.client(
                "s3",
                region_name=REGION,
                config=Config(max_pool_connections=MAX_POOL_CONNECTIONS),
            )
        return self.client

    def _call(self, what: str, key: str, call: Callable[[], Any]) -> Any:
        try:
            return call()
        except (Boto3Error, BotoCoreError, ClientError) as e:
            raise StoreError(f"{what} s3://{self.bucket}/{key} failed: {e}") from e

    def upload(self, path: Path, folder: str) -> str:
        key = self.key(folder, path.name)
        self._call(
            "upload to",
            key,
            lambda: self._s3().upload_file(str(path), self.bucket, key),
        )
        return key

    def download(self, folder: str, name: str, dest: Path) -> Path:
        key = self.key(folder, name)
        self._call(
            "download of",
            key,
            lambda: self._s3().download_file(self.bucket, key, str(dest)),
        )
        return dest

    def exists(self, folder: str, name: str) -> bool:
        """Whether the file is there. Only a 404 means it isn't: any other
        error raises, rather than pass for "absent"."""
        key = self.key(folder, name)
        try:
            self._s3().head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return False
            raise StoreError(f"check of s3://{self.bucket}/{key} failed: {e}") from e
        except (Boto3Error, BotoCoreError) as e:
            raise StoreError(f"check of s3://{self.bucket}/{key} failed: {e}") from e
        return True

    def names(self, folder: str) -> list[str]:
        """The files directly in ``folder``."""
        prefix = f"{self.prefix}/{folder}/"

        def list_keys() -> list[str]:
            pages = self._s3().get_paginator("list_objects_v2")
            return [
                obj["Key"]
                for page in pages.paginate(Bucket=self.bucket, Prefix=prefix)
                for obj in page.get("Contents") or []
            ]

        keys = self._call("listing of", prefix, list_keys)
        return sorted(k[len(prefix) :] for k in keys if "/" not in k[len(prefix) :])


def build_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Click options naming the build's folder; the command gets ``store``."""

    @click.option("--pipeline", required=True, envvar="BUILDKITE_PIPELINE_SLUG")
    @click.option("--branch", required=True, envvar="BUILDKITE_BRANCH")
    @click.option("--build-number", required=True, envvar="BUILDKITE_BUILD_NUMBER")
    @click.option("--commit", required=True, envvar="BUILDKITE_COMMIT")
    @click.option("--bucket", default=BUCKET, show_default=True)
    @functools.wraps(f)
    def wrapper(
        *args: Any,
        pipeline: str,
        branch: str,
        build_number: str,
        commit: str,
        bucket: str,
        **kwargs: Any,
    ) -> Any:
        try:
            store = ScanStore.for_build(pipeline, branch, build_number, commit, bucket)
        except StoreError as e:
            raise click.UsageError(str(e)) from e
        return f(*args, store=store, **kwargs)

    return wrapper
