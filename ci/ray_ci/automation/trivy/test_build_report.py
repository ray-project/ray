import gzip
import json
import sys
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
import runfiles
import yaml
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import build_report
from ci.ray_ci.automation.trivy.build_report import expected_images
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import BUCKET
from ci.ray_ci.automation.trivy.trivy_report import (
    EXIT_FINDINGS,
    EXIT_INCOMPLETE,
    MISSING_STATUS,
)
from ci.ray_ci.ray_image import RayImage

FIXTURES = Path(__file__).parent / "testdata"
COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"
PREFIX = "trivy-scans/postmerge/master/20897-80ceee083e49"
BUILD_ENV = {
    "BUILDKITE_PIPELINE_SLUG": "postmerge",
    "BUILDKITE_BRANCH": "master",
    "BUILDKITE_BUILD_NUMBER": "20897",
    "BUILDKITE_COMMIT": COMMIT,
}
CLEAN_RESULT = {
    "Results": [{"Target": "debian", "Class": "os-pkgs", "Packages": [{}]}],
}

PIPELINE = {
    "group": "image scan",
    "steps": [
        {"key": "trivy_db", "commands": ["bazel run //ci/x:trivy_db -- --output m"]},
        {
            "key": "images_scan",
            "commands": [
                "bazel run //ci/ray_ci/automation/trivy:scan_images -- "
                "--architecture aarch64 --platform {{array.platform}} "
                "--image ray:3.12 --image ray-extra:3.12"
            ],
            "array": {"platform": ["cpu", "cu12.8.1-cudnn"]},
        },
        {
            "key": "images_scan_tpu",
            "commands": [
                "bazel run //ci/ray_ci/automation/trivy:scan_images --"
                " --architecture x86_64 --platform tpu --image ray:3.11"
            ],
        },
    ],
}


def real_pipeline() -> dict:
    path = runfiles.Create().Rlocation(build_report.PIPELINE_RUNFILE)
    return yaml.safe_load(Path(path).read_text())


def test_expected_images_reads_plain_and_array_steps():
    names = [image.name for image in expected_images(PIPELINE)]
    assert names == [
        "ray-py3.12-cpu-aarch64",
        "ray-extra-py3.12-cpu-aarch64",
        "ray-py3.12-cu12.8.1-cudnn-aarch64",
        "ray-extra-py3.12-cu12.8.1-cudnn-aarch64",
        "ray-py3.11-tpu",
    ]
    tpu = expected_images(PIPELINE)[-1]
    assert (tpu.image_type, tpu.python_version, tpu.platform, tpu.architecture) == (
        "ray",
        "3.11",
        "tpu",
        "x86_64",
    )


def test_the_pipeline_asks_for_every_published_image_once():
    images = expected_images(real_pipeline())
    names = [image.name for image in images]
    assert len(names) == 184
    assert len(set(names)) == len(names)
    for image in images:
        RayImage(
            image.image_type, image.python_version, image.platform, image.architecture
        ).validate()
    assert {
        "ray-py3.11-cpu",
        "ray-py3.14-tpu",
        "ray-llm-extra-py3.12-cu13.0.0-cudnn",
        "ray-extra-py3.14-cu13.3.1-cudnn-aarch64",
    } <= set(names)


def test_two_steps_scanning_the_same_image_are_rejected():
    pipeline = {"steps": [PIPELINE["steps"][2], PIPELINE["steps"][2]]}
    with pytest.raises(ValueError, match="ray-py3.11-tpu"):
        expected_images(pipeline)


def put_scan(s3: FakeS3, name: str, result: Optional[dict], **fields) -> None:
    image_type, rest = name.split("-py", 1)
    python_version, platform = rest.split("-", 1)
    architecture = "x86_64"
    if platform.endswith("-aarch64"):
        platform, architecture = platform[: -len("-aarch64")], "aarch64"
    record = {
        "name": name,
        "image_type": image_type,
        "python_version": python_version,
        "platform": platform,
        "architecture": architecture,
        "status": "scanned" if result else "error",
        "error": "" if result else "crane pull failed",
        **fields,
    }
    key = f"{PREFIX}/results/{name}"
    s3.put(BUCKET, f"{key}.status.json", json.dumps(record).encode())
    if result:
        s3.put(BUCKET, f"{key}.json.gz", gzip.compress(json.dumps(result).encode()))


def run(tmp_path: Path, s3: FakeS3, pipeline: dict = PIPELINE):
    pipeline_file = tmp_path / "image-scan.rayci.yml"
    pipeline_file.write_text(yaml.safe_dump(pipeline))
    args = [
        "--pipeline-file",
        str(pipeline_file),
        "--output-dir",
        str(tmp_path / "out"),
        "--annotation-dir",
        str(tmp_path / "annotations"),
    ]
    with mock.patch.object(
        build_report.scan_store.boto3, "client", return_value=s3
    ), mock.patch.object(build_report.trivy_report, "annotate") as annotate:
        result = CliRunner().invoke(build_report.main, args, env=BUILD_ENV)
    # Postmerge containers have no buildkite-agent; the hook posts the file.
    annotate.assert_not_called()
    return result


def put_metadata(s3: FakeS3) -> None:
    metadata = {"trivy": {"version": "0.76.1"}, "db": {"created": "2026-11-10"}}
    s3.put(BUCKET, f"{PREFIX}/db/metadata.json", json.dumps(metadata).encode())


def test_main_reports_the_build_from_s3(tmp_path: Path):
    s3 = FakeS3()
    put_metadata(s3)
    findings = json.loads((FIXTURES / "ray-py3.12-cpu.json").read_text())
    put_scan(s3, "ray-py3.12-cpu-aarch64", findings)
    put_scan(s3, "ray-extra-py3.12-cpu-aarch64", CLEAN_RESULT)
    put_scan(s3, "ray-py3.12-cu12.8.1-cudnn-aarch64", None)
    s3.put(BUCKET, f"{PREFIX}/results/notes.txt", b"ignored")

    result = run(tmp_path, s3)
    assert result.exit_code == EXIT_INCOMPLETE, result.output

    report = (tmp_path / "out" / "report.md").read_text()
    assert "Trivy 0.76.1" in report
    assert "`ray-py3.12-cu12.8.1-cudnn-aarch64`: crane pull failed" in report
    for missing in ("ray-extra-py3.12-cu12.8.1-cudnn-aarch64", "ray-py3.11-tpu"):
        assert f"`{missing}`: {MISSING_STATUS}" in report
    assert f"s3://{BUCKET}/{PREFIX}/report/" in report

    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["totals"]["images"] == 5
    assert summary["totals"]["scanned"] == 2
    assert summary["build"]["build_number"] == "20897"

    (annotation,) = (tmp_path / "annotations").iterdir()
    assert annotation.name == "trivy-scan.error.md"
    assert annotation.read_text().startswith("## Trivy image scan")

    uploaded = {k.rsplit("/", 1)[1] for _, k in s3.objects if "/report/" in k}
    assert uploaded == {"report.md", "summary.json", "vulns.csv.gz"}
    csv_text = gzip.decompress(s3.get(BUCKET, f"{PREFIX}/report/vulns.csv.gz"))
    assert csv_text == (tmp_path / "out" / "vulns.csv").read_bytes()


def test_missing_metadata_and_results_are_reported_not_fatal(tmp_path: Path):
    """trivy_db and every scan job failed: the report still says so."""
    result = run(tmp_path, FakeS3())
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert "scanner metadata unavailable: download of s3://" in report
    assert report.count(MISSING_STATUS) == 5
    (annotation,) = (tmp_path / "annotations").iterdir()
    assert annotation.name == "trivy-scan.warning.md"


def test_a_failed_report_upload_fails_a_clean_build(tmp_path: Path, caplog):
    s3 = FakeS3()
    put_metadata(s3)
    pipeline = {"steps": [PIPELINE["steps"][2]]}
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    s3.denied.add(f"{PREFIX}/report/summary.json")
    result = run(tmp_path, s3, pipeline)
    assert result.exit_code == 1, result.output
    assert "Could not upload the report" in caplog.text
    # The annotation still goes up; it says the upload failed, isn't green,
    # and doesn't send readers to a report/ folder that isn't there.
    (annotation,) = (tmp_path / "annotations").iterdir()
    assert annotation.name == "trivy-scan.warning.md"
    body = annotation.read_text()
    assert "the report could not be uploaded to S3: upload to s3://" in body
    assert "AccessDenied" in body
    footer = body.rstrip().splitlines()[-1]
    assert f"{PREFIX}/report/" not in footer
    assert "only in this job's `trivy/report/` artifacts" in footer


def test_a_failed_report_upload_keeps_the_findings_exit_code(tmp_path: Path):
    s3 = FakeS3()
    put_metadata(s3)
    pipeline = {"steps": [PIPELINE["steps"][2]]}
    findings = json.loads((FIXTURES / "ray-py3.12-cpu.json").read_text())
    put_scan(s3, "ray-py3.11-tpu", findings)
    s3.denied.add(f"{PREFIX}/report/report.md")
    result = run(tmp_path, s3, pipeline)
    assert result.exit_code == EXIT_FINDINGS, result.output
    (annotation,) = (tmp_path / "annotations").iterdir()
    assert annotation.name == "trivy-scan.error.md"
    assert "the report could not be uploaded to S3" in annotation.read_text()


def test_a_failed_report_still_leaves_a_warning_annotation(tmp_path: Path):
    """Without an annotation, a soft-failed report job goes unnoticed."""
    s3 = FakeS3()
    duplicated = {"steps": [PIPELINE["steps"][2], PIPELINE["steps"][2]]}
    result = run(tmp_path, s3, duplicated)
    assert result.exit_code == 1, result.output
    (annotation,) = (tmp_path / "annotations").iterdir()
    assert annotation.name == "trivy-scan.warning.md"
    body = annotation.read_text()
    assert "report failed" in body
    assert "ValueError: more than one scan step scans ['ray-py3.11-tpu']" in body


def test_the_failure_annotation_cannot_break_its_markdown():
    error = RuntimeError("bad `code` | pipe\n" + "x" * 5000)
    body = build_report.failure_annotation(error)
    detail = body.split("\n")[2]
    assert "`" not in detail.strip("`")
    assert len(body) < 2000


def test_a_failed_download_is_named_on_its_image(tmp_path: Path):
    """The image's own line must give the real cause, not 'did not run'."""
    s3 = FakeS3()
    put_metadata(s3)
    put_scan(s3, "ray-py3.12-cpu-aarch64", CLEAN_RESULT)
    put_scan(s3, "ray-extra-py3.12-cpu-aarch64", CLEAN_RESULT)
    s3.broken.add(f"{PREFIX}/results/ray-py3.12-cpu-aarch64.status.json")
    s3.broken.add(f"{PREFIX}/results/ray-extra-py3.12-cpu-aarch64.json.gz")
    result = run(tmp_path, s3)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert (
        "`ray-py3.12-cpu-aarch64`: could not download its status record: "
        "download of s3://"
    ) in report
    assert (
        "`ray-extra-py3.12-cpu-aarch64`: could not download its result: "
        "download of s3://"
    ) in report
    assert "InternalError" in report
    # Images with nothing in S3 keep the usual reason.
    assert f"`ray-py3.11-tpu`: {MISSING_STATUS}" in report


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
