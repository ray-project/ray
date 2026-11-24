import gzip
import json
import re
import shlex
import sys
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
import runfiles
import yaml
from botocore.exceptions import ClientError
from click.testing import CliRunner

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import build_report
from ci.ray_ci.automation.trivy.build_report import expected_images
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import BUCKET, MAX_POOL_CONNECTIONS
from ci.ray_ci.automation.trivy.trivy_report import (
    EXIT_CLEAN,
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
    "SchemaVersion": 2,
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


PUSH_TARGET = "//ci/ray_ci/automation:push_ray_image"
PUSH_FILES = ("build.rayci.yml", "linux_aarch64.rayci.yml")


def buildkite_dir() -> Path:
    """Where //.buildkite:pipelines puts every pipeline file."""
    return Path(runfiles.Create().Rlocation("io_ray/.buildkite/build.rayci.yml")).parent


def published_images(pipeline: dict) -> set:
    """(type, python, platform, arch) of every image the push steps publish,
    read with push_ray_image's own option parser."""
    images = set()
    for step in pipeline.get("steps") or []:
        if not isinstance(step, dict):
            continue
        commands = step.get("commands") or step.get("command") or []
        commands = [commands] if isinstance(commands, str) else commands
        pushes = [c for c in commands if PUSH_TARGET in c]
        if not pushes:
            continue
        array = step.get("array") or {}
        # Combinations this doesn't expand, such as rayci's `adjustments`,
        # would publish images that neither side of the comparison sees.
        assert set(array) <= {"python", "platform"}, (step.get("key"), sorted(array))
        assert "matrix" not in step and "parallelism" not in step, step.get("key")
        for command in pushes:
            for python in array.get("python") or [""]:
                for platform in array.get("platform") or [""]:
                    expanded = command.replace("{{array.python}}", python)
                    expanded = expanded.replace("{{array.platform}}", platform)
                    args = shlex.split(expanded.split(PUSH_TARGET, 1)[1])
                    params = push_ray_image.main.make_context(
                        "push_ray_image",
                        args[args.index("--") + 1 :],
                        resilient_parsing=True,
                    ).params
                    for p in params["platform"]:
                        images.add(
                            (
                                params["image_type"],
                                params["python_version"],
                                p,
                                params["architecture"],
                            )
                        )
    return images


def test_the_scan_steps_cover_exactly_what_the_push_steps_publish():
    """A Python version or platform added to a push step must be scanned too:
    the report's coverage check reads only the scan steps."""
    published = set()
    for name in PUSH_FILES:
        pipeline = yaml.safe_load((buildkite_dir() / name).read_text())
        published |= published_images(pipeline)
    scanned = {
        (i.image_type, i.python_version, i.platform, i.architecture)
        for i in expected_images(real_pipeline())
    }
    assert len(published) == 184
    assert scanned == published


def test_only_the_known_pipeline_files_publish_images():
    """A push step in any other file would go unchecked by the test above."""
    root = buildkite_dir()
    publishing = sorted(
        str(path.relative_to(root))
        for path in root.rglob("*.yml")
        if PUSH_TARGET in path.read_text()
    )
    assert publishing == sorted(PUSH_FILES)


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


def test_the_report_waits_for_every_scan_step():
    """A scan step the report doesn't wait for races it: its images would
    show up as not scanned."""
    steps = [s for s in real_pipeline()["steps"] if isinstance(s, dict)]
    (report,) = [s for s in steps if s.get("key") == "trivy_report"]
    scans = {
        s["key"] + ("(*)" if "array" in s else "")
        for s in steps
        if expected_images({"steps": [s]})
    }
    assert len(scans) >= 2
    assert {"trivy_db", *scans} <= set(report["depends_on"])


SCAN = "bazel run //ci/ray_ci/automation/trivy:scan_images --"


TPU_SCAN = f"{SCAN} --architecture x86_64 --platform tpu --image ray:3.12"


@pytest.mark.parametrize(
    "step",
    [
        pytest.param({"command": TPU_SCAN}, id="command-string"),
        pytest.param({"command": [TPU_SCAN]}, id="command-list"),
        pytest.param({"commands": TPU_SCAN}, id="commands-string"),
        pytest.param({"commands": ["echo start", TPU_SCAN]}, id="commands-list"),
    ],
)
def test_every_way_buildkite_takes_a_command_is_read(step: dict):
    """Buildkite takes `command` or `commands`, each a string or a list, and
    rayci passes them through; a form left unread would drop its images."""
    pipeline = {"steps": [{"key": "s", **step}]}
    assert [i.name for i in expected_images(pipeline)] == ["ray-py3.12-tpu"]


def test_a_step_that_is_not_a_mapping_scans_nothing():
    pipeline = {"steps": ["wait", {"key": "s", "command": TPU_SCAN}]}
    assert [i.name for i in expected_images(pipeline)] == ["ray-py3.12-tpu"]


def test_only_the_scan_command_is_parsed():
    """Another command's options must not leak into the scan's."""
    step = {
        "key": "s",
        "commands": ["bazel run //x:setup -- --architecture aarch64", TPU_SCAN],
    }
    assert [i.name for i in expected_images({"steps": [step]})] == ["ray-py3.12-tpu"]


@pytest.mark.parametrize(
    ("command", "error"),
    [
        pytest.param(
            f"{TPU_SCAN} && echo --image ray:3.11",
            "passes scan_images unexpected arguments ['&&', 'echo']",
            id="trailing-command",
        ),
        pytest.param(
            f"{TPU_SCAN} --bogus x --image ray:3.13",
            "No such option: --bogus",
            id="unknown-option",
        ),
        pytest.param(
            f"{SCAN} --architecture x86_64 --platform tpu --image",
            "Option '--image' requires an argument",
            id="option-without-value",
        ),
    ],
)
def test_a_scan_command_with_anything_unexpected_is_an_error(command: str, error: str):
    """Parsing leniently would drop or invent images without a word."""
    with pytest.raises(ValueError, match=re.escape(error)) as info:
        expected_images({"steps": [{"key": "images_scan_x", "commands": [command]}]})
    assert "images_scan_x" in str(info.value)


@pytest.mark.parametrize(
    ("step", "error"),
    [
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform cpu "
                    "--image ray:{{array.python}}"
                ],
                "array": {"python": ["3.12"]},
            },
            "array keys this can't expand: ['python']",
            id="other-array-dimension",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --architecture x86_64 --platform cpu"]},
            "no --image",
            id="no-images",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --platform cpu --image ray:3.12"]},
            "no --architecture",
            id="no-architecture",
        ),
        pytest.param(
            {
                "commands": [
                    "bazel run //ci/ray_ci/automation/trivy:scan_images "
                    "--architecture x86_64 --platform cpu --image ray:3.12"
                ]
            },
            "no `--`",
            id="no-separator",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --architecture x86_64 --platform tpu"] * 2},
            "mentions scan_images 2 times",
            id="two-scans",
        ),
        pytest.param(
            {
                "commands": [
                    "bazel build //ci/ray_ci/automation/trivy:scan_images",
                    TPU_SCAN,
                ]
            },
            "mentions scan_images 2 times; expected one run",
            id="build-then-run",
        ),
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform {{{{array.platform}}}}"
                    " --image ray:3.12"
                ],
                "array": {
                    "platform": ["cpu"],
                    "adjustments": [{"with": {"platform": "tpu"}}],
                },
            },
            "array keys this can't expand: ['adjustments']",
            id="adjustments",
        ),
        pytest.param(
            {"commands": [TPU_SCAN], "matrix": ["a", "b"]},
            "uses matrix",
            id="matrix",
        ),
        pytest.param(
            {
                "commands": [
                    "bazel run ci/ray_ci/automation/trivy:scan_images -- "
                    "--architecture x86_64 --platform tpu --image ray:3.12"
                ]
            },
            "names scan_images in a form this can't read",
            id="other-spelling",
        ),
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture riscv64 --platform tpu --image ray:3.12"
                ]
            },
            "'riscv64' is not one of",
            id="architecture-scan_images-rejects",
        ),
        pytest.param(
            {"commands": [f"{TPU_SCAN} --help"]},
            "passes --help",
            id="help",
        ),
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform {{{{array.platform}}}}"
                    " --image ray:3.12"
                ],
                "array": {"platform": [12.1]},
            },
            "array.platform that isn't text",
            id="platform-not-text",
        ),
        pytest.param(
            {"commands": [f"{TPU_SCAN} --image 'ray:3.11"]},
            "can't be split",
            id="unbalanced-quote",
        ),
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform tpu --image ray-llm:3.12"
                ]
            },
            "ray-llm",
            id="unpublished-variant",
        ),
    ],
)
def test_a_scan_step_it_cannot_read_is_an_error(step: dict, error: str):
    """Guessing would make the coverage check quietly wrong."""
    with pytest.raises(ValueError, match=re.escape(error)) as info:
        expected_images({"steps": [{"key": "images_scan_x", **step}]})
    assert "images_scan_x" in str(info.value)


@pytest.mark.parametrize(
    ("steps", "where"),
    [
        pytest.param(
            [{"key": "a", "command": TPU_SCAN}, {"key": "b", "command": TPU_SCAN}],
            "by steps a and b",
            id="two-steps",
        ),
        pytest.param(
            [{"key": "s", "command": f"{TPU_SCAN} --image ray:3.12"}],
            "by step s",
            id="one-step",
        ),
    ],
)
def test_an_image_scanned_twice_is_rejected(steps: list, where: str):
    with pytest.raises(ValueError, match=f"ray-py3.12-tpu is scanned twice, {where}"):
        expected_images({"steps": steps})


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
    with mock.patch.object(build_report.scan_store.boto3, "client", return_value=s3):
        return CliRunner().invoke(build_report.main, args, env=BUILD_ENV)


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
    footer = body.rstrip().splitlines()[-1]
    assert "The report could not be uploaded to S3 (`upload to s3://" in footer
    assert "AccessDenied" in footer
    assert "Kept for 30 days" not in footer
    assert "only in this job's `trivy/report/` artifacts" in footer
    # Every image was scanned: the failed upload isn't a coverage gap.
    assert "### Incomplete" not in body
    # The artifacts say the same as the annotation.
    report = (tmp_path / "out" / "report.md").read_text()
    assert report.rstrip().splitlines()[-1] == footer
    assert "### Incomplete" not in report
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["exit_code"] == EXIT_CLEAN
    assert summary["issues"] == []


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
    assert "The report could not be uploaded to S3" in annotation.read_text()
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["exit_code"] == EXIT_FINDINGS


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
    assert (
        "ValueError: ray-py3.11-tpu is scanned twice, by step images_scan_tpu" in body
    )


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


@pytest.mark.parametrize(
    "status, error",
    [
        pytest.param(None, "crane pull failed", id="scan-failed"),
        pytest.param(b"{", "malformed status record: ", id="malformed-status"),
    ],
)
def test_a_failed_download_keeps_the_status_records_error(
    tmp_path: Path, status: Optional[bytes], error: str
):
    """Only a missing result is explained by its failed download; any other
    error comes from the status record and is the real cause."""
    s3 = FakeS3()
    put_metadata(s3)
    pipeline = {"steps": [PIPELINE["steps"][2]]}
    put_scan(s3, "ray-py3.11-tpu", None)
    key = f"{PREFIX}/results/ray-py3.11-tpu"
    if status is not None:
        s3.put(BUCKET, f"{key}.status.json", status)
    # A result left by an earlier attempt, whose download fails.
    s3.put(BUCKET, f"{key}.json.gz", gzip.compress(json.dumps(CLEAN_RESULT).encode()))
    s3.broken.add(f"{key}.json.gz")
    result = run(tmp_path, s3, pipeline)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert f"`ray-py3.11-tpu`: {error}" in report
    assert "could not download" not in report


def test_a_failed_listing_is_named_on_every_image(tmp_path: Path):
    """Nothing was checked, so no image's line may say its job didn't run."""
    s3 = FakeS3()
    put_metadata(s3)
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    denied = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        "ListObjectsV2",
    )
    with mock.patch.object(s3, "paginate", side_effect=denied):
        result = run(tmp_path, s3)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert "could not list the scan results: listing of s3://" in report
    assert report.count(f": {build_report.UNLISTED}") == 5
    assert MISSING_STATUS not in report


def test_unreadable_metadata_is_named_by_its_s3_url(tmp_path: Path):
    s3 = FakeS3()
    s3.put(BUCKET, f"{PREFIX}/db/metadata.json", b"[" * 100_000)
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    result = run(tmp_path, s3, {"steps": [PIPELINE["steps"][2]]})
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert f"scanner metadata unreadable (s3://{BUCKET}/{PREFIX}/db/metadata.json)" in (
        report
    )


def test_a_result_name_ending_in_a_newline_is_ignored(tmp_path: Path, caplog):
    s3 = FakeS3()
    put_metadata(s3)
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    stray = f"{PREFIX}/results/ray-py3.11-tpu.status.json\n"
    s3.put(BUCKET, stray, b"{}")
    s3.broken.add(stray)
    result = run(tmp_path, s3, {"steps": [PIPELINE["steps"][2]]})
    assert result.exit_code == EXIT_CLEAN, result.output
    assert "Ignoring unexpected file" in caplog.text


def test_results_no_scan_step_asks_for_are_issues(tmp_path: Path):
    """The report can't vouch for them; each is named, and only once."""
    s3 = FakeS3()
    put_metadata(s3)
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    put_scan(s3, "ray-py3.10-cpu", CLEAN_RESULT)
    put_scan(s3, "ray-py3.10-gpu", CLEAN_RESULT)
    s3.broken.add(f"{PREFIX}/results/ray-py3.10-gpu.json.gz")
    result = run(tmp_path, s3, {"steps": [PIPELINE["steps"][2]]})
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    for name in ("ray-py3.10-cpu", "ray-py3.10-gpu"):
        assert (
            f"`{name}`: has a status record, but no scan step asks for this image"
        ) in report
    assert report.count("could not download its result") == 1


def test_an_old_annotation_file_cannot_replace_this_runs(tmp_path: Path):
    """post-command posts every file for the context, in name order."""
    annotations = tmp_path / "annotations"
    annotations.mkdir()
    (annotations / "trivy-scan.warning.md").write_text("an old run's")
    (annotations / "other.info.md").write_text("another context's")
    s3 = FakeS3()
    put_metadata(s3)
    put_scan(s3, "ray-py3.11-tpu", CLEAN_RESULT)
    result = run(tmp_path, s3, {"steps": [PIPELINE["steps"][2]]})
    assert result.exit_code == EXIT_CLEAN, result.output
    names = sorted(p.name for p in annotations.iterdir())
    assert names == ["other.info.md", "trivy-scan.success.md"]


def test_the_download_threads_fit_the_connection_pool():
    assert build_report.DOWNLOAD_THREADS <= MAX_POOL_CONNECTIONS


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
