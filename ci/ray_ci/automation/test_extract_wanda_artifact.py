"""Tests for extract_wanda_artifact."""

import tempfile
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

from ci.ray_ci.automation.crane_lib import CraneError
from ci.ray_ci.automation.extract_wanda_artifact import main

# crane requires a real container registry, so we mock it and ecr_docker_login.
# The mock simulates crane export by writing files to the output directory.
_MODULE = "ci.ray_ci.automation.extract_wanda_artifact"


def _invoke(args, crane_side_effect):
    with mock.patch(
        f"{_MODULE}.call_crane_export", side_effect=crane_side_effect
    ), mock.patch(f"{_MODULE}.ecr_docker_login"):
        return CliRunner().invoke(main, args)


def _base_args(**overrides):
    defaults = {
        "--wanda-image-name": "test-image",
        "--file-glob": "*.tgz",
        "--rayci-work-repo": "ecr.example.com/repo",
        "--rayci-build-id": "build-123",
    }
    defaults.update(overrides)
    args = []
    for k, v in defaults.items():
        args.extend([k, v])
    return args


class TestExtraction:
    def test_single_tgz(self):
        def export(tag, d):
            (Path(d) / "ray.tgz").write_bytes(b"content")
            (Path(d) / "other.txt").write_bytes(b"noise")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(_base_args(**{"--output-dir": outdir}), export)
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "ray.tgz").read_bytes() == b"content"
            assert not (Path(outdir) / "other.txt").exists()

    def test_multiple_matches(self):
        def export(tag, d):
            (Path(d) / "a.tgz").write_bytes(b"aaa")
            (Path(d) / "b.tgz").write_bytes(b"bbb")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(
                _base_args(**{"--output-dir": outdir}),
                export,
            )
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "a.tgz").exists()
            assert (Path(outdir) / "b.tgz").exists()

    def test_nested_files_found_via_rglob(self):
        def export(tag, d):
            nested = Path(d) / "opt" / "artifacts"
            nested.mkdir(parents=True)
            (nested / "ray.tgz").write_bytes(b"nested")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(
                _base_args(**{"--output-dir": outdir}),
                export,
            )
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "ray.tgz").read_bytes() == b"nested"

    def test_custom_glob_whl(self):
        def export(tag, d):
            (Path(d) / "ray.whl").write_bytes(b"wheel")
            (Path(d) / "ray.tgz").write_bytes(b"tarball")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(
                _base_args(**{"--file-glob": "*.whl", "--output-dir": outdir}),
                export,
            )
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "ray.whl").exists()
            assert not (Path(outdir) / "ray.tgz").exists()

    def test_missing_file_glob_errors(self):
        args = [
            "--wanda-image-name",
            "test-image",
            "--rayci-work-repo",
            "ecr.example.com/repo",
            "--rayci-build-id",
            "build-123",
        ]
        result = _invoke(args, lambda tag, d: None)
        assert result.exit_code != 0
        assert "--file-glob" in result.output

    def test_creates_output_dir(self):
        def export(tag, d):
            (Path(d) / "ray.tgz").write_bytes(b"content")

        with tempfile.TemporaryDirectory() as tmpdir:
            outdir = Path(tmpdir) / "new" / "nested"
            result = _invoke(
                _base_args(**{"--output-dir": str(outdir)}),
                export,
            )
            assert result.exit_code == 0, result.output
            assert (outdir / "ray.tgz").exists()


class TestErrorHandling:
    def test_no_matching_files(self):
        def export(tag, d):
            (Path(d) / "something.txt").write_bytes(b"")

        result = _invoke(_base_args(), export)
        assert result.exit_code != 0
        assert "No files matching" in result.output

    def test_empty_image(self):
        result = _invoke(_base_args(), lambda tag, d: None)
        assert result.exit_code != 0

    def test_duplicate_basenames_raises(self):
        def export(tag, d):
            (Path(d) / "ray.tgz").write_bytes(b"top")
            nested = Path(d) / "sub"
            nested.mkdir()
            (nested / "ray.tgz").write_bytes(b"nested")

        result = _invoke(_base_args(), export)
        assert result.exit_code != 0
        assert "Duplicate basename" in result.output

    def test_crane_error_becomes_click_exception(self):
        def export(tag, d):
            raise CraneError("crane export failed (rc=1)")

        result = _invoke(_base_args(), export)
        assert result.exit_code != 0
        assert "crane export failed" in result.output
        # ClickException prints "Error: <message>" without a traceback.
        assert "Traceback" not in result.output

    def test_directories_matching_glob_are_skipped(self):
        def export(tag, d):
            # A directory whose name matches the glob should not be copied;
            # only the regular file inside it should be extracted.
            (Path(d) / "weirdly-named.tgz").mkdir()
            (Path(d) / "real.tgz").write_bytes(b"content")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(_base_args(**{"--output-dir": outdir}), export)
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "real.tgz").is_file()
            assert not (Path(outdir) / "weirdly-named.tgz").exists()

    def test_duplicate_detected_before_any_copy(self):
        # Interleaving validation with copying would leave the first file
        # behind on disk before the duplicate is reported. Verify nothing
        # is copied when a duplicate exists anywhere in the match set.
        def export(tag, d):
            (Path(d) / "a.tgz").write_bytes(b"first")
            nested = Path(d) / "sub"
            nested.mkdir()
            (nested / "a.tgz").write_bytes(b"second")

        with tempfile.TemporaryDirectory() as outdir:
            result = _invoke(_base_args(**{"--output-dir": outdir}), export)
            assert result.exit_code != 0
            assert "Duplicate basename" in result.output
            assert list(Path(outdir).iterdir()) == []


class TestWorkspaceResolution:
    def test_relative_output_resolved_against_workspace(self):
        # Under `bazel run`, cwd is the runfiles tree and BUILD_WORKSPACE_DIRECTORY
        # points at the workspace root. Relative --output-dir must resolve there.
        def export(tag, d):
            (Path(d) / "ray.tgz").write_bytes(b"content")

        with tempfile.TemporaryDirectory() as workspace, mock.patch.dict(
            "os.environ", {"BUILD_WORKSPACE_DIRECTORY": workspace}
        ):
            result = _invoke(_base_args(**{"--output-dir": ".whl"}), export)
            assert result.exit_code == 0, result.output
            assert (Path(workspace) / ".whl" / "ray.tgz").read_bytes() == b"content"

    def test_absolute_output_not_rewritten(self):
        def export(tag, d):
            (Path(d) / "ray.tgz").write_bytes(b"content")

        with tempfile.TemporaryDirectory() as workspace, tempfile.TemporaryDirectory() as outdir, mock.patch.dict(
            "os.environ", {"BUILD_WORKSPACE_DIRECTORY": workspace}
        ):
            result = _invoke(_base_args(**{"--output-dir": outdir}), export)
            assert result.exit_code == 0, result.output
            assert (Path(outdir) / "ray.tgz").exists()
            assert not (Path(workspace) / outdir.lstrip("/")).exists()


class TestImageTag:
    def test_constructs_repo_buildid_name(self):
        captured = []

        def export(tag, d):
            captured.append(tag)
            (Path(d) / "f.tgz").write_bytes(b"")

        _invoke(
            _base_args(
                **{
                    "--wanda-image-name": "my-image",
                    "--rayci-work-repo": "ecr.aws.com/rayci",
                    "--rayci-build-id": "abc-123",
                }
            ),
            export,
        )
        assert captured == ["ecr.aws.com/rayci:abc-123-my-image"]


class TestECRLogin:
    def test_extracts_registry_host(self):
        registries = []

        def export(tag, d):
            (Path(d) / "f.tgz").write_bytes(b"")

        with mock.patch(f"{_MODULE}.call_crane_export", side_effect=export), mock.patch(
            f"{_MODULE}.ecr_docker_login", side_effect=lambda r: registries.append(r)
        ):
            CliRunner().invoke(
                main,
                _base_args(
                    **{
                        "--rayci-work-repo": "830883877497.dkr.ecr.us-west-2.amazonaws.com/rayci"
                    }
                ),
            )
        assert registries == ["830883877497.dkr.ecr.us-west-2.amazonaws.com"]
