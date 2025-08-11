import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Optional

import pytest
import runfiles
from click.testing import CliRunner
from networkx import topological_sort

from ci.raydepsets.cli import (
    DEFAULT_UV_FLAGS,
    DependencySetManager,
    Depset,
    _append_uv_flags,
    _flatten_flags,
    _override_uv_flags,
    _uv_binary,
    load,
)
from ci.raydepsets.tests.utils import (
    append_to_file,
    copy_data_to_tmpdir,
    replace_in_file,
    save_file_as,
    save_packages_to_file,
)

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


def _create_test_manager(
    tmpdir: str, config_path: Optional[str] = None
) -> DependencySetManager:
    if config_path is None:
        config_path = "test.depsets.yaml"
    uv_cache_dir = Path(tmpdir) / "uv_cache"
    return DependencySetManager(
        config_path=config_path,
        workspace_dir=tmpdir,
        uv_cache_dir=uv_cache_dir.as_posix(),
    )


class TestCli(unittest.TestCase):
    def test_cli_load_fail_no_config(self):
        result = CliRunner().invoke(
            load,
            [
                "fake_path/test.depsets.yaml",
                "--workspace-dir",
                "/ci/raydepsets/test_data",
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)
        assert "No such file or directory" in str(result.exception)

    def test_dependency_set_manager_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert manager is not None
            assert manager.workspace.dir == tmpdir
            assert manager.config.depsets[0].name == "ray_base_test_depset"
            assert manager.config.depsets[0].operation == "compile"
            assert manager.config.depsets[0].requirements == ["requirements_test.txt"]
            assert manager.config.depsets[0].constraints == [
                "requirement_constraints_test.txt"
            ]
            assert manager.config.depsets[0].output == "requirements_compiled.txt"

    def test_dependency_set_manager_get_depset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            with self.assertRaises(KeyError):
                manager.get_depset("fake_depset")

    def test_uv_binary_exists(self):
        assert _uv_binary() is not None

    def test_uv_version(self):
        result = subprocess.run(
            [_uv_binary(), "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0
        assert "uv 0.7.20" in result.stdout.decode("utf-8")
        assert result.stderr.decode("utf-8") == ""

    def test_compile(self):
        compiled_file = Path(
            _runfiles.Rlocation(
                f"{_REPO_NAME}/ci/raydepsets/tests/test_data/requirements_compiled_test.txt"
            )
        )
        output_file = Path(
            _runfiles.Rlocation(
                f"{_REPO_NAME}/ci/raydepsets/tests/test_data/requirements_compiled.txt"
            )
        )
        shutil.copy(compiled_file, output_file)

        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="ray_base_test_depset",
                output="requirements_compiled.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_compile_update_package(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            compiled_file = Path(
                _runfiles.Rlocation(f"{tmpdir}/requirement_constraints_test.txt")
            )
            replace_in_file(compiled_file, "emoji==2.9.0", "emoji==2.10.0")
            output_file = Path(
                _runfiles.Rlocation(f"{tmpdir}/requirements_compiled.txt")
            )
            shutil.copy(compiled_file, output_file)
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="ray_base_test_depset",
                output="requirements_compiled.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test_update.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_compile_by_depset_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            uv_cache_dir = Path(tmpdir) / "uv_cache"

            result = CliRunner().invoke(
                load,
                [
                    "test.depsets.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "ray_base_test_depset",
                    "--uv-cache-dir",
                    uv_cache_dir.as_posix(),
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled.txt"
            assert output_fp.is_file()
            assert result.exit_code == 0

            assert (
                "Dependency set ray_base_test_depset compiled successfully"
                in result.output
            )

    def test_subset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            # Add six to requirements_test_subset.txt
            save_packages_to_file(
                Path(tmpdir) / "requirements_test_subset.txt",
                ["six==1.16.0"],
            )
            manager = _create_test_manager(tmpdir)
            # Compile general_depset with requirements_test.txt and requirements_test_subset.txt
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt", "requirements_test_subset.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="general_depset",
                output="requirements_compiled_general.txt",
            )
            # Subset general_depset with requirements_test.txt (should lock emoji & pyperclip)
            manager.subset(
                source_depset="general_depset",
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="subset_general_depset",
                output="requirements_compiled_subset_general.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_subset_general.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()

            assert output_text == output_text_valid

    def test_subset_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            # Add six to requirements_test_subset.txt
            save_packages_to_file(
                Path(tmpdir) / "requirements_test_subset.txt",
                ["six==1.16.0"],
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt", "requirements_test_subset.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="general_depset",
                output="requirements_compiled_general.txt",
            )

            with self.assertRaises(RuntimeError):
                manager.subset(
                    source_depset="general_depset",
                    requirements=["requirements_compiled_test.txt"],
                    append_flags=["--no-annotate", "--no-header"],
                    name="subset_general_depset",
                    output="requirements_compiled_subset_general.txt",
                )

    def test_check_if_subset_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            source_depset = Depset(
                name="general_depset",
                operation="compile",
                requirements=["requirements_1.txt", "requirements_2.txt"],
                constraints=["requirement_constraints_1.txt"],
                output="requirements_compiled_general.txt",
                append_flags=[],
                override_flags=[],
            )
            with self.assertRaises(RuntimeError):
                manager.check_subset_exists(
                    source_depset=source_depset,
                    requirements=["requirements_3.txt"],
                )

    def test_compile_bad_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            with self.assertRaises(RuntimeError):
                manager.compile(
                    constraints=[],
                    requirements=["requirements_test_bad.txt"],
                    name="general_depset",
                    output="requirements_compiled_general.txt",
                )

    def test_get_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert (
                manager.get_path("requirements_test.txt")
                == f"{tmpdir}/requirements_test.txt"
            )

    def test_append_uv_flags(self):
        assert _append_uv_flags(
            ["--no-annotate", "--no-header"], DEFAULT_UV_FLAGS.copy()
        ) == DEFAULT_UV_FLAGS.copy() + ["--no-annotate", "--no-header"]

    def test_override_uv_flag_single_flag(self):
        expected_flags = DEFAULT_UV_FLAGS.copy()
        expected_flags.remove("--extra-index-url")
        expected_flags.remove("https://download.pytorch.org/whl/cpu")
        expected_flags.extend(
            ["--extra-index-url", "https://download.pytorch.org/whl/cu128"]
        )
        assert (
            _override_uv_flags(
                ["--extra-index-url https://download.pytorch.org/whl/cu128"],
                DEFAULT_UV_FLAGS.copy(),
            )
            == expected_flags
        )

    def test_override_uv_flag_multiple_flags(self):
        expected_flags = DEFAULT_UV_FLAGS.copy()
        expected_flags.remove("--unsafe-package")
        expected_flags.remove("ray")
        expected_flags.remove("--unsafe-package")
        expected_flags.remove("grpcio-tools")
        expected_flags.remove("--unsafe-package")
        expected_flags.remove("setuptools")
        expected_flags.extend(["--unsafe-package", "dummy"])
        assert (
            _override_uv_flags(
                ["--unsafe-package dummy"],
                DEFAULT_UV_FLAGS.copy(),
            )
            == expected_flags
        )

    def test_flatten_flags(self):
        assert _flatten_flags(["--no-annotate", "--no-header"]) == [
            "--no-annotate",
            "--no-header",
        ]
        assert _flatten_flags(
            [
                "--no-annotate",
                "--no-header",
                "--extra-index-url https://download.pytorch.org/whl/cu128",
            ]
        ) == [
            "--no-annotate",
            "--no-header",
            "--extra-index-url",
            "https://download.pytorch.org/whl/cu128",
        ]

    def test_build_graph(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert manager.build_graph is not None
            assert len(manager.build_graph.nodes()) == 5
            assert len(manager.build_graph.edges()) == 3
            assert manager.build_graph.nodes["general_depset"]["operation"] == "compile"
            assert (
                manager.build_graph.nodes["subset_general_depset"]["operation"]
                == "subset"
            )
            assert (
                manager.build_graph.nodes["expand_general_depset"]["operation"]
                == "expand"
            )

            sorted_nodes = list(topological_sort(manager.build_graph))
            assert sorted_nodes[0] == "ray_base_test_depset"
            assert sorted_nodes[1] == "general_depset"
            assert sorted_nodes[2] == "expanded_depset"

    def test_build_graph_bad_operation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            with open(Path(tmpdir) / "test.depsets.yaml", "w") as f:
                f.write(
                    """
depsets:
    - name: invalid_op_depset
      operation: invalid_op
      requirements:
          - requirements_test.txt
      output: requirements_compiled_invalid_op.txt
                """
                )
            with self.assertRaises(ValueError):
                _create_test_manager(tmpdir)

    def test_execute(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)

    def test_expand(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_packages_to_file(
                Path(tmpdir) / "requirements_expanded.txt",
                ["six"],
            )
            save_file_as(
                Path(tmpdir) / "requirement_constraints_test.txt",
                Path(tmpdir) / "requirement_constraints_expand.txt",
            )
            append_to_file(
                Path(tmpdir) / "requirement_constraints_expand.txt",
                "six==1.17.0",
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="general_depset",
                output="requirements_compiled_general.txt",
            )
            manager.compile(
                constraints=[],
                requirements=["requirements_expanded.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="expanded_depset",
                output="requirements_compiled_expanded.txt",
            )
            manager.expand(
                depsets=["general_depset", "expanded_depset"],
                constraints=["requirement_constraints_expand.txt"],
                append_flags=["--no-annotate", "--no-header"],
                requirements=[],
                name="expand_general_depset",
                output="requirements_compiled_expand_general.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_expand_general.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test_expand.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_expand_with_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_packages_to_file(
                Path(tmpdir) / "requirements_expanded.txt",
                ["six"],
            )
            save_file_as(
                Path(tmpdir) / "requirement_constraints_test.txt",
                Path(tmpdir) / "requirement_constraints_expand.txt",
            )
            append_to_file(
                Path(tmpdir) / "requirement_constraints_expand.txt",
                "six==1.17.0",
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="general_depset",
                output="requirements_compiled_general.txt",
            )
            manager.expand(
                depsets=["general_depset"],
                requirements=["requirements_expanded.txt"],
                constraints=["requirement_constraints_expand.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="expand_general_depset",
                output="requirements_compiled_expand_general.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_expand_general.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test_expand.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
