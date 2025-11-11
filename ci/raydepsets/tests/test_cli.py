import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest
import runfiles
from click.testing import CliRunner
from networkx import topological_sort

from ci.raydepsets.cli import (
    DEFAULT_UV_FLAGS,
    DependencySetManager,
    _flatten_flags,
    _get_depset,
    _override_uv_flags,
    _uv_binary,
    build,
)
from ci.raydepsets.tests.utils import (
    append_to_file,
    copy_data_to_tmpdir,
    replace_in_file,
    save_file_as,
    save_packages_to_file,
    write_to_config_file,
)
from ci.raydepsets.workspace import (
    Depset,
)

_REPO_NAME = "io_ray"
_runfiles = runfiles.Create()


def _create_test_manager(
    tmpdir: str,
    config_path: Optional[str] = "test.depsets.yaml",
    check: bool = False,
    build_all_configs: Optional[bool] = False,
) -> DependencySetManager:
    uv_cache_dir = Path(tmpdir) / "uv_cache"
    return DependencySetManager(
        config_path=config_path,
        workspace_dir=tmpdir,
        uv_cache_dir=uv_cache_dir.as_posix(),
        check=check,
        build_all_configs=build_all_configs,
    )


def _invoke_build(tmpdir: str, config_path: str, name: Optional[str] = None):
    uv_cache_dir = Path(tmpdir) / "uv_cache"
    cmd = [
        config_path,
        "--workspace-dir",
        tmpdir,
        "--uv-cache-dir",
        uv_cache_dir.as_posix(),
    ]
    if name:
        cmd.extend(["--name", name])
    return CliRunner().invoke(
        build,
        cmd,
    )


class TestCli(unittest.TestCase):
    def test_cli_load_fail_no_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            result = _invoke_build(tmpdir, "fake_path/test.depsets.yaml")
            assert result.exit_code == 1
            assert isinstance(result.exception, FileNotFoundError)
            assert "No such file or directory" in str(result.exception)

    def test_dependency_set_manager_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert manager is not None
            assert manager.workspace.dir == tmpdir
            assert len(manager.config.depsets) > 0
            assert len(manager.build_graph.nodes) > 0

    def test_uv_binary_exists(self):
        assert _uv_binary() is not None

    def test_uv_version(self):
        result = subprocess.run(
            [_uv_binary(), "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0
        assert "uv 0.8.17" in result.stdout.decode("utf-8")
        assert result.stderr.decode("utf-8") == ""

    def test_compile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_file_as(
                Path(tmpdir) / "requirements_compiled_test.txt",
                Path(tmpdir) / "requirements_compiled.txt",
            )
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
            save_file_as(compiled_file, output_file)
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

    def test_compile_with_append_and_override_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--python-version 3.10"],
                override_flags=["--extra-index-url https://dummyurl.com"],
                name="ray_base_test_depset",
                output="requirements_compiled.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled.txt"
            output_text = output_file.read_text()
            assert "--python-version 3.10" in output_text
            assert "--extra-index-url https://dummyurl.com" in output_text
            assert (
                "--extra-index-url https://download.pytorch.org/whl/cu128"
                not in output_text
            )

    def test_compile_by_depset_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir, ignore_patterns="test2.depsets.yaml")
            result = _invoke_build(tmpdir, "test.depsets.yaml", "ray_base_test_depset")
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
                name="general_depset__py311_cpu",
                output="requirements_compiled_general.txt",
            )
            # Subset general_depset with requirements_test.txt (should lock emoji & pyperclip)
            manager.subset(
                source_depset="general_depset__py311_cpu",
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="subset_general_depset__py311_cpu",
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
                name="general_depset__py311_cpu",
                output="requirements_compiled_general.txt",
            )

            with self.assertRaises(RuntimeError) as e:
                manager.subset(
                    source_depset="general_depset__py311_cpu",
                    requirements=["requirements_compiled_test.txt"],
                    append_flags=["--no-annotate", "--no-header"],
                    name="subset_general_depset__py311_cpu",
                    output="requirements_compiled_subset_general.txt",
                )
            assert (
                "Requirement requirements_compiled_test.txt is not a subset of general_depset__py311_cpu in config test.depsets.yaml"
                in str(e.exception)
            )

    def test_check_if_subset_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            source_depset = Depset(
                name="general_depset__py311_cpu",
                operation="compile",
                requirements=["requirements_1.txt", "requirements_2.txt"],
                constraints=["requirement_constraints_1.txt"],
                output="requirements_compiled_general.txt",
                append_flags=[],
                override_flags=[],
                config_name="test.depsets.yaml",
            )
            with self.assertRaises(RuntimeError) as e:
                manager.check_subset_exists(
                    source_depset=source_depset,
                    requirements=["requirements_3.txt"],
                )
            assert (
                "Requirement requirements_3.txt is not a subset of general_depset__py311_cpu in config test.depsets.yaml"
                in str(e.exception)
            )

    def test_compile_bad_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            with self.assertRaises(RuntimeError) as e:
                manager.compile(
                    constraints=[],
                    requirements=["requirements_test_bad.txt"],
                    name="general_depset",
                    output="requirements_compiled_general.txt",
                )
            assert "File not found: `requirements_test_bad.txt" in str(e.exception)

    def test_get_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert (
                manager.get_path("requirements_test.txt")
                == Path(tmpdir) / "requirements_test.txt"
            )

    def test_append_uv_flags_exist_in_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=[],
                requirements=["requirements_test.txt"],
                name="general_depset",
                output="requirements_compiled_general.txt",
                append_flags=["--python-version=3.10"],
            )
            output_file = Path(tmpdir) / "requirements_compiled_general.txt"
            output_text = output_file.read_text()
            assert "--python-version=3.10" in output_text

    def test_append_uv_flags_with_space_in_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=[],
                requirements=["requirements_test.txt"],
                name="general_depset",
                output="requirements_compiled_general.txt",
                append_flags=["--python-version 3.10"],
            )
            output_file = Path(tmpdir) / "requirements_compiled_general.txt"
            output_text = output_file.read_text()
            assert "--python-version 3.10" in output_text

    def test_override_uv_flag_single_flag(self):
        expected_flags = DEFAULT_UV_FLAGS.copy()
        expected_flags.remove("--index-strategy")
        expected_flags.remove("unsafe-best-match")
        expected_flags.extend(["--index-strategy", "first-index"])
        assert (
            _override_uv_flags(
                ["--index-strategy first-index"],
                DEFAULT_UV_FLAGS.copy(),
            )
            == expected_flags
        )

    def test_override_uv_flag_multiple_flags(self):
        expected_flags = DEFAULT_UV_FLAGS.copy()
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
            assert len(manager.build_graph.nodes()) == 7
            assert len(manager.build_graph.edges()) == 4
            # assert that the compile depsets are first
            assert (
                manager.build_graph.nodes["general_depset__py311_cpu"]["operation"]
                == "compile"
            )
            assert (
                manager.build_graph.nodes["subset_general_depset"]["operation"]
                == "subset"
            )
            assert (
                manager.build_graph.nodes["expand_general_depset__py311_cpu"][
                    "operation"
                ]
                == "expand"
            )
            sorted_nodes = list(topological_sort(manager.build_graph))
            # assert that the root nodes are the compile depsets
            first_nodes = sorted_nodes[:4]
            assert all(
                manager.build_graph.nodes[node]["operation"] == "compile"
                or manager.build_graph.nodes[node]["operation"] == "pre_hook"
                for node in first_nodes
            )

    def test_build_graph_predecessors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            assert manager.build_graph is not None
            assert (
                manager.build_graph.nodes["general_depset__py311_cpu"]["operation"]
                == "compile"
            )
            assert (
                manager.build_graph.nodes["expanded_depset__py311_cpu"]["operation"]
                == "compile"
            )
            assert (
                manager.build_graph.nodes["expand_general_depset__py311_cpu"][
                    "operation"
                ]
                == "expand"
            )
            assert set(
                manager.build_graph.predecessors("expand_general_depset__py311_cpu")
            ) == {"general_depset__py311_cpu", "expanded_depset__py311_cpu"}

    def test_build_graph_bad_operation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir, ignore_patterns="test2.depsets.yaml")
            depset = Depset(
                name="invalid_op_depset",
                operation="invalid_op",
                requirements=["requirements_test.txt"],
                output="requirements_compiled_invalid_op.txt",
                config_name="test.depsets.yaml",
            )
            write_to_config_file(tmpdir, depset, "test.depsets.yaml")
            with self.assertRaises(ValueError) as e:
                _create_test_manager(tmpdir)
            assert (
                "Invalid operation: invalid_op for depset invalid_op_depset in config test.depsets.yaml"
                in str(e.exception)
            )

    def test_execute(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)

    def test_execute_single_depset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.execute(single_depset_name="general_depset__py311_cpu")
            assert (
                manager.build_graph.nodes["general_depset__py311_cpu"]["operation"]
                == "compile"
            )
            assert len(manager.build_graph.nodes()) == 1

    def test_execute_single_depset_that_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            with self.assertRaises(KeyError) as e:
                manager.execute(single_depset_name="fake_depset")
            assert "Dependency set fake_depset not found" in str(e.exception)

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
                name="general_depset__py311_cpu",
                output="requirements_compiled_general.txt",
            )
            manager.compile(
                constraints=[],
                requirements=["requirements_expanded.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="expanded_depset__py311_cpu",
                output="requirements_compiled_expanded.txt",
            )
            manager.expand(
                depsets=["general_depset__py311_cpu", "expanded_depset__py311_cpu"],
                constraints=["requirement_constraints_expand.txt"],
                append_flags=["--no-annotate", "--no-header"],
                requirements=[],
                name="expand_general_depset__py311_cpu",
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
                name="general_depset__py311_cpu",
                output="requirements_compiled_general.txt",
            )
            manager.expand(
                depsets=["general_depset__py311_cpu"],
                requirements=["requirements_expanded.txt"],
                constraints=["requirement_constraints_expand.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="expand_general_depset__py311_cpu",
                output="requirements_compiled_expand_general.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_expand_general.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test_expand.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_get_depset_with_build_arg_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.depsets.yaml",
                workspace_dir=tmpdir,
            )
            depset = _get_depset(
                manager.config.depsets, "build_args_test_depset__py311_cpu"
            )
            assert depset.name == "build_args_test_depset__py311_cpu"

    def test_get_depset_without_build_arg_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.depsets.yaml",
                workspace_dir=tmpdir,
            )
            depset = _get_depset(manager.config.depsets, "ray_base_test_depset")
            assert depset.name == "ray_base_test_depset"

    def test_execute_single_pre_hook(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            result = _invoke_build(tmpdir, "test2.depsets.yaml", "pre_hook_test_depset")
            assert (Path(tmpdir) / "test.depsets.yaml").exists()
            assert result.exit_code == 0
            assert "Pre-hook test" in result.output
            assert "Executed pre_hook pre-hook-test.sh successfully" in result.output

    def test_execute_single_invalid_pre_hook(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            result = _invoke_build(
                tmpdir, "test2.depsets.yaml", "pre_hook_invalid_test_depset"
            )
            assert result.exit_code == 1
            assert isinstance(result.exception, RuntimeError)
            assert (
                "Failed to execute pre_hook pre-hook-error-test.sh with error:"
                in str(result.exception)
            )

    def test_copy_lock_files_to_temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir, ignore_patterns="test2.depsets.yaml")
            depset = Depset(
                name="check_depset",
                operation="compile",
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                output="requirements_compiled_test.txt",
                config_name="test.depsets.yaml",
            )
            write_to_config_file(tmpdir, depset, "test.depsets.yaml")
            save_file_as(
                Path(tmpdir) / "requirements_compiled_test.txt",
                Path(tmpdir) / "requirements_compiled.txt",
            )
            manager = _create_test_manager(tmpdir, check=True)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="check_depset",
                output="requirements_compiled_test.txt",
            )
            assert (
                Path(manager.workspace.dir) / "requirements_compiled_test.txt"
            ).exists()
            assert (Path(manager.temp_dir) / "requirements_compiled_test.txt").exists()

    def test_diff_lock_files_out_of_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir, ignore_patterns="test2.depsets.yaml")
            depset = Depset(
                name="check_depset",
                operation="compile",
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                output="requirements_compiled_test.txt",
                config_name="test.depsets.yaml",
            )
            write_to_config_file(tmpdir, depset, "test.depsets.yaml")
            manager = _create_test_manager(tmpdir, check=True)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="check_depset",
                output="requirements_compiled_test.txt",
            )
            replace_in_file(
                Path(manager.workspace.dir) / "requirements_compiled_test.txt",
                "emoji==2.9.0",
                "emoji==2.8.0",
            )

            with self.assertRaises(RuntimeError) as e:
                manager.diff_lock_files()
            assert (
                "Lock files are not up to date for config: test.depsets.yaml. Please update lock files and push the changes."
                in str(e.exception)
            )
            assert "+emoji==2.8.0" in str(e.exception)
            assert "-emoji==2.9.0" in str(e.exception)

    def test_diff_lock_files_up_to_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir, ignore_patterns="test2.depsets.yaml")
            depset = Depset(
                name="check_depset",
                operation="compile",
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                output="requirements_compiled_test.txt",
                config_name="test.depsets.yaml",
            )
            write_to_config_file(tmpdir, depset, "test.depsets.yaml")
            manager = _create_test_manager(tmpdir, check=True)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="check_depset",
                output="requirements_compiled_test.txt",
            )
            manager.diff_lock_files()

    def test_compile_with_packages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_file_as(
                Path(tmpdir) / "requirements_compiled_test.txt",
                Path(tmpdir) / "requirements_compiled_test_packages.txt",
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                packages=["emoji==2.9.0", "pyperclip==1.6.0"],
                append_flags=["--no-annotate", "--no-header"],
                name="packages_test_depset",
                output="requirements_compiled_test_packages.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_test_packages.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_compile_with_packages_and_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_file_as(
                Path(tmpdir) / "requirements_compiled_test.txt",
                Path(tmpdir) / "requirements_compiled_test_packages.txt",
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                packages=["emoji==2.9.0", "pyperclip==1.6.0"],
                requirements=["requirements_test.txt"],
                append_flags=["--no-annotate", "--no-header"],
                name="packages_test_depset",
                output="requirements_compiled_test_packages.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_test_packages.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_requirements_ordering(self, mock_stdout):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_packages_to_file(
                Path(tmpdir) / "requirements_expanded.txt",
                ["six"],
            )
            save_packages_to_file(
                Path(tmpdir) / "requirements_compiled_test_expand.txt",
                ["zipp"],
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=[
                    "requirements_test.txt",
                    "requirements_expanded.txt",
                    "requirements_compiled_test_expand.txt",
                ],
                append_flags=["--no-annotate", "--no-header"],
                name="requirements_ordering_test_depset",
                output="requirements_compiled_requirements_ordering.txt",
            )
            stdout = mock_stdout.getvalue()
            assert (
                "requirements_compiled_test_expand.txt requirements_expanded.txt requirements_test.txt"
                in stdout
            )

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_constraints_ordering(self, mock_stdout):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            save_packages_to_file(
                Path(tmpdir) / "requirements_expanded.txt",
                ["six==1.17.0"],
            )
            save_packages_to_file(
                Path(tmpdir) / "requirements_compiled_test_expand.txt",
                ["zipp==3.19.2"],
            )
            manager = _create_test_manager(tmpdir)
            manager.compile(
                requirements=["requirements_test.txt"],
                constraints=[
                    "requirement_constraints_test.txt",
                    "requirements_expanded.txt",
                    "requirements_compiled_test_expand.txt",
                ],
                append_flags=["--no-annotate", "--no-header"],
                name="constraints_ordering_test_depset",
                output="requirements_compiled_constraints_ordering.txt",
            )
            stdout = mock_stdout.getvalue()
            assert (
                "-c requirement_constraints_test.txt -c requirements_compiled_test_expand.txt -c requirements_expanded.txt"
                in stdout
            )

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_execute_pre_hook(self, mock_stdout):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            manager.execute_pre_hook("pre-hook-test.sh test")
            stdout = mock_stdout.getvalue()
            assert "Pre-hook test\n" in stdout
            assert "Executed pre_hook pre-hook-test.sh test successfully" in stdout

    def test_get_expanded_depset_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(tmpdir)
            requirements = manager.get_expanded_depset_requirements(
                "general_depset__py311_cpu", []
            )
            assert requirements == ["requirements_test.txt"]
            requirements = manager.get_expanded_depset_requirements(
                "expand_general_depset__py311_cpu", []
            )
            assert sorted(requirements) == sorted(
                [
                    "requirements_test.txt",
                    "requirements_expanded.txt",
                ]
            )
            requirements = manager.get_expanded_depset_requirements(
                "nested_expand_depset__py311_cpu", []
            )
            assert sorted(requirements) == sorted(
                [
                    "requirements_compiled_test_expand.txt",
                    "requirements_expanded.txt",
                    "requirements_test.txt",
                ]
            )

    def test_build_all_configs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_data_to_tmpdir(tmpdir)
            manager = _create_test_manager(
                tmpdir, config_path="*.depsets.yaml", build_all_configs=True
            )
            assert manager.build_graph is not None
            assert len(manager.build_graph.nodes) == 12
            assert len(manager.build_graph.edges) == 8


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
