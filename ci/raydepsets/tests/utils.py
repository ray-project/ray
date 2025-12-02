"""Shared test utilities for raydepsets tests."""

import shutil
from pathlib import Path
from typing import List, Optional

import runfiles

from ci.raydepsets.workspace import Depset

_REPO_NAME = "io_ray"
_runfiles = runfiles.Create()


def copy_data_to_tmpdir(tmpdir, ignore_patterns: Optional[str] = None):
    """Copy test data to a temporary directory."""
    shutil.copytree(
        _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/tests/test_data"),
        tmpdir,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(ignore_patterns) if ignore_patterns else None,
    )


def replace_in_file(filepath, old, new):
    with open(filepath, "r") as f:
        contents = f.read()

    contents = contents.replace(old, new)

    with open(filepath, "w") as f:
        f.write(contents)


def save_packages_to_file(filepath, packages):
    with open(filepath, "w") as f:
        for package in packages:
            f.write(package + "\n")


def save_file_as(input_file, output_file):
    with open(input_file, "rb") as f:
        contents = f.read()
    with open(output_file, "wb") as f:
        f.write(contents)


def append_to_file(filepath, new):
    with open(filepath, "a") as f:
        f.write(new + "\n")


def get_depset_by_name(depsets, name):
    for depset in depsets:
        if depset.name == name:
            return depset


def write_to_config_file(
    tmpdir: str,
    depsets: List[Depset],
    config_name: str,
    build_arg_sets: List[str] = None,
):
    with open(Path(tmpdir) / config_name, "w") as f:
        f.write(
            """
depsets:\n"""
        )

        for depset in depsets:
            f.write(
                f"""\n
    - name: {depset.name}
      operation: {depset.operation}
      {f"constraints: {depset.constraints}" if depset.constraints else ""}
      {f"requirements: {depset.requirements}" if depset.requirements else ""}
      output: {depset.output}
      {f"pre_hooks: {depset.pre_hooks}" if depset.pre_hooks else ""}
      {f"depsets: {depset.depsets}" if depset.depsets else ""}
      {f"source_depset: {depset.source_depset}" if depset.source_depset else ""}
      {f"append_flags: {depset.append_flags}" if depset.append_flags else ""}
      {f"override_flags: {depset.override_flags}" if depset.override_flags else ""}
      {f"packages: {depset.packages}" if depset.packages else ""}
      {f"build_arg_sets: {build_arg_sets}" if build_arg_sets else ""}
                """
            )
