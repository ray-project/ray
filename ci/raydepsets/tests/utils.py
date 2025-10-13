"""Shared test utilities for raydepsets tests."""

import shutil

import runfiles

_REPO_NAME = "io_ray"
_runfiles = runfiles.Create()


def copy_data_to_tmpdir(tmpdir):
    """Copy test data to a temporary directory."""
    shutil.copytree(
        _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/tests/test_data"),
        tmpdir,
        dirs_exist_ok=True,
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
