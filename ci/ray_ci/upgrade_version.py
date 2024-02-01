import click
import os
import subprocess
from typing import Set

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

non_java_files = {
    "ci/ray_ci/utils.py",
    "python/ray/_version.py",
    "src/ray/common/constants.h",
}

DEFAULT_NON_JAVA_VERSION = "3.0.0.dev0"
DEFAULT_JAVA_VERSION = "2.0.0-SNAPSHOT"


def list_java_files(root_dir: str):
    """
    Scan the directories and return the set of pom.xml and pom_template.xml files.
    """
    files = set()
    for current_root_dir, _, file_names in os.walk(root_dir):
        for file_name in file_names:
            if file_name in ["pom.xml", "pom_template.xml"]:
                files.add(os.path.join(current_root_dir, file_name))
    return files


def get_check_output(file_path: str):
    return subprocess.check_output(["python", file_path], text=True)


def get_current_version(root_dir: str):
    """
    Scan for current Ray version and return the current versions.
    """
    version_file_path = os.path.join(root_dir, "python/ray/_version.py")
    ray_version_output = get_check_output(version_file_path).split(" ")
    if len(ray_version_output) != 2:
        raise ValueError(
            f"Unexpected output from {version_file_path}: {ray_version_output}"
        )
    version = ray_version_output[0]

    if version != DEFAULT_NON_JAVA_VERSION:
        non_java_version = version
        java_version = version
        return (non_java_version, java_version)
    return (DEFAULT_NON_JAVA_VERSION, DEFAULT_JAVA_VERSION)


def upgrade_file_version(
    non_java_files: Set[str],
    java_files: Set[str],
    non_java_version: str,
    java_version: str,
    new_version: str,
    root_dir: str,
):
    """
    Modify the version in the files to the specified version.
    """
    for file_path in non_java_files | java_files:
        with open(os.path.join(root_dir, file_path), "r") as f:
            content = f.read()
        current_version = (
            non_java_version if file_path in non_java_files else java_version
        )
        content = content.replace(current_version, new_version)

        with open(os.path.join(root_dir, file_path), "w") as f:
            f.write(content)


@click.command()
@click.option("--new_version", required=True, type=str)
def main(new_version: str):
    """
    Update the version in the files to the specified version.
    """
    non_java_version, java_version = get_current_version(bazel_workspace_dir)

    java_files = list_java_files(bazel_workspace_dir)
    upgrade_file_version(
        non_java_files,
        java_files,
        non_java_version,
        java_version,
        new_version,
        bazel_workspace_dir,
    )


if __name__ == "__main__":
    main()
