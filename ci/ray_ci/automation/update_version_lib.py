import os
import subprocess

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

MASTER_BRANCH_VERSION = "3.0.0.dev0"


def get_check_output(file_path: str):
    return subprocess.check_output(["python", file_path], text=True)


def get_current_version(root_dir: str):
    """
    Scan for current Ray version and return the current version.
    """
    version_file_path = os.path.join(root_dir, "python/ray/_version.py")
    ray_version_output = get_check_output(version_file_path).split()
    if len(ray_version_output) != 2:
        raise ValueError(
            f"Unexpected output from {version_file_path}: {ray_version_output}"
        )
    version = ray_version_output[0]

    if version != MASTER_BRANCH_VERSION:
        return version
    return MASTER_BRANCH_VERSION


def update_file_version(
    main_version: str,
    new_version: str,
    root_dir: str,
):
    """
    Modify the version in the files to the specified version.
    """
    files = [
        "ci/ray_ci/utils.py",
        "python/ray/_version.py",
        "rayci.env",
        "src/ray/common/constants.h",
    ]
    files.sort()

    def replace_version_in_file(file_path: str, old_version: str):
        """
        Helper function to replace old version in file with new version.
        """
        abs_file_path = os.path.join(root_dir, file_path)
        if not os.path.exists(abs_file_path):
            raise ValueError(f"File {abs_file_path} does not exist.")
        with open(abs_file_path, "r") as f:
            content = f.read()
        content = content.replace(old_version, new_version)
        with open(abs_file_path, "w") as f:
            f.write(content)

    for file_path in files:
        replace_version_in_file(file_path, main_version)
