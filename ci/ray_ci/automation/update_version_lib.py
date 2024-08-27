import os
import subprocess

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

MASTER_BRANCH_VERSION = "3.0.0.dev0"
MASTER_BRANCH_JAVA_VERSION = "2.0.0-SNAPSHOT"


def list_java_files(root_dir: str):
    """
    Scan the directories and return the sorted list of
        pom.xml and pom_template.xml files.
    """
    files = []
    for current_root_dir, _, file_names in os.walk(root_dir):
        for file_name in file_names:
            if file_name in ["pom.xml", "pom_template.xml"]:
                files.append(os.path.join(current_root_dir, file_name))
    return sorted(files)


def get_check_output(file_path: str):
    return subprocess.check_output(["python", file_path], text=True)


def get_current_version(root_dir: str):
    """
    Scan for current Ray version and return the current versions.
    """
    version_file_path = os.path.join(root_dir, "python/ray/_version.py")
    ray_version_output = get_check_output(version_file_path).split()
    if len(ray_version_output) != 2:
        raise ValueError(
            f"Unexpected output from {version_file_path}: {ray_version_output}"
        )
    version = ray_version_output[0]

    if version != MASTER_BRANCH_VERSION:
        main_version = version
        java_version = version
        return main_version, java_version
    return MASTER_BRANCH_VERSION, MASTER_BRANCH_JAVA_VERSION


def update_file_version(
    main_version: str,
    java_version: str,
    new_version: str,
    root_dir: str,
):
    """
    Modify the version in the files to the specified version.
    """

    def list_java_files():
        """
        Scan the directories and return the sorted list of
            pom.xml and pom_template.xml files.
        """
        files = []
        for current_root_dir, _, file_names in os.walk(root_dir):
            for file_name in file_names:
                if file_name in ["pom.xml", "pom_template.xml"]:
                    files.append(os.path.join(current_root_dir, file_name))
        return sorted(files)

    non_java_files = [
        "ci/ray_ci/utils.py",
        "python/ray/_version.py",
        "src/ray/common/constants.h",
    ]
    non_java_files.sort()
    java_files = list_java_files()
    assert len(java_files) > 0

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

    for file_path in non_java_files:
        replace_version_in_file(file_path, main_version)
    for file_path in java_files:
        replace_version_in_file(file_path, java_version)
