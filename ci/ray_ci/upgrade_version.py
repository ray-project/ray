import click
import os
import subprocess

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

non_java_files = {
    'ci/ray_ci/utils.py',
    'python/ray/_version.py',
    'src/ray/common/constants.h',
}

def list_java_files():
    files = set()
    for root_dir, _, file_names in os.walk(bazel_workspace_dir):
        for file_name in file_names:
            if file_name in ["pom.xml", "pom_template.xml"]:
                files.add(os.path.join(root_dir, file_name))
    return files


def get_current_version(non_java_version: str, java_version: str):
    """
    Scan for current Ray version and update the version global variables.
    """
    version_file_path = os.path.join(bazel_workspace_dir, "python/ray/_version.py")
    output = subprocess.run(['python', version_file_path], stdout=subprocess.PIPE, text=True)

    if output.returncode != 0 or not output.stdout:
        raise Exception('Failed to get current version')
    version = output.stdout.split(' ')[0]
    
    if version != non_java_version:
        non_java_version = version
        java_version = version
    return (non_java_version, java_version)

def upgrade_file_version(non_java_files, java_files, non_java_version: str, java_version: str, new_version: str):
    """
    Modify the version in the files to the specified version.
    """
    for file_path in non_java_files | java_files:
        with open(os.path.join(bazel_workspace_dir, file_path), 'r') as f:
            content = f.read()
            print("Content: ", content)
        current_version = non_java_version if file_path in non_java_files else java_version
        content = content.replace(current_version, new_version)

        with open(os.path.join(bazel_workspace_dir, file_path), 'w') as f:
            f.write(content)

@click.command()
@click.option("--new_version", required=True, type=str)
def main(new_version: str):
    """
    Update the version in the files to the specified version.
    """
    non_java_version = "3.0.0.dev0"
    java_version = "2.0.0-SNAPSHOT"
    non_java_version, java_version = get_current_version(non_java_version, java_version)

    java_files = list_java_files()
    upgrade_file_version(non_java_files, java_files, non_java_version, java_version, new_version)

if __name__ == "__main__":
    main()