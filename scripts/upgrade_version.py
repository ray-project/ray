import click
import os
import subprocess
import sys

RAY_ABSOLUTE_PATH = ""

NON_JAVA_FILES = set([
    'ci/ray_ci/utils.py',
    'python/ray/_version.py',
    'src/ray/common/constants.h',
])

JAVA_FILES = set([
    'java/api/pom_template.xml',
    'java/performance_test/pom_template.xml',
    'java/pom.xml',
    'java/runtime/pom_template.xml',
    'java/serve/pom_template.xml',
    'java/test/pom_template.xml',
])

NON_JAVA_VERSION = "3.0.0.dev0"
JAVA_VERSION = "2.0.0-SNAPSHOT"
NEW_VERSION = "3.0.0.dev0"

def update_current_version():
    """
    Scan for current Ray version and update the version global variables.
    """
    global NON_JAVA_VERSION
    global JAVA_VERSION
    output = subprocess.run(['python', 'python/ray/_version.py'], stdout=subprocess.PIPE, text=True)

    if output.returncode != 0 or not output.stdout:
        raise Exception('Failed to get current version')

    version = output.stdout.split(' ')[0]
    if version != NON_JAVA_VERSION:
        NON_JAVA_VERSION = version
        JAVA_VERSION = version

def find_ray_absolute_path():
    """
    Find the absolute path of the ray directory.
    """
    current_directory = os.path.dirname(__file__)
    # Loop to parent directory until ray directory is found
    while current_directory != os.path.dirname(current_directory): # Stop at root
        if os.path.basename(current_directory) == 'ray':
            return current_directory
        current_directory = os.path.dirname(current_directory)
    raise Exception('ray directory not found')

def upgrade_version(files):
    """
    Modify the version in the files to the specified version.
    """
    global NEW_VERSION, RAY_ABSOLUTE_PATH, NON_JAVA_VERSION, JAVA_VERSION, NON_JAVA_FILES, JAVA_FILES
    for file_path in files:
        with open(os.path.join(RAY_ABSOLUTE_PATH, file_path), 'r') as f:
            content = f.read()

        current_version = NON_JAVA_VERSION if file_path in NON_JAVA_FILES else JAVA_VERSION
        content = content.replace(current_version, NEW_VERSION)

        with open(os.path.join(RAY_ABSOLUTE_PATH, file_path), 'w') as f:
            f.write(content)

@click.command()
@click.option("--new_version", required=True, type=str)
def main(new_version: str):
    """
    Update the version in the files to the specified version.
    """
    global NEW_VERSION, RAY_ABSOLUTE_PATH

    NEW_VERSION = new_version
    update_current_version()
    RAY_ABSOLUTE_PATH = find_ray_absolute_path()

    upgrade_version(NON_JAVA_FILES | JAVA_FILES)

if __name__ == "__main__":
    main()