import os
from typing import Optional

import click

from ci.ray_ci.automation.update_version_lib import (
    get_current_version,
    update_file_version,
)


@click.command()
@click.option("--new_version", required=True, type=str)
@click.option("--root_dir", required=False, type=str)
def main(new_version: str, root_dir: Optional[str] = None):
    """
    Update the version in the files to the specified version.
    """
    if not root_dir:
        root_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
        if not root_dir:
            raise Exception("Please specify --root_dir when not running with Bazel.")

    main_version, java_version = get_current_version(root_dir)

    update_file_version(
        main_version,
        java_version,
        new_version,
        root_dir,
    )


if __name__ == "__main__":
    main()
