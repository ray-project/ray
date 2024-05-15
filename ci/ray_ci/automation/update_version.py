import click

from ci.ray_ci.automation.update_version_lib import (
    get_current_version,
    update_file_version,
)


@click.command()
@click.option("--root_dir", required=True, type=str)
@click.option("--new_version", required=True, type=str)
def main(root_dir: str, new_version: str):
    """
    Update the version in the files to the specified version.
    """
    main_version, java_version = get_current_version(root_dir)

    update_file_version(
        main_version,
        java_version,
        new_version,
        root_dir,
    )


if __name__ == "__main__":
    main()
