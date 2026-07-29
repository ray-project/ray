"""
Extract globs from a Wanda-cached image to a local directory.
"""

import os
import shutil
import tempfile
from pathlib import Path

import click

from ci.ray_ci.automation.crane_lib import CraneError, call_crane_export
from ci.ray_ci.utils import ecr_docker_login, logger


@click.command()
@click.option(
    "--wanda-image-name",
    type=str,
    required=True,
    help="Wanda image name (without repo/build-id prefix).",
)
@click.option(
    "--file-glob",
    type=str,
    required=True,
    help="Glob pattern for files to extract (e.g. '*.whl', '*.tgz').",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=".",
    help="Directory to place extracted files (default: current directory).",
)
@click.option(
    "--rayci-work-repo",
    type=str,
    envvar="RAYCI_WORK_REPO",
    required=True,
    help="RAYCI work repository URL.",
)
@click.option(
    "--rayci-build-id",
    type=str,
    envvar="RAYCI_BUILD_ID",
    required=True,
    help="RAYCI build ID.",
)
def main(
    wanda_image_name: str,
    file_glob: str,
    output_dir: Path,
    rayci_work_repo: str,
    rayci_build_id: str,
) -> None:
    """Extract artifacts matching a glob pattern from a Wanda-cached image."""
    wanda_image = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
    logger.info(f"Extracting '{file_glob}' from: {wanda_image}")

    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    if not output_dir.is_absolute():
        workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
        if workspace:
            output_dir = Path(workspace) / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            call_crane_export(wanda_image, tmpdir)
        except CraneError as e:
            raise click.ClickException(str(e))

        matches = [f for f in Path(tmpdir).rglob(file_glob) if f.is_file()]
        if not matches:
            raise click.ClickException(
                f"No files matching '{file_glob}' in image {wanda_image}"
            )

        seen = set()
        for f in matches:
            if f.name in seen:
                raise click.ClickException(
                    f"Duplicate basename '{f.name}' in image {wanda_image}: "
                    f"found at multiple paths"
                )
            seen.add(f.name)

        for f in matches:
            dest = output_dir / f.name
            shutil.copy2(f, dest)
            logger.info(f"  {f.name} ({f.stat().st_size} bytes)")

    logger.info(f"Extracted {len(matches)} file(s) to {output_dir}")


if __name__ == "__main__":
    main()
