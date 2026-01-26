import os
import shutil
import tempfile
from pathlib import Path

import click

from ci.ray_ci.automation.crane_lib import call_crane_export
from ci.ray_ci.utils import ecr_docker_login, logger


def _default_output_dir() -> str:
    """Get default output directory, using BUILD_WORKSPACE_DIRECTORY if available."""
    workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
    if workspace:
        return os.path.join(workspace, ".whl")
    return ".whl"


class ExtractWandaWheelsError(Exception):
    """Error raised when extracting wheels from a Wanda-cached image fails."""


@click.command()
@click.option(
    "--wanda-image-name",
    type=str,
    required=True,
    help="Name of the Wanda-cached image (e.g., 'forge').",
)
@click.option(
    "--rayci-work-repo",
    type=str,
    envvar="RAYCI_WORK_REPO",
    required=True,
    help="RAYCI work repository URL. Read from RAYCI_WORK_REPO env var if not set.",
)
@click.option(
    "--rayci-build-id",
    type=str,
    envvar="RAYCI_BUILD_ID",
    required=True,
    help="RAYCI build ID. Read from RAYCI_BUILD_ID env var if not set.",
)
@click.option(
    "--output-dir",
    default=None,
    help="Directory to output extracted wheels (default: .whl in workspace)",
)
def main(
    wanda_image_name: str,
    rayci_work_repo: str,
    rayci_build_id: str,
    output_dir: str | None,
) -> None:
    """
    Extract wheels from a Wanda-cached image to the specified output directory.
    """
    # Clear existing wheels and create output directory
    if output_dir is None:
        output_dir = _default_output_dir()
    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True)

    wanda_image = f"{rayci_work_repo}:{rayci_build_id}-{wanda_image_name}"
    logger.info(f"Extracting wheels from: {wanda_image}")

    ecr_registry = rayci_work_repo.split("/")[0]
    ecr_docker_login(ecr_registry)

    with tempfile.TemporaryDirectory() as tmpdir:
        call_crane_export(wanda_image, tmpdir)
        wheels = list(Path(tmpdir).rglob("*.whl"))
        for wheel in wheels:
            shutil.move(wheel, output_path / wheel.name)

    # Verify that wheels were actually extracted by looking at the output directory
    wheels = list(output_path.rglob("*.whl"))
    if not wheels:
        raise ExtractWandaWheelsError(
            f"No wheel files were extracted from image: {wanda_image}."
        )

    logger.info(
        f"Extracted {len(wheels)} wheel(s) to: {output_path.absolute().resolve()}"
    )
    for wheel in wheels:
        logger.info(f"  {wheel.name}")


if __name__ == "__main__":
    main()
