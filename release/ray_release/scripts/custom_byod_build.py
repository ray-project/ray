from typing import Optional

import click

from ray_release.byod.build import build_anyscale_custom_byod_image
from ray_release.byod.build_context import BuildContext


@click.command()
@click.option("--image-name", type=str, required=True)
@click.option("--base-image", type=str, required=True)
@click.option("--post-build-script", type=str)
@click.option("--python-depset", type=str)
def main(
    image_name: str,
    base_image: str,
    post_build_script: Optional[str],
    python_depset: Optional[str],
):
    if not post_build_script and not python_depset:
        raise click.UsageError(
            "Either post_build_script or python_depset must be provided"
        )
    build_context: BuildContext = {}
    if post_build_script:
        build_context["post_build_script"] = post_build_script
    if python_depset:
        build_context["python_depset"] = python_depset
    build_anyscale_custom_byod_image(image_name, base_image, build_context)


if __name__ == "__main__":
    main()
