import click
from ray_release.byod.build import build_anyscale_custom_byod_image
from typing import Optional


@click.command()
@click.option("--image-name", type=str, required=True)
@click.option("--base-image", type=str, required=True)
@click.option("--post-build-script", type=str, required=True)
@click.option("--python-depset", type=str)
def main(
    image_name: str,
    base_image: str,
    post_build_script: str,
    python_depset: Optional[str],
):
    build_anyscale_custom_byod_image(
        image_name, base_image, post_build_script, python_depset
    )


if __name__ == "__main__":
    main()
