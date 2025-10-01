import click
from ray_release.byod.build import build_anyscale_custom_byod_image


@click.command()
@click.option("--image-name", type=str, required=True)
@click.option("--base-image", type=str, required=True)
@click.option("--post-build-script", type=str, required=True)
def main(image_name: str, base_image: str, post_build_script: str):
    build_anyscale_custom_byod_image(image_name, base_image, post_build_script)


if __name__ == "__main__":
    main()
