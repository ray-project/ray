import os
import subprocess
import sys

import click

from ci.ray_ci.doc.build_cache import BuildCache


@click.command()
@click.option(
    "--ray-checkout-dir",
    default="/ray",
)
def main(ray_checkout_dir: str) -> None:
    """
    This script builds ray doc and upload build artifacts to S3.
    """
    # Add the safe.directory config to the global git config so that the doc build
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", ray_checkout_dir],
        check=True,
    )

    print("--- Building ray doc.", file=sys.stderr)
    _build(ray_checkout_dir)

    dry_run = False
    if os.environ.get("RAYCI_STAGE", "") != "postmerge":
        dry_run = True
        print(
            "Not uploading build artifacts because this is not a postmerge pipeline.",
            file=sys.stderr,
        )
    elif os.environ.get("BUILDKITE_BRANCH") != "master":
        dry_run = True
        print(
            "Not uploading build artifacts because this is not the master branch.",
            file=sys.stderr,
        )

    print("--- Uploading build artifacts to S3.", file=sys.stderr)
    BuildCache(os.path.join(ray_checkout_dir, "doc")).upload(dry_run=dry_run)


def _build(ray_checkout_dir):
    env = os.environ.copy()
    # We need to unset PYTHONPATH to use the Python from the environment instead of
    # from the Bazel runfiles.
    env.update({"PYTHONPATH": ""})
    subprocess.run(
        ["make", "html"],
        cwd=os.path.join(ray_checkout_dir, "doc"),
        env=env,
        check=True,
    )


if __name__ == "__main__":
    main()
