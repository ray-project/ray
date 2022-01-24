import argparse
import os
import shutil
import subprocess
import tempfile


def build_multinode_image(source_image: str, target_image: str):
    """Build docker image from source_image.

    This docker image will contain packages needed for the fake multinode
    docker cluster to work.
    """
    tempdir = tempfile.mkdtemp()

    dockerfile = os.path.join(tempdir, "Dockerfile")
    with open(dockerfile, "wt") as f:
        f.write(f"FROM {source_image}\n")
        f.write("RUN sudo apt update\n")
        f.write("RUN sudo apt install -y openssh-server\n")

    subprocess.check_output(
        f"docker build -t {target_image} .", shell=True, cwd=tempdir)

    shutil.rmtree(tempdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source_image", type=str)
    parser.add_argument("target_image", type=str)
    args = parser.parse_args()

    build_multinode_image(args.source_image, args.target_image)
