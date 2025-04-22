import sys
import subprocess
import argparse
import os
import json
import base64
import logging

logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def install_ray_package(ray_version, whl_dir):
    pip_install_command = [sys.executable, "-m", "pip", "install", "-U"]

    if whl_dir:
        # generate whl file name
        whl_file_name = (
            f"ant_ray-*cp{sys.version_info[0]}" f"{sys.version_info[1]}*.whl"
        )

        # whl file path

        whl_file_path = os.path.join(whl_dir, whl_file_name)
        pip_install_command.append(whl_file_path)
    else:
        # generate ray package name
        ray_package_name = f"ant_ray=={ray_version}"
        pip_install_command.append(ray_package_name)

    logger.info("Starting install ray: {}".format(pip_install_command))

    # install whl
    result = subprocess.run(" ".join(pip_install_command), shell=True)

    if result.returncode != 0:
        raise RuntimeError(f"Failed to install ray, got ex: {result.stderr}")


def install_pip_package(pip_packages, isolate_pip_installation):
    formatted_pip_packages = []
    for package in pip_packages:
        package = package.strip("'")
        formatted_pip_packages.append(f"{package}")
    pip_install_command = [sys.executable, "-m", "pip", "install", "-U"]

    pip_install_command.extend(formatted_pip_packages)
    logger.info("Starting install pip package: {}".format(pip_install_command))
    env = os.environ.copy()
    if isolate_pip_installation:
        # Remove PYTHONPATH from the environment variables
        env.pop("PYTHONPATH", None)
        # install pip packages without python path
        logger.info("Installing pip packages without `PYTHONPATH`.")
    result = subprocess.run(pip_install_command, env=env)

    if result.returncode != 0:
        raise Exception(f"Failed to install pip packages, got ex {result.stderr}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Install Ray whl package and pip packages."
    )
    parser.add_argument(
        "--whl-dir", required=False, help="Directory containing Ray whl packages."
    )

    parser.add_argument(
        "--packages", required=False, help="Containing pip packages from runtime env."
    )

    parser.add_argument(
        "--ray-version", required=False, help="Install Which version for Ray."
    )

    parser.add_argument(
        "--isolate-pip-installation",
        required=False,
        help="Enable to install pip install without python path",
    )

    args = parser.parse_args()
    isolate_pip_installation = (
        args.isolate_pip_installation.lower() == "true"
        if args.isolate_pip_installation is not None
        else False
    )

    if args.ray_version or args.whl_dir:
        install_ray_package(args.ray_version, args.whl_dir)
    if args.packages:
        pip_packages = json.loads(
            base64.b64decode(args.packages.encode("utf-8")).decode("utf-8")
        )
        install_pip_package(pip_packages, isolate_pip_installation)
