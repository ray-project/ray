#!/usr/bin/env python
# ruff: noqa: E402
"""This script allows you to develop Ray Python code without needing to compile
Ray.
See https://docs.ray.io/en/master/development.html#building-ray-python-only"""

import os
import sys

# types.py can conflict with stdlib's types.py in some python versions,
# see https://github.com/python/cpython/issues/101210.
# To avoid import errors, we move the current working dir to the end of sys.path.
this_dir = os.path.dirname(__file__)
if this_dir in sys.path:
    sys.path.remove(this_dir)
    sys.path.append(this_dir)

import argparse
import click
import shutil
import subprocess

import ray


def do_link(package, force=False, skip_list=None, allow_list=None, local_path=None):
    if skip_list and package in skip_list:
        print(f"Skip creating symbolic link for {package}")
        return
    if allow_list is not None and package not in allow_list:
        print(f"Skip creating symbolic link for {package} (not in allow list)")
        return
    package_home = os.path.abspath(os.path.join(ray.__file__, f"../{package}"))
    # Infer local_path automatically.
    if local_path is None:
        local_path = f"../{package}"
    local_home = os.path.abspath(os.path.join(__file__, local_path))
    # If installed package dir does not exist, continue either way. We'll
    # remove it/create a link from there anyways.
    if not os.path.isdir(package_home) and not os.path.isfile(package_home):
        print(f"{package_home} does not exist. Continuing to link.")
    # Make sure the path we are linking to does exist.
    assert os.path.exists(local_home), local_home
    # Confirm with user.
    if not force and not click.confirm(
        f"This will replace:\n  {package_home}\nwith " f"a symlink to:\n  {local_home}",
        default=True,
    ):
        return

    # Windows: Create directory junction.
    if os.name == "nt":
        try:
            shutil.rmtree(package_home)
        except FileNotFoundError:
            pass
        except OSError:
            os.remove(package_home)

        # create symlink for directory or file
        if os.path.isdir(local_home):
            subprocess.check_call(
                ["mklink", "/J", package_home, local_home], shell=True
            )
        elif os.path.isfile(local_home):
            subprocess.check_call(
                ["mklink", "/H", package_home, local_home], shell=True
            )
        else:
            print(f"{local_home} is neither directory nor file. Link failed.")

    # Posix: Use `ln -s` to create softlink.
    else:
        sudo = []
        if not os.access(os.path.dirname(package_home), os.W_OK):
            print("You don't have write permission " f"to {package_home}, using sudo:")
            sudo = ["sudo"]
        print(f"Creating symbolic link from \n {local_home} to \n {package_home}")

        # Preserve ray/serve/generated
        serve_temp_dir = "/tmp/ray/_serve/"
        if package == "serve":
            # Copy generated folder to a temp dir
            generated_folder = os.path.join(package_home, "generated")
            if not os.path.exists(serve_temp_dir):
                os.makedirs(serve_temp_dir)
            subprocess.check_call(["mv", generated_folder, serve_temp_dir])

        # Create backup of the old directory if it exists
        if os.path.exists(package_home):
            backup_dir = f"{package_home}.bak"
            print(f"Creating backup of {package_home} to {backup_dir}")
            subprocess.check_call(sudo + ["cp", "-r", package_home, backup_dir])

        subprocess.check_call(sudo + ["rm", "-rf", package_home])
        subprocess.check_call(sudo + ["ln", "-s", local_home, package_home])

        # Move generated folder to local_home
        if package == "serve":
            tmp_generated_folder = os.path.join(serve_temp_dir, "generated")
            package_generated_folder = os.path.join(package_home, "generated")
            if not os.path.exists(package_generated_folder):
                subprocess.check_call(
                    ["mv", tmp_generated_folder, package_generated_folder]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter, description="Setup dev."
    )
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Don't ask for confirmation."
    )
    parser.add_argument(
        "--skip",
        "-s",
        nargs="*",
        help="List of folders to skip linking to facilitate workspace dev",
        required=False,
    )
    parser.add_argument(
        "--allow",
        "-a",
        nargs="*",
        help="List of folders to link (only these will be linked)",
        required=False,
    )
    parser.add_argument(
        "--extras",
        "-e",
        nargs="*",
        help="List of extra folders to link to facilitate workspace dev",
        required=False,
    )

    args = parser.parse_args()
    if args.skip and args.allow:
        print("Error: --skip and --allow cannot be used together.")
        sys.exit(1)

    if not args.yes:
        print("NOTE: Use '-y' to override all python files without confirmation.")

    # Dictionary of packages to link, with optional local_path
    packages_to_link = {
        "llm": None,
        "serve/llm": None,
        "data/llm.py": None,
        "rllib": "../../../rllib",
        "air": None,
        "tune": None,
        "train": None,
        "autoscaler": None,
        "cloudpickle": None,
        "data": None,
        "scripts": None,
        "internal": None,
        "tests": None,
        "experimental": None,
        "util": None,
        "workflow": None,
        "serve": None,
        "dag": None,
        "widgets": None,
        "cluster_utils.py": None,
        "_private": None,
        "_common": None,
        "dashboard": None,
    }

    # Link all packages using a for loop
    for package, local_path in packages_to_link.items():
        do_link(
            package,
            force=args.yes,
            skip_list=args.skip,
            allow_list=args.allow,
            local_path=local_path,
        )

    if args.extras is not None:
        for package in args.extras:
            do_link(package, force=args.yes, skip_list=args.skip, allow_list=args.allow)

    print(
        "Created links.\n\nIf you run into issues initializing Ray, please "
        "ensure that your local repo and the installed Ray are in sync "
        "(pip install -U the latest wheels at "
        "https://docs.ray.io/en/master/installation.html, "
        "and ensure you are up-to-date on the master branch on git).\n\n"
        "Note that you may need to delete the package symlinks when pip "
        "installing new Ray versions to prevent pip from overwriting files "
        "in your git repo."
    )
