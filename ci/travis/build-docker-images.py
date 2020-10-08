import datetime
import glob
import os
import re
import shutil
import subprocess
import sys
from typing import List

import docker

DOCKER_USERNAME = "raytravisbot"
DOCKER_CLIENT = None


def _subprocess_wrapper(cmd):
    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if result.returncode != 0:
        print(f"COMMAND FAILED: {cmd}")
        print(f"STDOUT is: {result.stdout}")
        print(f"STDERR is: {result.stderr}")
        raise Exception("Command Failed") from None
    return (result.stdout or b"").decode()


def _merge_build():
    return os.environ.get("TRAVIS_PULL_REQUEST") == "false"


def _release_build():
    branch = os.environ.get("$TRAVIS_BRANCH")
    return branch != "master" and "releases" in branch


def _get_curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


def _get_root_dir():
    return os.path.join(_get_curr_dir(), "../../")


def _get_wheel_name():
    matches = glob.glob(f"{_get_root_dir()}/.whl/*cp37m-manylinux*")
    assert len(matches) == 1
    f"Found ({len(matches)}) matches '*cp37m-manylinux*' instead of 1"
    return matches[0]


def _docker_affected():
    variable_definitions = _subprocess_wrapper(
        f"python {_get_curr_dir()}/determine_tests_to_run.py").split()
    env_var_dict = {
        x.split("=")[0]: x.split("=")[1]
        for x in variable_definitions
    }
    affected = env_var_dict["RAY_CI_DOCKER_AFFECTED"] == "1" or \
        env_var_dict["RAY_CI_PYTHON_DEPENDENCIES_AFFECTED"] == "1"
    print(f"Docker affected: {affected}")
    return affected


def _build_helper(image_name) -> List[str]:
    built_images = []
    for gpu in ["-cpu", "-gpu"]:
        build_args = {}
        if image_name == "base-deps":
            build_args["BASE_IMAGE"] = (
                "nvidia/cuda:10.1-cudnn8-runtime-ubuntu18.04"
                if gpu else "ubuntu:focal")
        else:
            build_args["GPU"] = gpu

        if "ray" in image_name:
            build_args["WHEEL_PATH"] = f".whl/{_get_wheel_name()}"

        tagged_name = f"rayproject/{image_name}:nightly:{gpu}"

        DOCKER_CLIENT.api.build(
            path=f"{_get_root_dir()}/docker/{image_name}",
            tag=tagged_name,
            nocache=True,
            buildargs=build_args)

        print("BUILT: ", tagged_name)
        built_images.append(tagged_name)
    return built_images


def copy_wheels():
    root_dir = _get_root_dir()
    wheel = _get_wheel_name()
    source = os.path.join(root_dir, ".whl", wheel)
    shutil.copy(source, os.path.join(root_dir, "docker/ray/.whl/"))
    shutil.copy(source, os.path.join(root_dir, "docker/ray-deps/.whl/"))


def build_or_pull_base_images(is_docker_affected: bool) -> List[str]:
    """Returns images to tag and build"""
    _ = DOCKER_CLIENT.api.pull(
        repository="rayproject/base-deps", tag="nightly")

    age = DOCKER_CLIENT.api.inspect_image("rayproject/base-deps:nightly")[
        "Created"]
    short_date = datetime.datetime.strptime(age.split("T")[0], "%Y-%m-%d")
    is_stale = (
        datetime.datetime.now() - short_date) > datetime.timedelta(days=14)

    if is_stale or is_docker_affected or _release_build():
        for image in ["base-deps", "ray-deps"]:
            _build_helper(image)
        return True
    else:
        print("Just pulling images!")
        _ = DOCKER_CLIENT.api.pull(
            repository="rayproject/base-deps", tag="nightly-cpu")
        _ = DOCKER_CLIENT.api.pull(
            repository="rayproject/base-deps", tag="nightly-gpu")

        _ = DOCKER_CLIENT.api.pull(
            repository="rayproject/ray-deps", tag="nightly-gpu")
        _ = DOCKER_CLIENT.api.pull(
            repository="rayproject/ray-deps", tag="nightly-cpu")
        return False


def build_ray():
    return _build_helper("ray")


def build_ray_ml():
    root_dir = _get_root_dir()
    requirement_files = glob.glob(
        f"{_get_root_dir()}/python/requirements*.txt")
    for fl in requirement_files:
        shutil.copy(fl, os.path.join(root_dir, "docker/ray-ml/"))
    ray_ml_images = _build_helper("ray-ml")
    for img in ray_ml_images:
        tag = img.split(":")[-1]
        DOCKER_CLIENT.api.tag(
            image=img, repository="rayproject/autoscaler", tag=tag)


# For non-release builds, push "nightly" & "sha"
# For release builds, push "nightly" & "latest" & "x.x.x"
def push_and_tag_images(push_base_images: bool):
    if _merge_build():
        docker_password = os.environ.get("DOCKER_PASSWORD")
        assert docker_password is not None, "DOCKER_PASSWORD not set."
        DOCKER_CLIENT.api.login(
            username=DOCKER_USERNAME,
            password=os.environ.get("DOCKER_PASSWORD"))

    def docker_push(image, tag):
        if _merge_build():
            DOCKER_CLIENT.api.push(image, tag=tag)
        else:
            print(
                "This is a PR Build! On a merge build, we would normally push "
                f"to: {image}:{tag}")

    def get_new_tag(old_tag, new_tag):
        return old_tag.replace("nightly", new_tag)

    date_tag = datetime.datetime.now().strftime("%Y-%m-%d")
    sha_tag = os.environ.get("TRAVIS_COMMIT")[:6]
    if _release_build():
        release_name = re.search("[0-9]\.[0-9]\.[0-9]",
                                 os.environ.get("TRAVIS_BRANCH"))
        date_tag = release_name
        sha_tag = release_name

    image_list = ["ray", "ray-ml", "autoscaler"]
    if push_base_images:
        image_list.extend(["base-deps", "ray-deps"])

    for image in image_list:
        full_image = f"rayproject/{image}"

        # Generate <IMAGE_NAME>:nightly from nightly-cpu
        DOCKER_CLIENT.api.tag(
            image=f"{full_image}:nightly-cpu",
            repository=full_image,
            tag="nightly")

        for arch_tag in ["-cpu", "-gpu", ""]:
            full_arch_tag = f"nightly{arch_tag}"
            # Tag and push rayproject/<image>:nightly<arch_tag>
            docker_push(full_image, full_arch_tag)

            specific_tag = get_new_tag(
                full_arch_tag, date_tag if "-deps" in image else sha_tag)
            # Tag and push rayproject/<image>:<sha/date><arch_tag>
            DOCKER_CLIENT.api.tag(
                image=full_image, repository=full_image, tag=specific_tag)
            docker_push(full_image, specific_tag)

            if _release_build():
                latest_tag = get_new_tag(full_arch_tag, "latest")
                # Tag and push rayproject/<image>:latest<arch_tag>
                DOCKER_CLIENT.api.tag(
                    image=full_image, repository=full_image, tag=latest_tag)
                docker_push(full_image, latest_tag)


# Build base-deps/ray-deps only on file change, 2 weeks, per release
# Build ray, ray-ml, autoscaler every time

if __name__ == "__main__":
    print("RUNNING WITH: ", sys.version)
    if os.environ.get("TRAVIS") == "true":
        is_docker_affected = _docker_affected()
        if _merge_build() or is_docker_affected:
            DOCKER_CLIENT = docker.from_env()
            copy_wheels()
            freshly_built = build_or_pull_base_images(is_docker_affected)
            build_ray()
            build_ray_ml()
            push_and_tag_images(freshly_built)
