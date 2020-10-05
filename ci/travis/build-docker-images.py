import datetime
import os
import re
import shutil
import subprocess
import sys
from typing import List

# Why, because fuck bash
LOGGED_IN = False
DOCKER_USERNAME = "raytravisbot"


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
    return _subprocess_wrapper(
        f"basename {_get_root_dir()}/.whl/*cp37m-manylinux*")


def _docker_affected():
    variable_definitions = _subprocess_wrapper(
        f"python {_get_curr_dir()}/determine_tests_to_run.py").split()
    env_var_dict = {
        x.split("=")[0]: x.split("=")[1]
        for x in variable_definitions
    }
    affected = env_var_dict["RAY_CI_DOCKER_AFFECTED"] == 1 or \
        env_var_dict["RAY_CI_PYTHON_DEPENDENCIES_AFFECTED"] == 1
    print(f"Docker affected: {affected}")
    return affected


def _build_helper(image_name) -> List[str]:
    built_images = []
    for gpu in ["-cpu", "-gpu"]:
        if image_name == "base-deps":
            gpu_arg = "BASE_IMAGE={}".format(
                "nvidia/cuda:10.1-cudnn8-runtime-ubuntu18.04"
                if gpu else "ubuntu:focal")
        else:
            gpu_arg = f"GPU={gpu}"

        if "ray" in image_name:
            wheel_arg = f"--build-arg WHEEL_PATH=.whl/{_get_wheel_name()}"
        else:
            wheel_arg = ""

        tagged_name = f"rayproject/{image_name}:nightly:{gpu}"
        build_command = (
            f"docker build --no-cache --build-arg {gpu_arg} {wheel_arg} "
            f"-t {tagged_name} {_get_root_dir()}/docker/{image_name}")

        _subprocess_wrapper(build_command)
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
    _subprocess_wrapper("docker pull rayproject/base-deps:nightly")
    age = _subprocess_wrapper(
        "docker inspect -f '{{ .Created }}' rayproject/base-deps:nightly")
    short_date = datetime.datetime.strptime(age.split("T")[0], "%Y-%m-%d")
    is_stale = (
        datetime.datetime.now() - short_date) > datetime.timedelta(days=14)

    if is_stale or is_docker_affected or _release_build():
        for image in ["base-deps", "ray-deps"]:
            _build_helper(image)
        return True
    else:
        print("Just pulling images!")
        _subprocess_wrapper("docker pull rayproject/base-deps:nightly-cpu")
        _subprocess_wrapper("docker pull rayproject/ray-deps:nightly-gpu")
        _subprocess_wrapper("docker pull rayproject/ray-deps:nightly-cpu")
        return False


def build_ray() -> None:
    return _build_helper("ray")


def build_ray_ml() -> None:
    root_dir = _get_root_dir()
    requirement_files = _subprocess_wrapper(
        f"basename {_get_root_dir()}/python/requirements*.txt").split()
    for fl in requirement_files:
        shutil.copy(
            os.path.join(root_dir, "python/", fl),
            os.path.join(root_dir, "docker/ray-ml/"))
    ray_ml_images = _build_helper("ray-ml")
    for img in ray_ml_images:
        _subprocess_wrapper(
            f"docker tag {img} {img.replace('ray-ml', 'autoscaler')}")


# For non-release builds, push "nightly" & "sha"
# For release builds, push "nightly" & "latest" & "x.x.x"
def push_and_tag_images(push_base_images: bool) -> None:
    if _merge_build():
        _subprocess_wrapper(
            "echo \"$DOCKER_PASSWORD\" | " +
            f"docker login -u {DOCKER_USERNAME} --password-stdin")

    def docker_push(image):
        if _merge_build():
            _subprocess_wrapper(f"docker push {image}")
        else:
            print(f"PR Build, would normally push: {image}")

    def re_tag(image, new_tag):
        return image.replace("nightly", new_tag)

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
        nightly_base_name = f"rayproject/{image}:nightly"

        _subprocess_wrapper(
            f"docker tag {nightly_base_name}-cpu {nightly_base_name}")
        for arch_tag in ["-cpu", "-gpu", ""]:
            # Tag and push rayproject/<image>:nightly<arch_tag>
            nightly_tag = f"{nightly_base_name}{arch_tag}"
            docker_push(nightly_tag)

            specific_image = re_tag(nightly_tag, date_tag
                                    if "-deps" in image else sha_tag)
            # Tag and push rayproject/<image>:<sha/date><arch_tag>
            _subprocess_wrapper(f"docker tag {nightly_tag} {specific_image}")
            docker_push(specific_image)

            if _release_build():
                latest = re_tag(nightly_tag, "latest")
                # Tag and push rayproject/<image>:latest<arch_tag>
                _subprocess_wrapper(f"docker tag {nightly_tag} {latest}")
                docker_push(latest)


# Build base-deps/ray-deps only on file change, 2 weeks, per release
# Build ray, ray-ml, autoscaler every time

if __name__ == "__main__":
    print("RUNNING WITH: ", sys.version)
    if os.environ.get("TRAVIS") == "true":
        is_docker_affected = _docker_affected()
        if _merge_build() or is_docker_affected:
            copy_wheels()
            freshly_built = build_or_pull_base_images(is_docker_affected)
            build_ray()
            build_ray_ml()
            push_and_tag_images(freshly_built)
