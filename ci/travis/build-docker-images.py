import datetime
import os
import re
import shutil
import subprocess
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
    return os.environ.get("$TRAVIS_BRANCH") != "master"


def _get_curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


def _get_root_dir():
    return os.path.join(_get_curr_dir(), "../../")


def _get_wheel_name():
    return _subprocess_wrapper(
        f"basename {_get_root_dir()}/.whl/*cp37m-manylinux*")


def _log_in():
    global LOGGED_IN
    if LOGGED_IN:
        return
    _subprocess_wrapper(f"echo \"$DOCKER_PASSWORD\" | " +
                        "docker login -u {DOCKER_USERNAME} --password-stdin")
    LOGGED_IN = True


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


def _docker_push(image):
    if _merge_build():
        _log_in()
        _subprocess_wrapper(f"docker push {image}")
    else:
        print("Skipping docker push because it's in PR environment.")


def _build_helper(image_name):
    built_images = []
    for gpu in ["", "-gpu"]:
        if image_name == "base-deps":
            gpu_arg = "BASE_IMAGE={}".format(
                "nvidia/cuda:10.1-cudnn8-runtime-ubuntu18.04"
                if gpu else "ubuntu:focal")
        else:
            gpu_arg = f"GPU={gpu}"
        wheel_arg = "--build-arg WHEEL_PATH=.whl/" + _get_wheel_name(
        ) if "ray" in image_name else ""
        build_command = (
            f"docker build --no-cache --build-arg {gpu_arg} {wheel_arg} "
            f"-t rayproject/{image_name}:nightly:{gpu} "
            f"{_get_root_dir()}/docker/{image_name}")
        _subprocess_wrapper(build_command)
        built_images.append(f"rayproject/{image_name}:nightly:{gpu}")
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
        ret_list = []
        for image in ["base-deps", "ray-deps"]:
            ret_list.extend(_build_helper(image))
        return ret_list
    else:
        print("Just pulling images!")
        _subprocess_wrapper("docker pull rayproject/base-deps:nightly")
        _subprocess_wrapper("docker pull rayproject/ray-deps:nightly-gpu")
        _subprocess_wrapper("docker pull rayproject/ray-deps:nightly")
        return []


def build_ray() -> List[str]:
    return _build_helper("ray")


def build_ray_ml() -> List[str]:
    root_dir = _get_root_dir()
    requirement_files = _subprocess_wrapper(
        f"basename {_get_root_dir()}/python/requirements*.txt").split()
    for fl in requirement_files:
        shutil.copy(
            os.path.join(root_dir, "python/", fl),
            os.path.join(root_dir, "docker/ray-ml/"))
    return_list = _build_helper("ray-ml")
    autoscaler_list = [x.replace("ray-ml", "autoscaler") for x in return_list]
    return_list.extend(autoscaler_list)
    return return_list


# For non-release builds, push "nightly" & "sha"
# For release builds, push "nightly" & "latest" & "x.x.x"
def push_and_tag_images(image_list) -> List[str]:
    def re_tag(image, new_tag):
        return image.replace("nightly", new_tag)

    date_tag = datetime.datetime.now().strftime("%Y-%m-%d")
    sha_tag = os.environ.get("TRAVIS_COMMIT")[:6]
    if _release_build():
        release_name = re.search("[0-9]\.[0-9]\.[0-9]",
                                 os.environ.get("TRAVIS_BRANCH"))
        date_tag = release_name
        sha_tag = release_name
    for image in image_list:
        _subprocess_wrapper(f"docker push {image}")

        new_image = re_tag(image, date_tag if "-deps" in image else sha_tag)
        _subprocess_wrapper(f"docker tag {image} {new_image}")
        _subprocess_wrapper(f"docker push {new_image}")

        if _release_build():
            latest = re_tag(image, "latest")
            _subprocess_wrapper(f"docker tag {image} {latest}")
            _subprocess_wrapper(f"docker push {latest}")


# Build base-deps/ray-deps only on file change, 2 weeks, per release
# Build ray, ray-ml, autoscaler every time

if __name__ == "__main__":
    if os.environ.get("TRAVIS") == "true":
        is_docker_affected = _docker_affected()
        if _merge_build() or is_docker_affected:
            copy_wheels()
            images_to_tag = []
            images_to_tag.extend(build_or_pull_base_images(is_docker_affected))
            images_to_tag.extend(build_ray())
            images_to_tag.extend(build_ray_ml())
            push_and_tag_images(images_to_tag)
