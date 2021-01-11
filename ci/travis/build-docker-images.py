import datetime
import functools
import glob
import os
import re
import runpy
import shutil
import sys
from contextlib import redirect_stdout
from io import StringIO
from typing import List, Tuple

import docker

print = functools.partial(print, file=sys.stderr, flush=True)
DOCKER_USERNAME = "raytravisbot"
DOCKER_CLIENT = None
PYTHON_WHL_VERSION = "cp37m"

DOCKER_HUB_DESCRIPTION = {
    "base-deps": ("Internal Image, refer to "
                  "https://hub.docker.com/r/rayproject/ray"),
    "ray-deps": ("Internal Image, refer to "
                 "https://hub.docker.com/r/rayproject/ray"),
    "ray": "Official Docker Images for Ray, the distributed computing API.",
    "ray-ml": "Developer ready Docker Image for Ray.",
    "autoscaler": (
        "Deprecated image, please use: "
        "https://hub.docker.com/repository/docker/rayproject/ray-ml")
}


def _merge_build():
    return os.environ.get("TRAVIS_PULL_REQUEST").lower() == "false"


def _release_build():
    branch = os.environ.get("TRAVIS_BRANCH")
    if not branch:
        print("Branch not found!")
        print(os.environ)
        print("Environment is above ^^")
        return False
    return branch != "master" and branch.startswith("releases")


def _get_curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


def _get_root_dir():
    return os.path.join(_get_curr_dir(), "../../")


def _get_wheel_name():
    matches = glob.glob(
        f"{_get_root_dir()}/.whl/*{PYTHON_WHL_VERSION}-manylinux*")
    assert len(matches) == 1, (
        f"Found ({len(matches)}) matches "
        f"'*{PYTHON_WHL_VERSION}-manylinux*' instead of 1")
    return os.path.basename(matches[0])


def _docker_affected():
    result = StringIO()
    with redirect_stdout(result):
        runpy.run_path(
            f"{_get_curr_dir()}/determine_tests_to_run.py",
            run_name="__main__")
    variable_definitions = result.getvalue().split()
    env_var_dict = {
        x.split("=")[0]: x.split("=")[1]
        for x in variable_definitions
    }
    affected = env_var_dict["RAY_CI_DOCKER_AFFECTED"] == "1" or \
        env_var_dict["RAY_CI_PYTHON_DEPENDENCIES_AFFECTED"] == "1"
    print(f"Docker affected: {affected}")
    return affected


def _build_cpu_gpu_images(image_name, no_cache=True) -> List[str]:
    built_images = []
    for gpu in ["-cpu", "-gpu"]:
        build_args = {}
        if image_name == "base-deps":
            build_args["BASE_IMAGE"] = (
                "nvidia/cuda:10.1-cudnn7-runtime-ubuntu18.04"
                if gpu == "-gpu" else "ubuntu:focal")
        else:
            build_args["GPU"] = gpu

        if "ray" in image_name:
            build_args["WHEEL_PATH"] = f".whl/{_get_wheel_name()}"

        tagged_name = f"rayproject/{image_name}:nightly{gpu}"
        for i in range(2):
            output = DOCKER_CLIENT.api.build(
                path=os.path.join(_get_root_dir(), "docker", image_name),
                tag=tagged_name,
                nocache=no_cache,
                buildargs=build_args)

            full_output = ""
            try:
                start = datetime.datetime.now()
                current_iter = start
                for line in output:
                    if datetime.datetime.now(
                    ) - current_iter >= datetime.timedelta(minutes=5):
                        current_iter = datetime.datetime.now()
                        elapsed = datetime.datetime.now() - start
                        print(f"Still building {tagged_name} after "
                              f"{elapsed.seconds} seconds")
                    full_output += line.decode("utf-8")
            except Exception as e:
                print(f"FAILURE with error {e}")

            if len(DOCKER_CLIENT.api.images(tagged_name)) == 0:
                print(f"ERROR building: {tagged_name} & error below:")
                print(full_output)
                if (i == 1):
                    raise Exception("FAILED TO BUILD IMAGE")
                print("TRYING AGAIN")
            else:
                break

        print("BUILT: ", tagged_name)
        built_images.append(tagged_name)
    return built_images


def copy_wheels():
    root_dir = _get_root_dir()
    wheel = _get_wheel_name()
    source = os.path.join(root_dir, ".whl", wheel)
    ray_dst = os.path.join(root_dir, "docker/ray/.whl/")
    ray_dep_dst = os.path.join(root_dir, "docker/ray-deps/.whl/")
    os.makedirs(ray_dst, exist_ok=True)
    shutil.copy(source, ray_dst)
    os.makedirs(ray_dep_dst, exist_ok=True)
    shutil.copy(source, ray_dep_dst)


def build_or_pull_base_images(is_docker_affected: bool) -> List[str]:
    """Returns images to tag and build"""
    DOCKER_CLIENT.api.pull(repository="rayproject/base-deps", tag="nightly")

    age = DOCKER_CLIENT.api.inspect_image("rayproject/base-deps:nightly")[
        "Created"]
    short_date = datetime.datetime.strptime(age.split("T")[0], "%Y-%m-%d")
    is_stale = (
        datetime.datetime.now() - short_date) > datetime.timedelta(days=14)

    print("Pulling images for caching")

    DOCKER_CLIENT.api.pull(
        repository="rayproject/base-deps", tag="nightly-cpu")
    DOCKER_CLIENT.api.pull(
        repository="rayproject/base-deps", tag="nightly-gpu")

    DOCKER_CLIENT.api.pull(repository="rayproject/ray-deps", tag="nightly-gpu")
    DOCKER_CLIENT.api.pull(repository="rayproject/ray-deps", tag="nightly-cpu")

    # TODO(ilr) See if any caching happens
    if True or (is_stale or is_docker_affected or _release_build()):
        for image in ["base-deps", "ray-deps"]:
            _build_cpu_gpu_images(image, no_cache=False)
        return True
    else:
        print("Just pulling images!")
        return False


def build_ray():
    return _build_cpu_gpu_images("ray")


def build_ray_ml():
    root_dir = _get_root_dir()
    requirement_files = glob.glob(
        f"{_get_root_dir()}/python/requirements*.txt")
    requirement_files.extend(
        glob.glob(f"{_get_root_dir()}/python/requirements/*.txt"))
    for fl in requirement_files:
        shutil.copy(fl, os.path.join(root_dir, "docker/ray-ml/"))
    ray_ml_images = _build_cpu_gpu_images("ray-ml")
    for img in ray_ml_images:
        tag = img.split(":")[-1]
        DOCKER_CLIENT.api.tag(
            image=img, repository="rayproject/autoscaler", tag=tag)


def _get_docker_creds() -> Tuple[str, str]:
    docker_password = os.environ.get("DOCKER_PASSWORD")
    assert docker_password, "DOCKER_PASSWORD not set."
    return DOCKER_USERNAME, docker_password


# For non-release builds, push "nightly" & "sha"
# For release builds, push "nightly" & "latest" & "x.x.x"
def push_and_tag_images(push_base_images: bool):
    if _merge_build():
        username, password = _get_docker_creds()
        DOCKER_CLIENT.api.login(username=username, password=password)

    def docker_push(image, tag):
        if _merge_build():
            print(f"PUSHING: {image}:{tag}, result:")
            # This docker API is janky. Without "stream=True" it returns a
            # massive string filled with every progress bar update, which can
            # cause CI to back up.
            #
            # With stream=True, it's a line-at-a-time generator of the same
            # info. So we can slow it down by printing every couple hundred
            # lines
            i = 0
            for progress_line in DOCKER_CLIENT.api.push(
                    image, tag=tag, stream=True):
                if i % 100 == 0:
                    print(progress_line)
        else:
            print(
                "This is a PR Build! On a merge build, we would normally push "
                f"to: {image}:{tag}")

    def get_new_tag(old_tag, new_tag):
        return old_tag.replace("nightly", new_tag)

    date_tag = datetime.datetime.now().strftime("%Y-%m-%d")
    sha_tag = os.environ.get("TRAVIS_COMMIT")[:6]
    if _release_build():
        release_name = re.search("[0-9]\.[0-9]\.[0-9].*",
                                 os.environ.get("TRAVIS_BRANCH")).group(0)
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
            # Do not tag release builds because they are no longer up to date
            # after the branch cut.
            if not _release_build():
                # Tag and push rayproject/<image>:nightly<arch_tag>
                docker_push(full_image, full_arch_tag)

            # Ex: specific_tag == "1.0.1" or "<sha>" or "<date>"
            specific_tag = get_new_tag(
                full_arch_tag, date_tag if "-deps" in image else sha_tag)
            # Tag and push rayproject/<image>:<sha/date><arch_tag>
            DOCKER_CLIENT.api.tag(
                image=f"{full_image}:{full_arch_tag}",
                repository=full_image,
                tag=specific_tag)
            docker_push(full_image, specific_tag)


# Push infra here:
# https://github.com/christian-korneck/docker-pushrm/blob/master/README-containers.md#push-a-readme-file-to-dockerhub # noqa
def push_readmes():
    if not _merge_build():
        print("Not pushing README because this is a PR build.")
        return
    username, password = _get_docker_creds()
    for image, tag_line in DOCKER_HUB_DESCRIPTION.items():
        environment = {
            "DOCKER_USER": username,
            "DOCKER_PASS": password,
            "PUSHRM_FILE": f"/myvol/docker/{image}/README.md",
            "PUSHRM_DEBUG": 1,
            "PUSHRM_SHORT": tag_line
        }
        cmd_string = (f"rayproject/{image}")

        print(
            DOCKER_CLIENT.containers.run(
                "chko/docker-pushrm:1",
                command=cmd_string,
                volumes={
                    os.path.abspath(_get_root_dir()): {
                        "bind": "/myvol",
                        "mode": "rw",
                    }
                },
                environment=environment,
                remove=True,
                detach=False,
                stderr=True,
                stdout=True,
                tty=False))


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
            # TODO(ilr) Re-Enable Push READMEs by using a normal password
            # (not auth token :/)
            # push_readmes()
