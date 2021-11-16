import argparse
import datetime
import json
import functools
import glob
import itertools
import os
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from typing import List, Tuple

import docker

print = functools.partial(print, file=sys.stderr, flush=True)
DOCKER_USERNAME = "raytravisbot"
DOCKER_CLIENT = None
PYTHON_WHL_VERSION = "cp3"

DOCKER_HUB_DESCRIPTION = {
    "base-deps": ("Internal Image, refer to "
                  "https://hub.docker.com/r/rayproject/ray"),
    "ray-deps": ("Internal Image, refer to "
                 "https://hub.docker.com/r/rayproject/ray"),
    "ray": "Official Docker Images for Ray, the distributed computing API.",
    "ray-ml": "Developer ready Docker Image for Ray.",
    "ray-worker-container": "Internal Image for CI test",
}

PY_MATRIX = {
    "py36": "3.6.12",
    "py37": "3.7.7",
    "py38": "3.8.5",
    "py39": "3.9.5"
}

BASE_IMAGES = {
    "cu112": "nvidia/cuda:11.2.0-cudnn8-devel-ubuntu18.04",
    "cu111": "nvidia/cuda:11.1.1-cudnn8-devel-ubuntu18.04",
    "cu110": "nvidia/cuda:11.0.3-cudnn8-devel-ubuntu18.04",
    "cu102": "nvidia/cuda:10.2-cudnn8-devel-ubuntu18.04",
    "cu101": "nvidia/cuda:10.1-cudnn8-devel-ubuntu18.04",
    "cpu": "ubuntu:focal",
}

CUDA_FULL = {
    "cu112": "CUDA 11.2",
    "cu111": "CUDA 11.1",
    "cu110": "CUDA 11.0",
    "cu102": "CUDA 10.2",
    "cu101": "CUDA 10.1"
}

# The CUDA version to use for the ML Docker image.
ML_CUDA_VERSION = "cu112"

DEFAULT_PYTHON_VERSION = "py37"

IMAGE_NAMES = list(DOCKER_HUB_DESCRIPTION.keys())


def _get_branch():
    branch = (os.environ.get("TRAVIS_BRANCH")
              or os.environ.get("BUILDKITE_BRANCH"))
    if not branch:
        print("Branch not found!")
        print(os.environ)
        print("Environment is above ^^")
    return branch


def _release_build():
    branch = _get_branch()
    if branch is None:
        return False
    return branch != "master" and branch.startswith("releases")


def _valid_branch():
    branch = _get_branch()
    if branch is None:
        return False
    return branch == "master" or _release_build()


def _get_curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


def _get_root_dir():
    return os.path.join(_get_curr_dir(), "../../")


def _get_commit_sha():
    sha = (os.environ.get("TRAVIS_COMMIT")
           or os.environ.get("BUILDKITE_COMMIT") or "")
    if len(sha) < 6:
        print("INVALID SHA FOUND")
        return "ERROR"
    return sha[:6]


def _configure_human_version():
    global _get_branch
    global _get_commit_sha
    fake_branch_name = input("Provide a 'branch name'. For releases, it "
                             "should be `releases/x.x.x`")
    _get_branch = lambda: fake_branch_name  # noqa: E731
    fake_sha = input("Provide a SHA (used for tag value)")
    _get_commit_sha = lambda: fake_sha  # noqa: E731


def _get_wheel_name(minor_version_number):
    if minor_version_number:
        matches = [
            file for file in glob.glob(
                f"{_get_root_dir()}/.whl/ray-*{PYTHON_WHL_VERSION}"
                f"{minor_version_number}*-manylinux*")
            if "+" not in file  # Exclude dbg, asan  builds
        ]
        assert len(matches) == 1, (
            f"Found ({len(matches)}) matches for 'ray-*{PYTHON_WHL_VERSION}"
            f"{minor_version_number}*-manylinux*' instead of 1.\n"
            f"wheel matches: {matches}")
        return os.path.basename(matches[0])
    else:
        matches = glob.glob(
            f"{_get_root_dir()}/.whl/*{PYTHON_WHL_VERSION}*-manylinux*")
        return [os.path.basename(i) for i in matches]


def _check_if_docker_files_modified():
    stdout = subprocess.check_output([
        sys.executable, f"{_get_curr_dir()}/determine_tests_to_run.py",
        "--output=json"
    ])
    affected_env_var_list = json.loads(stdout)
    affected = ("RAY_CI_DOCKER_AFFECTED" in affected_env_var_list or
                "RAY_CI_PYTHON_DEPENDENCIES_AFFECTED" in affected_env_var_list)
    print(f"Docker affected: {affected}")
    return affected


def _build_docker_image(image_name: str,
                        py_version: str,
                        image_type: str,
                        no_cache=True):
    """Builds Docker image with the provided info.

    image_name (str): The name of the image to build. Must be one of
        IMAGE_NAMES.
    py_version (str): The Python version to build the image for.
        Must be one of PY_MATRIX.keys()
    image_type (str): The image type to build. Must be one of
        BASE_IMAGES.keys()
    no_cache (bool): If True, don't use caching when building the image.
    """

    if image_name not in IMAGE_NAMES:
        raise ValueError(
            f"The provided image name {image_name} is not "
            f"recognized. Image names must be one of {IMAGE_NAMES}")

    if py_version not in PY_MATRIX.keys():
        raise ValueError(f"The provided python version {py_version} is not "
                         f"recognized. Python version must be one of"
                         f" {PY_MATRIX.keys()}")

    if image_type not in BASE_IMAGES.keys():
        raise ValueError(f"The provided CUDA version {image_type} is not "
                         f"recognized. CUDA version must be one of"
                         f" {image_type.keys()}")

    # TODO(https://github.com/ray-project/ray/issues/16599):
    # remove below after supporting ray-ml images with Python 3.9
    if image_name == "ray-ml" and py_version == "py39":
        print(f"{image_name} image is currently unsupported with "
              "Python 3.9")
        return

    build_args = {}
    build_args["PYTHON_VERSION"] = PY_MATRIX[py_version]
    # I.e. "py36"[-1] == 6
    build_args["PYTHON_MINOR_VERSION"] = py_version[-1]

    device_tag = f"{image_type}"

    if image_name == "base-deps":
        base_image = BASE_IMAGES[image_type]
    else:
        base_image = f"-{py_version}-{device_tag}"

    if image_name != "ray-worker-container":
        build_args["BASE_IMAGE"] = base_image

    if image_name in ["ray", "ray-deps", "ray-worker-container"]:
        wheel = _get_wheel_name(build_args["PYTHON_MINOR_VERSION"])
        build_args["WHEEL_PATH"] = f".whl/{wheel}"
        # Add pip option "--find-links .whl/" to ensure ray-cpp wheel
        # can be found.
        build_args["FIND_LINKS_PATH"] = ".whl"

    tagged_name = f"rayproject/{image_name}:nightly-{py_version}-{device_tag}"

    for i in range(2):
        cleanup = DOCKER_CLIENT.containers.prune().get("SpaceReclaimed")
        if cleanup is not None:
            print(f"Cleaned up {cleanup / (2 ** 20)}MB")

        labels = {
            "image-name": image_name,
            "python-version": PY_MATRIX[py_version]
        }
        if image_type in CUDA_FULL:
            labels["cuda-version"] = CUDA_FULL[image_type]

        output = DOCKER_CLIENT.api.build(
            path=os.path.join(_get_root_dir(), "docker", image_name),
            tag=tagged_name,
            nocache=no_cache,
            labels=labels,
            buildargs=build_args)

        cmd_output = []
        try:
            start = datetime.datetime.now()
            current_iter = start
            for line in output:
                cmd_output.append(line.decode("utf-8"))
                if datetime.datetime.now(
                ) - current_iter >= datetime.timedelta(minutes=5):
                    current_iter = datetime.datetime.now()
                    elapsed = datetime.datetime.now() - start
                    print(f"Still building {tagged_name} after "
                          f"{elapsed.seconds} seconds")
                    if elapsed >= datetime.timedelta(minutes=15):
                        print("Additional build output:")
                        print(*cmd_output, sep="\n")
                        # Clear cmd_output after printing, so the next
                        # iteration will not print out the same lines.
                        cmd_output = []
        except Exception as e:
            print(f"FAILURE with error {e}")

        if len(DOCKER_CLIENT.api.images(tagged_name)) == 0:
            print(f"ERROR building: {tagged_name}. Output below:")
            print(*cmd_output, sep="\n")
            if i == 1:
                raise Exception("FAILED TO BUILD IMAGE")
            print("TRYING AGAIN")
        else:
            break

    print("BUILT: ", tagged_name)


def copy_wheels(human_build):
    if human_build:
        print("Please download images using:\n"
              "`pip download --python-version <py_version> ray==<ray_version>")
    root_dir = _get_root_dir()
    wheels = _get_wheel_name(None)
    for wheel in wheels:
        source = os.path.join(root_dir, ".whl", wheel)
        ray_dst = os.path.join(root_dir, "docker/ray/.whl/")
        ray_dep_dst = os.path.join(root_dir, "docker/ray-deps/.whl/")
        ray_worker_container_dst = os.path.join(
            root_dir, "docker/ray-worker-container/.whl/")
        os.makedirs(ray_dst, exist_ok=True)
        shutil.copy(source, ray_dst)
        os.makedirs(ray_dep_dst, exist_ok=True)
        shutil.copy(source, ray_dep_dst)
        os.makedirs(ray_worker_container_dst, exist_ok=True)
        shutil.copy(source, ray_worker_container_dst)


def check_staleness(repository, tag):
    DOCKER_CLIENT.api.pull(repository=repository, tag=tag)

    age = DOCKER_CLIENT.api.inspect_image(f"{repository}:{tag}")["Created"]
    short_date = datetime.datetime.strptime(age.split("T")[0], "%Y-%m-%d")
    is_stale = (
        datetime.datetime.now() - short_date) > datetime.timedelta(days=14)
    return is_stale


def build_for_all_versions(image_name, py_versions, image_types, **kwargs):
    """Builds the given Docker image for all Python & CUDA versions"""
    for py_version in py_versions:
        for image_type in image_types:
            _build_docker_image(
                image_name,
                py_version=py_version,
                image_type=image_type,
                **kwargs)


def build_base_images(py_versions, image_types):
    build_for_all_versions(
        "base-deps", py_versions, image_types, no_cache=False)
    build_for_all_versions(
        "ray-deps", py_versions, image_types, no_cache=False)


def build_or_pull_base_images(py_versions: List[str],
                              image_types: List[str],
                              rebuild_base_images: bool = True) -> bool:
    """Returns images to tag and build."""
    repositories = ["rayproject/base-deps", "rayproject/ray-deps"]
    tags = [
        f"nightly-{py_version}-{image_type}"
        for py_version, image_type in itertools.product(
            py_versions, image_types)
    ]

    try:
        is_stale = check_staleness(repositories[0], tags[0])

        # We still pull even if we have to rebuild the base images to help with
        # caching.
        for repository in repositories:
            for tag in tags:
                DOCKER_CLIENT.api.pull(repository=repository, tag=tag)
    except Exception as e:
        print(e)
        is_stale = True

    if rebuild_base_images or _release_build() or is_stale:
        build_base_images(py_versions, image_types)
        return True
    else:
        print("Just pulling images!")
        return False


def prep_ray_ml():
    root_dir = _get_root_dir()
    requirement_files = glob.glob(
        f"{_get_root_dir()}/python/**/requirements*.txt", recursive=True)
    for fl in requirement_files:
        shutil.copy(fl, os.path.join(root_dir, "docker/ray-ml/"))
    # Install atari roms script
    shutil.copy(f"{_get_root_dir()}/rllib/utils/install_atari_roms.sh",
                os.path.join(root_dir, "docker/ray-ml/"))


def _get_docker_creds() -> Tuple[str, str]:
    docker_password = os.environ.get("DOCKER_PASSWORD")
    assert docker_password, "DOCKER_PASSWORD not set."
    return DOCKER_USERNAME, docker_password


def _docker_push(image, tag):
    print(f"PUSHING: {image}:{tag}, result:")
    # This docker API is janky. Without "stream=True" it returns a
    # massive string filled with every progress bar update, which can
    # cause CI to back up.
    #
    # With stream=True, it's a line-at-a-time generator of the same
    # info. So we can slow it down by printing every couple hundred
    # lines
    i = 0
    for progress_line in DOCKER_CLIENT.api.push(image, tag=tag, stream=True):
        if i % 100 == 0:
            print(progress_line)


def _tag_and_push(full_image_name, old_tag, new_tag, merge_build=False):
    # Do not tag release builds because they are no longer up to
    # date after the branch cut.
    if "nightly" in new_tag and _release_build():
        return
    if old_tag != new_tag:
        DOCKER_CLIENT.api.tag(
            image=f"{full_image_name}:{old_tag}",
            repository=full_image_name,
            tag=new_tag)
    if not merge_build:
        print("This is a PR Build! On a merge build, we would normally push"
              f"to: {full_image_name}:{new_tag}")
    else:
        _docker_push(full_image_name, new_tag)


def _create_new_tags(all_tags, old_str, new_str):
    new_tags = []
    for full_tag in all_tags:
        new_tag = full_tag.replace(old_str, new_str)
        new_tags.append(new_tag)
    return new_tags


# For non-release builds, push "nightly" & "sha"
# For release builds, push "nightly" & "latest" & "x.x.x"
def push_and_tag_images(py_versions: List[str],
                        image_types: List[str],
                        push_base_images: bool,
                        merge_build: bool = False):

    date_tag = datetime.datetime.now().strftime("%Y-%m-%d")
    sha_tag = _get_commit_sha()
    if _release_build():
        release_name = re.search("[0-9]\.[0-9]\.[0-9].*",
                                 _get_branch()).group(0)
        date_tag = release_name
        sha_tag = release_name

    image_list = ["ray", "ray-ml"]
    if push_base_images:
        image_list.extend(["base-deps", "ray-deps"])

    for image_name in image_list:
        full_image_name = f"rayproject/{image_name}"

        # Mapping from old tags to new tags.
        # These are the tags we will push.
        # The key is the full image name, and the values are all the tags
        # for that image.
        tag_mapping = defaultdict(list)
        for py_name in py_versions:
            for image_type in image_types:
                if image_name == "ray-ml" and image_type != ML_CUDA_VERSION:
                    print("ML Docker image is not built for the following "
                          f"device type: {image_type}")
                    continue

                # TODO(https://github.com/ray-project/ray/issues/16599):
                # remove below after supporting ray-ml images with Python 3.9
                if image_name in ["ray-ml"
                                  ] and PY_MATRIX[py_name].startswith("3.9"):
                    print(f"{image_name} image is currently "
                          f"unsupported with Python 3.9")
                    continue

                tag = f"nightly-{py_name}-{image_type}"
                tag_mapping[tag].append(tag)

        # If no device is specified, it should map to CPU image.
        # For ray-ml image, if no device specified, it should map to GPU image.
        # There is no CPU image for ray-ml.
        # "-gpu" tag should refer to the ML_CUDA_VERSION
        for old_tag in tag_mapping.keys():
            if "cpu" in old_tag:
                new_tags = _create_new_tags(
                    tag_mapping[old_tag], old_str="-cpu", new_str="")
                tag_mapping[old_tag].extend(new_tags)
            elif ML_CUDA_VERSION in old_tag:
                new_tags = _create_new_tags(
                    tag_mapping[old_tag],
                    old_str=f"-{ML_CUDA_VERSION}",
                    new_str="-gpu")
                tag_mapping[old_tag].extend(new_tags)

                if image_name == "ray-ml":
                    new_tags = _create_new_tags(
                        tag_mapping[old_tag],
                        old_str=f"-{ML_CUDA_VERSION}",
                        new_str="")
                    tag_mapping[old_tag].extend(new_tags)

        # No Python version specified should refer to DEFAULT_PYTHON_VERSION
        for old_tag in tag_mapping.keys():
            if DEFAULT_PYTHON_VERSION in old_tag:
                new_tags = _create_new_tags(
                    tag_mapping[old_tag],
                    old_str=f"-{DEFAULT_PYTHON_VERSION}",
                    new_str="")
                tag_mapping[old_tag].extend(new_tags)

        # For all tags, create Date/Sha tags
        for old_tag in tag_mapping.keys():
            new_tags = _create_new_tags(
                tag_mapping[old_tag],
                old_str="nightly",
                new_str=date_tag if "-deps" in image_name else sha_tag)
            tag_mapping[old_tag].extend(new_tags)

        # Sanity checking.
        for old_tag in tag_mapping.keys():
            if DEFAULT_PYTHON_VERSION in old_tag:
                if "-cpu" in old_tag:
                    assert "nightly-cpu" in tag_mapping[old_tag]
                    assert "nightly" in tag_mapping[old_tag]
                    if "-deps" in image_name:
                        assert f"{date_tag}-cpu" in tag_mapping[old_tag]
                        assert f"{date_tag}" in tag_mapping[old_tag]
                    else:
                        assert f"{sha_tag}-cpu" in tag_mapping[old_tag]
                        assert f"{sha_tag}" in tag_mapping[old_tag]

                elif ML_CUDA_VERSION in old_tag:
                    assert "nightly-gpu" in tag_mapping[old_tag]
                    if "-deps" in image_name:
                        assert f"{date_tag}-gpu" in tag_mapping[old_tag]
                    else:
                        assert f"{sha_tag}-gpu" in tag_mapping[old_tag]

                    if image_name == "ray-ml":
                        assert "nightly" in tag_mapping[old_tag]
                        assert f"{sha_tag}" in tag_mapping[old_tag]

        print(f"These tags will be created for {image_name}: ", tag_mapping)

        # Tag and push all images.
        for old_tag in tag_mapping.keys():
            for new_tag in tag_mapping[old_tag]:
                _tag_and_push(
                    full_image_name,
                    old_tag=old_tag,
                    new_tag=new_tag,
                    merge_build=merge_build)


# Push infra here:
# https://github.com/christian-korneck/docker-pushrm/blob/master/README-containers.md#push-a-readme-file-to-dockerhub # noqa
def push_readmes(merge_build: bool):
    if not merge_build:
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
# Build ray, ray-ml every time
# build-docker-images.py --py-versions PY37 --build-type PR --rebuild-all
MERGE = "MERGE"
HUMAN = "HUMAN"
PR = "PR"
BUILDKITE = "BUILDKITE"
BUILD_TYPES = [MERGE, HUMAN, PR, BUILDKITE]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--py-versions",
        choices=list(PY_MATRIX.keys()),
        default="py37",
        nargs="*",
        help="Which python versions to build. "
        "Must be in (py36, py37, py38, py39)")
    parser.add_argument(
        "--device-types",
        choices=list(BASE_IMAGES.keys()),
        default=None,
        nargs="*",
        help="Which device types (CPU/CUDA versions) to build images for. "
        "If not specified, images will be built for all device types.")
    parser.add_argument(
        "--build-type",
        choices=BUILD_TYPES,
        required=True,
        help="Whether to bypass checking if docker is affected")
    parser.add_argument(
        "--build-base",
        dest="base",
        action="store_true",
        help="Whether to build base-deps & ray-deps")
    parser.add_argument("--no-build-base", dest="base", action="store_false")
    parser.set_defaults(base=True)
    parser.add_argument(
        "--only-build-worker-container",
        dest="only_build_worker_container",
        action="store_true",
        help="Whether only to build ray-worker-container")
    parser.set_defaults(only_build_worker_container=False)

    args = parser.parse_args()
    py_versions = args.py_versions
    py_versions = py_versions if isinstance(py_versions,
                                            list) else [py_versions]

    image_types = args.device_types if args.device_types else list(
        BASE_IMAGES.keys())

    assert set(list(CUDA_FULL.keys()) + ["cpu"]) == set(BASE_IMAGES.keys())

    # Make sure the python images and cuda versions we build here are
    # consistent with the ones used with fix-latest-docker.sh script.
    py_version_file = os.path.join(_get_root_dir(), "docker/retag-lambda",
                                   "python_versions.txt")
    with open(py_version_file) as f:
        py_file_versions = f.read().splitlines()
        assert set(PY_MATRIX.keys()) == set(py_file_versions), \
            (PY_MATRIX.keys(), py_file_versions)

    cuda_version_file = os.path.join(_get_root_dir(), "docker/retag-lambda",
                                     "cuda_versions.txt")

    with open(cuda_version_file) as f:
        cuda_file_versions = f.read().splitlines()
        assert set(BASE_IMAGES.keys()) == set(cuda_file_versions + ["cpu"]),\
            (BASE_IMAGES.keys(), cuda_file_versions + ["cpu"])

    print("Building the following python versions: ",
          [PY_MATRIX[py_version] for py_version in py_versions])
    print("Building images for the following devices: ", image_types)
    print("Building base images: ", args.base)

    build_type = args.build_type
    is_buildkite = build_type == BUILDKITE

    if build_type == BUILDKITE:
        if os.environ.get("BUILDKITE_PULL_REQUEST", "") == "false":
            build_type = MERGE
        else:
            build_type = PR

    if build_type == HUMAN:
        # If manually triggered, request user for branch and SHA value to use.
        _configure_human_version()
    if (build_type in {HUMAN, MERGE, BUILDKITE}
            or _check_if_docker_files_modified()
            or args.only_build_worker_container):
        DOCKER_CLIENT = docker.from_env()
        is_merge = build_type == MERGE
        # Buildkite is authenticated in the background.
        if is_merge and not is_buildkite:
            # We do this here because we want to be authenticated for
            # Docker pulls as well as pushes (to avoid rate-limits).
            username, password = _get_docker_creds()
            DOCKER_CLIENT.api.login(username=username, password=password)
        copy_wheels(build_type == HUMAN)
        is_base_images_built = build_or_pull_base_images(
            py_versions, image_types, args.base)

        if args.only_build_worker_container:
            build_for_all_versions("ray-worker-container", py_versions,
                                   image_types)
            # TODO Currently don't push ray_worker_container
        else:
            # Build Ray Docker images.
            build_for_all_versions("ray", py_versions, image_types)

            if ML_CUDA_VERSION in image_types:
                # Build Ray ML Docker images only if ML_CUDA_VERSION is
                # specified.
                prep_ray_ml()
                # Only build ML Docker for the ML_CUDA_VERSION
                build_for_all_versions(
                    "ray-ml", py_versions, image_types=[ML_CUDA_VERSION])

            if build_type in {MERGE, PR}:
                valid_branch = _valid_branch()
                if (not valid_branch) and is_merge:
                    print(f"Invalid Branch found: {_get_branch()}")
                push_and_tag_images(py_versions, image_types,
                                    is_base_images_built, valid_branch
                                    and is_merge)

        # TODO(ilr) Re-Enable Push READMEs by using a normal password
        # (not auth token :/)
        # push_readmes(build_type is MERGE)
