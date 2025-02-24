#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
from typing import List, Set
from pprint import pformat


def _list_changed_files(commit_range):
    """Returns a list of names of files changed in the given commit range.

    The function works by opening a subprocess and running git. If an error
    occurs while running git, the script will abort.

    Args:
        commit_range: The commit range to diff, consisting of the two
            commit IDs separated by \"..\"

    Returns:
        list: List of changed files within the commit range
    """
    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH")
    if base_branch:
        pull_command = ["git", "fetch", "-q", "origin", base_branch]
        subprocess.check_call(pull_command)

    command = ["git", "diff", "--name-only", commit_range, "--"]
    diff_names = subprocess.check_output(command).decode()

    files: List[str] = []
    for line in diff_names.splitlines():
        line = line.strip()
        if line:
            files.append(line)
    return files


def _is_pull_request():
    return os.environ.get("BUILDKITE_PULL_REQUEST", "false") != "false"


def _get_commit_range():
    return "origin/{}...{}".format(
        os.environ["BUILDKITE_PULL_REQUEST_BASE_BRANCH"],
        os.environ["BUILDKITE_COMMIT"],
    )


_ALL_TAGS = set(
    """
    lint python cpp core_cpp java workflow accelerated_dag dashboard
    data serve ml tune train llm rllib rllib_gpu rllib_directly
    linux_wheels macos_wheels docker doc python_dependencies tools
    release_tests compiled_python
    """.split()
)

if __name__ == "__main__":
    assert os.environ.get("BUILDKITE")

    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    tags: Set[str] = set()

    tags.add("lint")

    def _emit(line: str):
        tags.update(line.split())

    if _is_pull_request():
        commit_range = _get_commit_range()
        files = _list_changed_files(commit_range)
        print(pformat(commit_range), file=sys.stderr)
        print(pformat(files), file=sys.stderr)

        skip_prefix_list = [
            "doc/",
            "examples/",
            "dev/",
            "kubernetes/",
            "site/",
        ]

        for changed_file in files:
            if changed_file.startswith("python/ray/air"):
                _emit("ml train tune data linux_wheels macos_wheels")
            elif (
                changed_file.startswith("python/ray/llm")
                or changed_file == ".buildkite/llm.rayci.yml"
                or changed_file == "ci/docker/llm.build.Dockerfile"
            ):
                _emit("llm")
            elif (
                changed_file.startswith("python/ray/data")
                or changed_file == ".buildkite/data.rayci.yml"
                or changed_file == "ci/docker/data.build.Dockerfile"
                or changed_file == "ci/docker/data.build.wanda.yaml"
                or changed_file == "ci/docker/datan.build.wanda.yaml"
                or changed_file == "ci/docker/data9.build.wanda.yaml"
                or changed_file == "ci/docker/datal.build.wanda.yaml"
            ):
                _emit("data ml train linux_wheels macos_wheels")
            elif changed_file.startswith("python/ray/workflow"):
                _emit("workflow linux_wheels macos_wheels")
            elif changed_file.startswith("python/ray/tune"):
                _emit("ml train tune linux_wheels macos_wheels")
            elif changed_file.startswith("python/ray/train"):
                _emit("ml train linux_wheels macos_wheels")
            elif (
                changed_file == ".buildkite/ml.rayci.yml"
                or changed_file == ".buildkite/pipeline.test.yml"
                or changed_file == "ci/docker/ml.build.Dockerfile"
                or changed_file == ".buildkite/pipeline.gpu.yml"
                or changed_file == ".buildkite/pipeline.gpu_large.yml"
                or changed_file == "ci/docker/ml.build.wanda.yaml"
                or changed_file == "ci/ray_ci/ml.tests.yml"
                or changed_file == "ci/docker/min.build.Dockerfile"
                or changed_file == "ci/docker/min.build.wanda.yaml"
            ):
                _emit("ml train tune")
            elif (
                re.match("^(python/ray/)?rllib/", changed_file)
                or changed_file == "ray_ci/rllib.tests.yml"
                or changed_file == ".buildkite/rllib.rayci.yml"
            ):
                _emit("rllib rllib_gpu rllib_directly linux_wheels macos_wheels")
            elif (
                changed_file.startswith("python/ray/serve")
                or changed_file == ".buildkite/serve.rayci.yml"
                or changed_file == "ci/docker/serve.build.Dockerfile"
            ):
                _emit("serve linux_wheels macos_wheels java")
            elif changed_file.startswith("python/ray/dashboard"):
                _emit("dashboard linux_wheels macos_wheels")
            elif changed_file.startswith("python/"):
                _emit("ml tune train serve workflow data")

                # Python changes might impact cross language stack in Java.
                # Java also depends on Python CLI to manage processes.
                _emit("python dashboard linux_wheels macos_wheels java")
                if (
                    changed_file.startswith("python/setup.py")
                    or re.match(r".*requirements.*\.txt", changed_file)
                    or changed_file == "python/requirements_compiled.txt"
                ):
                    _emit("python_dependencies")

                # Some accelerated DAG tests require GPUs so we only run them
                # if Ray DAGs or experimental.channels were affected.
                if changed_file.startswith("python/ray/dag") or changed_file.startswith(
                    "python/ray/experimental/channel"
                ):
                    _emit("accelerated_dag")

            elif changed_file == ".buildkite/core.rayci.yml":
                _emit("python core_cpp")
            elif changed_file == ".buildkite/serverless.rayci.yml":
                _emit("python")
            elif (
                changed_file.startswith("java/")
                or changed_file == ".buildkite/others.rayci.yml"
            ):
                _emit("java")
            elif (
                changed_file.startswith("cpp/")
                or changed_file == ".buildkite/pipeline.build_cpp.yml"
            ):
                _emit("cpp")
            elif (
                changed_file.startswith("docker/")
                or changed_file == ".buildkite/pipeline.build_release.yml"
            ):
                _emit("docker linux_wheels")
            elif changed_file == ".readthedocs.yaml":
                _emit("doc")
            elif changed_file.startswith("doc/"):
                if (
                    changed_file.endswith(".py")
                    or changed_file.endswith(".ipynb")
                    or changed_file.endswith("BUILD")
                    or changed_file.endswith(".rst")
                ):
                    _emit("doc")
                # Else, this affects only a rst file or so. In that case,
                # we pass, as the flag RAY_CI_DOC_AFFECTED is only
                # used to indicate that tests/examples should be run
                # (documentation will be built always)
            elif (
                changed_file == "ci/docker/doctest.build.Dockerfile"
                or changed_file == "ci/docker/doctest.build.wanda.yaml"
            ):
                # common doctest always run without coverage
                pass
            elif changed_file.startswith("release/") or changed_file.startswith(
                ".buildkite/release"
            ):
                if changed_file.startswith("release/ray_release"):
                    # Release test unit tests are ALWAYS RUN, so pass
                    pass
                elif not changed_file.endswith(".yaml") and not changed_file.endswith(
                    ".md"
                ):
                    # Do not run on config changes
                    _emit("release_tests")
            elif any(changed_file.startswith(prefix) for prefix in skip_prefix_list):
                # nothing is run but linting in these cases
                pass
            elif (
                changed_file.startswith("ci/lint")
                or changed_file == ".buildkite/lint.rayci.yml"
            ):
                # Linter will always be run
                _emit("tools")
            elif (
                changed_file == ".buildkite/macos.rayci.yml"
                or changed_file == ".buildkite/pipeline.macos.yml"
                or changed_file == "ci/ray_ci/macos/macos_ci.sh"
                or changed_file == "ci/ray_ci/macos/macos_ci_build.sh"
            ):
                _emit("macos_wheels")
            elif (
                changed_file.startswith("ci/pipeline")
                or changed_file.startswith("ci/build")
                or changed_file.startswith("ci/ray_ci")
                or changed_file == ".buildkite/_forge.rayci.yml"
                or changed_file == ".buildkite/_forge.aarch64.rayci.yml"
                or changed_file == "ci/docker/forge.wanda.yaml"
                or changed_file == "ci/docker/forge.aarch64.wanda.yaml"
                or changed_file == ".buildkite/pipeline.build.yml"
                or changed_file == ".buildkite/hooks/post-command"
            ):
                # These scripts are always run as part of the build process
                _emit("tools")
            elif (
                changed_file == ".buildkite/base.rayci.yml"
                or changed_file == ".buildkite/build.rayci.yml"
                or changed_file == ".buildkite/pipeline.arm64.yml"
                or changed_file == "ci/docker/manylinux.Dockerfile"
                or changed_file == "ci/docker/manylinux.wanda.yaml"
                or changed_file == "ci/docker/manylinux.aarch64.wanda.yaml"
                or changed_file == "ci/docker/ray.cpu.base.wanda.yaml"
                or changed_file == "ci/docker/ray.cpu.base.aarch64.wanda.yaml"
                or changed_file == "ci/docker/ray.cuda.base.wanda.yaml"
                or changed_file == "ci/docker/ray.cuda.base.aarch64.wanda.yaml"
                or changed_file == "ci/docker/windows.build.Dockerfile"
                or changed_file == "ci/docker/windows.build.wanda.yaml"
            ):
                _emit("docker linux_wheels tools")
            elif changed_file.startswith("ci/run") or changed_file == "ci/ci.sh":
                _emit("tools")
            elif changed_file.startswith("src/"):
                _emit("ml tune train serve core_cpp cpp")
                _emit("java python linux_wheels macos_wheels")
                _emit("dashboard release_tests accelerated_dag")
            elif changed_file.startswith(".github/"):
                pass
            else:
                print(
                    "Unhandled source code change: {changed_file}".format(
                        changed_file=changed_file
                    ),
                    file=sys.stderr,
                )

                _emit("ml tune train data serve core_cpp cpp java python doc")
                _emit("linux_wheels macos_wheels dashboard tools release_tests")
    else:
        _emit("ml tune train rllib rllib_directly serve")
        _emit("cpp core_cpp java python doc linux_wheels macos_wheels docker")
        _emit("dashboard tools workflow data release_tests")

    # Log the modified environment variables visible in console.
    output_string = " ".join(list(tags))
    for tag in tags:
        assert tag in _ALL_TAGS, f"Unknown tag {tag}"

    print(output_string, file=sys.stderr)  # Debug purpose
    print(output_string)
