import os
import subprocess
import sys
import tempfile
from typing import List, Set

import runfiles
import pytest
import yaml

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


class FileToTags:
    file: str
    tags: Set[str]

    def __init__(self, file: str, tags: Set[str]):
        self.file = file
        self.tags = tags


_TESTS_YAML = """
ci/pipeline/test_conditional_testing.py: lint tools
python/ray/data/__init__.py: lint data linux_wheels macos_wheels ml train
doc/index.md: lint

python/ray/air/__init__.py: lint ml train tune data linux_wheels macos_wheels
python/ray/llm/llm.py: lint llm
python/ray/workflow/workflow.py: lint workflow linux_wheels macos_wheels
python/ray/tune/tune.py: lint ml train tune linux_wheels macos_wheels
python/ray/train/train.py: lint ml train linux_wheels macos_wheels
.buildkite/ml.rayci.yml: lint ml train tune
rllib/rllib.py: lint rllib rllib_gpu rllib_directly linux_wheels macos_wheels

python/ray/serve/serve.py: lint serve linux_wheels macos_wheels java
python/ray/dashboard/dashboard.py: lint dashboard linux_wheels macos_wheels
python/core.py:
    - lint ml tune train serve workflow data
    - python dashboard linux_wheels macos_wheels java
python/setup.py:
    - lint ml tune train serve workflow data
    - python dashboard linux_wheels macos_wheels java python_dependencies
python/requirements/test-requirements.txt:
    - lint ml tune train serve workflow data
    - python dashboard linux_wheels macos_wheels java python_dependencies
python/_raylet.pyx:
    - lint ml tune train serve workflow data
    - python dashboard linux_wheels macos_wheels java compiled_python
python/ray/dag/dag.py:
    - lint ml tune train serve workflow data
    - python dashboard linux_wheels macos_wheels java accelerated_dag

.buildkite/core.rayci.yml: lint python core_cpp
.buildkite/serverless.rayci.yml: lint python
java/ray.java: lint java
.buildkite/others.rayci.yml: lint java
cpp/ray.cc: lint cpp
docker/Dockerfile.ray: lint docker linux_wheels

.readthedocs.yaml: lint doc
doc/code.py: lint doc
doc/example.ipynb: lint doc
doc/tutorial.rst: lint doc
ci/docker/doctest.build.Dockerfile: lint
release/requirements_buildkite.txt: lint release_tests
ci/lint/lint.sh: lint tools
.buildkite/lint.rayci.yml: lint tools
.buildkite/macos.rayci.yml: lint macos_wheels
ci/ray_ci/tester.py: lint tools
.buildkite/base.rayci.yml: lint docker linux_wheels tools
ci/ci.sh: lint tools

src/ray.cpp:
    - lint ml tune train serve core_cpp cpp java python
    - linux_wheels macos_wheels dashboard release_tests accelerated_dag

.github/CODEOWNERS: lint
BUILD.bazel:
    - lint ml tune train data serve core_cpp cpp java
    - python doc linux_wheels macos_wheels dashboard tools
    - release_tests compiled_python
"""


def test_conditional_testing_pull_request():
    script = _runfiles.Rlocation(_REPO_NAME + "/ci/pipeline/determine_tests_to_run.py")

    test_cases: List[FileToTags] = []
    test_cases_yaml = yaml.safe_load(_TESTS_YAML)
    for file, value in test_cases_yaml.items():
        tags: Set[str] = set()
        if isinstance(value, list):
            for line in value:
                tags.update(line.split())
        else:
            tags.update(value.split())
        test_cases.append(FileToTags(file=file, tags=set(tags)))

    with tempfile.TemporaryDirectory() as origin, tempfile.TemporaryDirectory() as workdir:
        subprocess.check_call(["git", "init", "--bare"], cwd=origin)
        subprocess.check_call(["git", "init"], cwd=workdir)
        subprocess.check_call(
            ["git", "config", "user.email", "rayci@ray.io"], cwd=workdir
        )
        subprocess.check_call(
            ["git", "config", "user.name", "Ray CI Test"], cwd=workdir
        )
        subprocess.check_call(["git", "remote", "add", "origin", origin], cwd=workdir)

        with open(os.path.join(workdir, "README.md"), "w") as f:
            f.write("# README\n")
        subprocess.check_call(["git", "add", "README.md"], cwd=workdir)
        subprocess.check_call(["git", "commit", "-m", "init with readme"], cwd=workdir)
        subprocess.check_call(["git", "push", "origin", "master"], cwd=workdir)

        for test_case in test_cases:
            subprocess.check_call(
                ["git", "checkout", "-B", "pr01", "master"], cwd=workdir
            )

            add_files = [test_case.file]
            for f in add_files:
                dirname = os.path.dirname(f)
                if dirname:
                    os.makedirs(os.path.join(workdir, dirname), exist_ok=True)
                with open(os.path.join(workdir, f), "w") as f:
                    f.write("...\n")

            subprocess.check_call(["git", "add", "."], cwd=workdir)
            subprocess.check_call(
                ["git", "commit", "-m", "add test files"], cwd=workdir
            )
            commit = (
                subprocess.check_output(
                    ["git", "show", "HEAD", "-q", "--format=%H"], cwd=workdir
                )
                .decode()
                .strip()
            )

            envs = os.environ.copy()
            envs["BUILDKITE"] = "true"
            envs["BUILDKITE_PULL_REQUEST_BASE_BRANCH"] = "master"
            envs["BUILDKITE_PULL_REQUEST"] = "true"
            envs["BUILDKITE_COMMIT"] = commit

            output = (
                subprocess.check_output([sys.executable, script], env=envs, cwd=workdir)
                .decode()
                .strip()
            )
            tags = output.split()

            want = test_case.tags
            assert want == set(tags), f"file {test_case.file}, want {want}, got {tags}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
