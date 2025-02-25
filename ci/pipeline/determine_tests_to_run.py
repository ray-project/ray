#!/usr/bin/env python3

import argparse
import fnmatch
import os
import subprocess
import sys
from typing import List, Optional, Set, Tuple
from pprint import pformat


_ALL_TAGS = set(
    """
    lint python cpp core_cpp java workflow accelerated_dag dashboard
    data serve ml tune train llm rllib rllib_gpu rllib_directly
    linux_wheels macos_wheels docker doc python_dependencies tools
    release_tests compiled_python
    """.split()
)

_TAG_RULE = """
python/ray/air/
@ ml train tune data linux_wheels macos_wheels
;

python/ray/llm/
.buildkite/llm.rayci.yml
ci/docker/llm.build.Dockerfile
@ llm
;

python/ray/data/
.buildkite/data.rayci.yml
ci/docker/data.build.Dockerfile
ci/docker/data.build.wanda.yaml
ci/docker/datan.build.wanda.yaml
ci/docker/data9.build.wanda.yaml
ci/docker/datal.build.wanda.yaml
@ data ml train linux_wheels macos_wheels
;

python/ray/workflow/
@ workflow linux_wheels macos_wheels
;

python/ray/tune/
@ ml train tune linux_wheels macos_wheels
;

python/ray/train/
@ ml train linux_wheels macos_wheels
;

.buildkite/ml.rayci.yml
.buildkite/pipeline.test.yml
ci/docker/ml.build.Dockerfile
.buildkite/pipeline.gpu.yml
.buildkite/pipeline.gpu_large.yml
ci/docker/ml.build.wanda.yaml
ci/ray_ci/ml.tests.yml
ci/docker/min.build.Dockerfile
ci/docker/min.build.wanda.yaml
@ ml train tune
;

rllib/
python/ray/rllib/
ray_ci/rllib.tests.yml
.buildkite/rllib.rayci.yml
@ rllib rllib_gpu rllib_directly linux_wheels macos_wheels
;

python/ray/serve/
.buildkite/serve.rayci.yml
ci/docker/serve.build.Dockerfile
@ serve linux_wheels macos_wheels java
;

python/ray/dashboard/
@ dashboard linux_wheels macos_wheels
;

python/setup.py
python/requirements.txt
python/requirements_compiled.txt
python/requirements/
@ ml tune train serve workflow data
@ python dashboard linux_wheels macos_wheels java
@ python_dependencies
;

python/ray/dag/
python/ray/experimental/channel/
@ ml tune train serve workflow data
@ python dashboard linux_wheels macos_wheels java
@ accelerated_dag
;

python/
@ ml tune train serve workflow data
# Python changes might impact cross language stack in Java.
# Java also depends on Python CLI to manage processes.
@ python dashboard linux_wheels macos_wheels java
;

.buildkite/core.rayci.yml
@ python core_cpp
;

.buildkite/serverless.rayci.yml
@ python
;

java/
.buildkite/others.rayci.yml
@ java
;

cpp/
.buildkite/pipeline.build_cpp.yml
@ cpp
;

docker/
.buildkite/pipeline.build_cpp.yml
@ docker linux_wheels
;

.readthedocs.yaml
@ doc
;

doc/*.py
doc/*.ipynb
doc/BUILD
doc/*/BUILD
doc/*.rst
@ doc
;

ci/docker/doctest.build.Dockerfile
ci/docker/doctest.build.wanda.yaml
# pass
;

release/ray_release/
# Release test unit tests are ALWAYS RUN, so pass
;

release/*.md
release/*.yaml
# Do not run on config changes
;

release/
@ release_tests
;

doc/
examples/
dev/
kubernetes/
site/
# pass
;

ci/lint/
.buildkite/lint.rayci.yml
@ tools
;

.buildkite/macos.rayci.yml
.buildkite/pipeline.macos.yml
ci/ray_ci/macos/macos_ci.sh
ci/ray_ci/macos/macos_ci_build.sh
@ macos_wheels
;

ci/pipeline/
ci/build/
ci/ray_ci/
.buildkite/_forge.rayci.yml
.buildkite/_forge.aarch64.rayci.yml
ci/docker/forge.wanda.yaml
ci/docker/forge.aarch64.wanda.yaml
.buildkite/pipeline.build.yml
.buildkite/hooks/post-command
@ tools
;

.buildkite/base.rayci.yml
.buildkite/build.rayci.yml
.buildkite/pipeline.arm64.yml
ci/docker/manylinux.Dockerfile
ci/docker/manylinux.wanda.yaml
ci/docker/manylinux.aarch64.wanda.yaml
ci/docker/ray.cpu.base.wanda.yaml
ci/docker/ray.cpu.base.aarch64.wanda.yaml
ci/docker/ray.cuda.base.wanda.yaml
ci/docker/ray.cuda.base.aarch64.wanda.yaml
ci/docker/windows.build.Dockerfile
ci/docker/windows.build.wanda.yaml
@ docker linux_wheels tools
;

ci/run/
ci/ci.sh
@ tools
;

src/
@ ml tune train serve core_cpp cpp
@ java python linux_wheels macos_wheels
@ dashboard release_tests accelerated_dag
;

.github/
# pass
;

"""


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


class TagRule:
    def __init__(
        self,
        tags: List[str],
        dirs: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        patterns: Optional[List[str]] = None,
    ):
        self.tags = set(tags)
        self.dirs = dirs or []
        self.patterns = patterns or []
        self.files = files or []

    def match(self, changed_file: str) -> bool:
        for dir_name in self.dirs:
            if changed_file == dir_name or changed_file.startswith(dir_name + "/"):
                return True
        for file in self.files:
            if changed_file == file:
                return True
        for pattern in self.patterns:
            if fnmatch.fnmatch(changed_file, pattern):
                return True
        return False

    def match_tags(self, changed_file: str) -> Tuple[Set[str], bool]:
        if self.match(changed_file):
            return self.tags, True
        return set(), False


def _parse_rules(rule_content: str) -> List[TagRule]:
    """
    Parse the rule config content into a list ot TagRule's.

    The rule content is a string with the following format:

    ```
    # Comment content, after '#', will be ignored.
    # Empty lines will be ignored too.

    dir/  # Directory to match
    file  # File to match
    dir/*.py  # Pattern to match, using fnmatch, matches dir/a.py dir/dir/b.py or dir/.py
    @ tag1 tag2 tag3 # Tags to emit for a rule. A rule without tags is a skipping rule.

    ;  # Semicolon to separate rules
    ```

    Rules are evaluated in order, and the first matched rule will be used.
    """
    rules: List[TagRule] = []

    tags: Set[str] = set()
    dirs: List[str] = []
    files: List[str] = []
    patterns: List[str] = []

    lines = rule_content.splitlines()
    lineno = 0
    for line in lines:
        lineno += 1
        line = line.strip()
        if not line or line.startswith("#"):
            continue  # Skip empty lines and comments.

        comment_index = line.find("#")  # Find the first '#' to remove comments.
        if comment_index != -1:
            line = line[:comment_index].strip()  # Remove comments.

        if line.startswith("@"):  # tags.
            # Strip the leading '@' and split into tags.
            tags.update(line[1:].split())
        elif line.startswith(";"):  # End of a rule.
            if line != ";":
                raise ValueError(f"Unexpected tokens after semicolon on line {lineno}.")
            rules.append(TagRule(tags, dirs, files, patterns))
            tags, dirs, files, patterns = set(), [], [], []
        else:
            if line.find("*") != -1:  # Patterns.
                patterns.append(line)
            elif line.endswith("/"):  # Directories.
                dirs.append(line[:-1])
            else:  # Files.
                files.append(line)

    # Append the last rule if not empty.
    if tags or dirs or files or patterns:
        rules.append(TagRule(tags, dirs, files, patterns))

    return rules


class TagRuleSet:
    def __init__(self, content: str):
        self.rules = _parse_rules(content)

    def match_tags(self, changed_file: str) -> Tuple[Set[str], bool]:
        for rule in self.rules:
            match_tags, matched = rule.match_tags(changed_file)
            if matched:
                return match_tags, True
        return set(), False


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

        rule_set = TagRuleSet(_TAG_RULE)

        for changed_file in files:
            match_tags, matched = rule_set.match_tags(changed_file)
            if matched:
                tags.update(match_tags)
                continue

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
