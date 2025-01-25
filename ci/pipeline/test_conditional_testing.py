import os
import subprocess
import sys
import tempfile

import runfiles
import pytest

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


def test_conditional_testing_pull_request():
    script = _runfiles.Rlocation(_REPO_NAME + "/ci/pipeline/determine_tests_to_run.py")

    test_cases = [
        (["ci/pipeline/test_conditional_testing.py"], {"lint", "tools"}),
        (
            ["python/ray/data/__init__.py"],
            {"lint", "data", "linux_wheels", "macos_wheels", "ml", "train"},
        ),
    ]

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

            add_files = test_case[0]
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

            want = test_case[1]
            assert want == set(tags), f"want {want}, got {tags}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
