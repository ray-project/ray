"""Runner for the ray-stop Windows smoke test.

Invoked from the CI step (inside job_env: WINDOWS). Spawns a Docker container
using the windowsbuild image with the built wheel artifact mounted, then runs
smoke_test_stop.py inside that container.

Usage:
    python ci/ray_ci/windows/run_smoke_test_stop.py <python-version>

    e.g.  python ci/ray_ci/windows/run_smoke_test_stop.py 3.11
"""
import os
import subprocess
import sys


def _ecr_login(ecr_registry: str) -> None:
    """Login to ECR using the AWS CLI (avoids boto3 / ci.ray_ci imports)."""
    password = subprocess.check_output(
        ["aws", "ecr", "get-login-password", "--region", "us-west-2"],
        text=True,
    ).strip()
    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", ecr_registry],
        input=password,
        text=True,
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def main(python_version: str) -> int:
    py_tag = "cp" + python_version.replace(".", "")

    rayci_work_repo = os.environ["RAYCI_WORK_REPO"]
    windowsbuild_image = (
        f"{rayci_work_repo}:{os.environ['RAYCI_BUILD_ID']}-windowsbuild"
    )

    # Required before docker pull/run against ECR.
    # Uses AWS CLI (installed by install_tools.sh) rather than boto3 so this
    # script has no dependencies outside the standard library.
    ecr_registry = rayci_work_repo.split("/")[0]
    _ecr_login(ecr_registry)
    checkout_dir = os.path.abspath(os.environ["RAYCI_CHECKOUT_DIR"])

    # The artifact mount mirrors the convention in WindowsContainer.get_artifact_mount():
    #   host  C:\tmp\artifacts  <->  container  C:\artifact-mount
    artifact_host = "C:\\tmp\\artifacts"
    artifact_container = "C:\\artifact-mount"

    # Inside the container, the wheel is at C:\artifact-mount\python\dist\
    # (POSIX path for bash: /c/artifact-mount/python/dist/)
    inner_script = "\n".join(
        [
            "set -euo pipefail",
            f"WHEEL=$(ls /c/artifact-mount/python/dist/ray-*-{py_tag}-*-win_amd64.whl 2>/dev/null | head -1)",
            'if [[ -z "$WHEEL" ]]; then',
            '  echo "ERROR: no wheel found for ' + py_tag + '" >&2',
            "  ls /c/artifact-mount/python/dist/ >&2 || true",
            "  exit 1",
            "fi",
            'echo "Installing wheel: $WHEEL"',
            'pip install "$WHEEL"',
            "python ci/ray_ci/windows/smoke_test_stop.py",
        ]
    )

    cmd = [
        "docker",
        "run",
        "--rm",
        "--volume",
        f"{artifact_host}:{artifact_container}",
        "--volume",
        f"{checkout_dir}:C:\\rayci",
        "--workdir",
        "C:\\rayci",
        windowsbuild_image,
        "bash",
        "-c",
        inner_script,
    ]

    result = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
    return result.returncode


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <python-version>", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
