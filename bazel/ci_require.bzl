"""Dependency labels for the CI driver closure (see //ci/ray_ci/deps:aliases.bzl).

TODO(elliot-barn): Remove this Windows-specific dep machinery once the Windows CI
system Python is upgraded to 3.10. At that point the driver deps can be bundled
from the hermetic py set on Windows like every other platform, so this list, the
select() in //ci/ray_ci/deps:aliases.bzl, the //ci/raydepsets ci_windows_depset,
and the agent-side pip install in ci/ray_ci/windows/install_tools.sh all go away.
"""

WINDOWS_DRIVER_DEPS = [
    "aioboto3",
    "anyscale",
    "aws-requests-auth",
    "azure-identity",
    "azure-storage-blob",
    "bazel-runfiles",
    "boto3",
    "botocore",
    "click",
    "freezegun",
    "google-cloud-storage",
    "jinja2",
    "msal",
    "pybuildkite",
    "pytest",
    "pyyaml",
    "requests",
    "responses",
]

def normalize_dep(name):
    return name.lower().replace("-", "_").replace(".", "_")

def ci_require(name):
    return "//ci/ray_ci/deps:" + normalize_dep(name)
