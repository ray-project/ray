"""Dependency labels for the CI driver closure (see //ci/ray_ci/deps:aliases.bzl)."""

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
