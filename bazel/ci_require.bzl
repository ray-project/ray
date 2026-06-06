"""Platform-aware CI dependency labels for the Windows driver closure.

ci_require() returns a plain alias label (//ci/ray_ci/deps:<pkg>) so it can be
used as an ordinary `deps` element with NO load of the pip repositories. That
keeps //release and //ci/ray_ci cheap to parse in minimal build contexts (e.g.
the ray-core / ray-cpp-core / ray-java image builds, which include `bazel/` but
not the Windows lock).

The actual per-platform select() lives in the alias targets, defined by
ci_require_aliases() in //ci/ray_ci/deps:aliases.bzl, which is only evaluated
when a Windows driver target is built.

Adding a dep to the Windows driver closure means: add it to WINDOWS_DRIVER_DEPS
below AND to release/requirements_windows.in (so it lands in @py_deps_windows).
"""

# Every third-party package referenced via ci_require()/bk_require() in the
# Windows driver closure (//ci/ray_ci/BUILD.bazel and //release/BUILD.bazel).
# Each must exist in @py_deps_windows (release/requirements_windows.in).
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
    """Return the per-platform alias label for pip package `name`."""
    return "//ci/ray_ci/deps:" + normalize_dep(name)
