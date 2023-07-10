import os

import runfiles

REPO_NAME = "com_github_ray_project_ray"
_LEGACY_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../.."),
)

the_runfiles = runfiles.Create()


def _norm_path_join(*args):
    return os.path.normpath(os.path.join(*args))


def bazel_runfile(*args):
    """Return the path to a runfile in the release directory."""
    p = _norm_path_join(*args)
    if the_runfiles:
        return the_runfiles.Rlocation(os.path.join(REPO_NAME, p))
    return os.path.join(_LEGACY_REPO_ROOT, p)
