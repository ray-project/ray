import os

import runfiles

REPO_NAME = "io_ray"
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
        # Runfiles paths are always "/"-separated; the repo mapping (io_ray ->
        # _main under bzlmod) only applies when the repo name is split off by "/".
        f = the_runfiles.Rlocation(REPO_NAME + "/" + p.replace(os.sep, "/"))
        if f:
            return f
    return os.path.join(_LEGACY_REPO_ROOT, p)
