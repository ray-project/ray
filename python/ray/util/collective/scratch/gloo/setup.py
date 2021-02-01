import argparse
import errno
import glob
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
import time

from itertools import chain
from itertools import takewhile

import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

SUPPORTED_PYTHONS = [(3, 6), (3, 7), (3, 8)]
SUPPORTED_BAZEL = (3, 2, 0)

ROOT_DIR = os.path.dirname(__file__)

install_requires = [
    # "aioredis",
    # "libuv",
]


# Calls Bazel in PATH, falling back to the standard user installatation path
# (~/.bazel/bin/bazel) if it isn't found.
def bazel_invoke(invoker, cmdline, *args, **kwargs):
    home = os.path.expanduser("~")
    first_candidate = os.getenv("BAZEL_PATH", "bazel")
    candidates = [first_candidate]
    if sys.platform == "win32":
        mingw_dir = os.getenv("MINGW_DIR")
        if mingw_dir:
            candidates.append(mingw_dir + "/bin/bazel.exe")
    else:
        candidates.append(os.path.join(home, ".bazel", "bin", "bazel"))
    result = None
    for i, cmd in enumerate(candidates):
        try:
            result = invoker([cmd] + cmdline, *args, **kwargs)
            break
        except IOError:
            if i >= len(candidates) - 1:
                raise
    return result


def move_file(target_dir, filename):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    source = filename
    destination = os.path.join(target_dir, filename.split('/')[-1])
    # Create the target directory if it doesn't already exist.
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if not os.path.exists(destination):
        print("Copying {} to {}.".format(source, destination))
        if sys.platform == "win32":
            # Does not preserve file mode (needed to avoid read-only bit)
            shutil.copyfile(source, destination, follow_symlinks=True)
        else:
            # Preserves file mode (needed to copy executable bit)
            # shutil.copy(source, destination)
            shutil.copy(source, destination, follow_symlinks=True)
            # os.system(f"cp {source} {destination}")


def build():
    # no support windows
    if tuple(sys.version_info[:2]) not in SUPPORTED_PYTHONS:
        msg = ("Detected Python version {}, which is not supported. "
               "Only Python {} are supported.").format(
                   ".".join(map(str, sys.version_info[:2])),
                   ", ".join(".".join(map(str, v)) for v in SUPPORTED_PYTHONS))
        raise RuntimeError(msg)

    bazel_env = dict(os.environ, PYTHON3_BIN_PATH=sys.executable)

    version_info = bazel_invoke(subprocess.check_output, ["--version"])
    bazel_version_str = version_info.rstrip().decode("utf-8").split(" ", 1)[1]
    bazel_version_split = bazel_version_str.split(".")
    bazel_version_digits = [
        "".join(takewhile(str.isdigit, s)) for s in bazel_version_split
    ]
    bazel_version = tuple(map(int, bazel_version_digits))
    if bazel_version < SUPPORTED_BAZEL:
        logger.warning("Expected Bazel version {} but found {}".format(
            ".".join(map(str, SUPPORTED_BAZEL)), bazel_version_str))

    bazel_targets = ["//pygloo:all"]
    return bazel_invoke(
        subprocess.check_call,
        ["build", "--verbose_failures", "--"] + bazel_targets,
        env=bazel_env)


def pip_run(build_ext):
    build()

    files_to_include = ["./bazel-bin/pygloo/pygloo.so"]

    for filename in files_to_include:
        move_file(build_ext.build_lib, filename)


if __name__ == "__main__":
    import setuptools
    import setuptools.command.build_ext

    class build_ext(setuptools.command.build_ext.build_ext):
        def run(self):
            return pip_run(self)

    class BinaryDistribution(setuptools.Distribution):
        def has_ext_modules(self):
            return True

    setuptools.setup(
        name="pygloo",
        version="0.0.1",
        # author="",
        # author_email="",
        description=("A python binding for gloo"),
        # long_description=io.open(
        #     os.path.join(ROOT_DIR, os.path.pardir, "README.rst"),
        #     "r",
        #     encoding="utf-8").read(),
        url="",
        keywords=("collective communication"),
        packages=setuptools.find_packages(),
        cmdclass={"build_ext": build_ext},
        # The BinaryDistribution argument triggers build_ext.
        distclass=BinaryDistribution,
        install_requires=install_requires,
        setup_requires=["wheel"],
        include_package_data=True,
        zip_safe=False,
        # license="Apache 2.0"
    )
