from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
import glob
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile

from itertools import chain

import setuptools.command.build_ext as _build_ext
from setuptools import setup, find_packages, Distribution

import urllib.parse as urllib_parse
import urllib.request as urllib_request

try:
    import requests
except ImportError:
    requests = None

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.

pyarrow_url = (
    "https://s3-us-west-2.amazonaws.com/"
    "arrow-wheels/3a11193d9530fe8ec7fdb98057f853b708f6f6ae/index.html")
pyarrow_version = "0.14.0.RAY"
pickle5_url = (
    "https://files.pythonhosted.org/packages/"
    "cd/5a/cbdf36134804809d55ffd4c248343bd36680a92b6425885a3fd204d32f7b"
    "/pickle5-0.0.9.tar.gz")

ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server",
    "ray/core/src/ray/gcs/redis_module/libray_redis_module.so",
    "ray/core/src/plasma/plasma_store_server",
    "ray/_raylet.so",
    "ray/core/src/ray/raylet/raylet_monitor",
    "ray/core/src/ray/raylet/raylet",
    "ray/dashboard/dashboard.py",
]

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
]

optional_ray_files = []

ray_autoscaler_files = [
    "ray/autoscaler/aws/example-full.yaml",
    "ray/autoscaler/gcp/example-full.yaml",
    "ray/autoscaler/local/example-full.yaml",
    "ray/autoscaler/kubernetes/example-full.yaml",
    "ray/autoscaler/kubernetes/kubectl-rsync.sh",
]

ray_project_files = [
    "ray/projects/schema.json", "ray/projects/templates/cluster_template.yaml",
    "ray/projects/templates/project_template.yaml",
    "ray/projects/templates/requirements.txt"
]

ray_dashboard_files = [
    "ray/dashboard/client/build/favicon.ico",
    "ray/dashboard/client/build/index.html",
]
for dirname in ["css", "js", "media"]:
    ray_dashboard_files += glob.glob(
        "ray/dashboard/client/build/static/{}/*".format(dirname))

optional_ray_files += ray_autoscaler_files
optional_ray_files += ray_project_files
optional_ray_files += ray_dashboard_files

if "RAY_USE_NEW_GCS" in os.environ and os.environ["RAY_USE_NEW_GCS"] == "on":
    ray_files += [
        "ray/core/src/credis/build/src/libmember.so",
        "ray/core/src/credis/build/src/libmaster.so",
        "ray/core/src/credis/redis/src/redis-server"
    ]

extras = {
    "rllib": [
        "pyyaml", "gym[atari]", "opencv-python-headless", "lz4", "scipy",
        "tabulate"
    ],
    "debug": ["psutil", "setproctitle", "py-spy >= 0.2.0"],
    "dashboard": ["aiohttp", "google", "grpcio", "psutil", "setproctitle"],
    "serve": ["uvicorn", "pygments", "werkzeug", "flask", "pandas"],
    "tune": ["tabulate"],
}

extras["all"] = list(set(chain.from_iterable(extras.values())))


def is_native_windows_or_msys():
    """Check to see if we are running on native Windows,
    but NOT WSL (which is seen as Linux)."""
    return sys.platform == "msys" or sys.platform == "win32"


def is_invalid_windows_platform():
    # 'GCC' check is how you detect MinGW:
    # https://github.com/msys2/MINGW-packages/blob/abd06ca92d876b9db05dd65f27d71c4ebe2673a9/mingw-w64-python2/0410-MINGW-build-extensions-with-GCC.patch#L53
    platform = sys.platform
    ver = sys.version
    return platform == "msys" or (platform == "win32" and ver and "GCC" in ver)


def download(url):
    if requests:
        result = requests.get(url).content
    else:
        result = urllib_request.urlopen(url).read()
    return result


class build_ext(_build_ext.build_ext):
    def run(self):
        if is_invalid_windows_platform():
            msg = ("Please use official native CPython on Windows,"
                   " not Cygwin/MSYS/MSYS2/MinGW/etc.")
            raise OSError(msg)

        root_dir = os.path.dirname(os.getcwd())
        if is_native_windows_or_msys():
            BAZEL_SH = os.getenv("BAZEL_SH")
            SYSTEMROOT = os.getenv("SystemRoot")
            wsl_bash = os.path.join(SYSTEMROOT, "System32", "bash.exe")
            if (not BAZEL_SH) and SYSTEMROOT and os.path.isfile(wsl_bash):
                msg = (
                    "You appear to have Bash from WSL,"
                    " which Bazel is not compatible with."
                    "To avoid potential problems,"
                    " please explicitly set the {name!r}"
                    " environment variable for Bazel.").format(name="BAZEL_SH")
                raise ValueError(msg)

        if not os.path.isabs(root_dir):
            raise ValueError("root_dir must be an absolute path")

        # Note: We are passing in sys.executable so that we use the same
        # version of Python to build pyarrow inside the build.sh script. Note
        # that certain flags will not be passed along such as --user or sudo.
        # TODO(rkn): Fix this.
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-q",
            "pyarrow==" + pyarrow_version, "--find-links", pyarrow_url,
            "--target",
            os.path.join(root_dir, "python", "ray", "pyarrow_files")
        ])
        if (3, 6) <= sys.version_info[:2] <= (3, 7):
            subdir = os.path.join("python", "ray", "pickle5_files")
            pickle5_dir = os.path.join(root_dir, subdir)
            work_dir = tempfile.mkdtemp()
            try:
                urlpath = urllib_parse.urlparse(pickle5_url).path
                pickle5_name = os.path.basename(urllib_parse.unquote(urlpath))
                tgz = re.sub("\\.tar\\.gz$", ".tgz", pickle5_name, flags=re.I)
                pickle5_dirname = os.path.splitext(tgz)[0]
                tf = tarfile.open(None, "r", io.BytesIO(download(pickle5_url)))
                try:
                    tf.extractall(work_dir)
                finally:
                    tf.close()
                subprocess.check_call(
                    [sys.executable, "setup.py", "bdist_wheel"],
                    cwd=os.path.join(work_dir, pickle5_dirname))
                wheel_glob = os.path.join(work_dir, "dist", "*.whl")
                for wheel in glob.glob(wheel_glob):
                    wzf = zipfile.ZipFile(wheel, "r")
                    try:
                        wzf.extractall(pickle5_dir)
                    finally:
                        wzf.close()
            finally:
                shutil.rmtree(work_dir)

        bazel_env = os.environ.copy()
        # Apparently both Python 2 and 3 should be set to the same executable?
        bazel_env["PYTHON2_BIN_PATH"] = sys.executable
        bazel_env["PYTHON3_BIN_PATH"] = sys.executable
        bazel_build_cmd = ["bazel", "build", "--verbose_failures"]
        bazel_build_cmd.append("//:ray_pkg")
        if os.getenv("RAY_INSTALL_JAVA") == "1":
            # Also build binaries for Java if the above env variable exists.
            bazel_build_cmd.append("//java:all")
        subprocess.check_call(bazel_build_cmd, env=bazel_env)

        # We also need to install pyarrow along with Ray, so make sure that the
        # relevant non-Python pyarrow files get copied.
        pyarrow_files = []
        for (root, dirs, filenames) in os.walk("./ray/pyarrow_files/pyarrow"):
            for name in filenames:
                pyarrow_files.append(os.path.join(root, name))

        # We also need to install pickle5 along with Ray, so make sure that the
        # relevant non-Python pickle5 files get copied.
        pickle5_files = []
        for (root, dirs, filenames) in os.walk("./ray/pickle5_files/pickle5"):
            for name in filenames:
                pickle5_files.append(os.path.join(root, name))

        files_to_include = ray_files + pyarrow_files + pickle5_files

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    files_to_include.append(os.path.join(directory, filename))

        for filename in files_to_include:
            self.move_file(filename)

        # Try to copy over the optional files.
        for filename in optional_ray_files:
            try:
                self.move_file(filename)
            except Exception:
                print("Failed to copy optional file {}. This is ok."
                      .format(filename))

    def move_file(self, filename):
        # TODO(rkn): This feels very brittle. It may not handle all cases. See
        # https://github.com/apache/arrow/blob/master/python/setup.py for an
        # example.
        source = filename
        destination = os.path.join(self.build_lib, filename)
        # Create the target directory if it doesn't already exist.
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        if not os.path.exists(destination):
            print("Copying {} to {}.".format(source, destination))
            shutil.copy(source, destination)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


def find_version(*filepath):
    # Extract version information from filepath
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


requires = [
    "numpy >= 1.14",
    "filelock",
    "jsonschema",
    "funcsigs",
    "click",
    "colorama",
    "pytest",
    "pyyaml",
    "redis>=3.3.2",
    # NOTE: Don't upgrade the version of six! Doing so causes installation
    # problems. See https://github.com/ray-project/ray/issues/4169.
    "six >= 1.0.0",
    "faulthandler;python_version<'3.3'",
    "protobuf >= 3.8.0",
]

setup(
    name="ray",
    version=find_version("ray", "__init__.py"),
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=("A system for parallel and distributed Python that unifies "
                 "the ML ecosystem."),
    long_description=open(
        os.path.join(
            os.path.dirname(__file__), os.path.pardir, "README.rst"),
        "r").read(),
    url="https://github.com/ray-project/ray",
    keywords=("ray distributed parallel machine-learning "
              "reinforcement-learning deep-learning python"),
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=requires,
    setup_requires=["cython >= 0.29"],
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "ray=ray.scripts.scripts:main",
            "rllib=ray.rllib.scripts:cli [rllib]", "tune=ray.tune.scripts:cli"
        ]
    },
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
