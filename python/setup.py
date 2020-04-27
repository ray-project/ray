import errno
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

import urllib.parse
import urllib.request

try:
    import requests
except ImportError:
    requests = None

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

SUPPORTED_PYTHONS = [(3, 5), (3, 6), (3, 7), (3, 8)]

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"

root_dir = os.path.dirname(__file__)

pickle5_url = (
    "https://github.com/suquark/pickle5-backport/archive/"
    "8ffe41ceba9d5e2ce8a98190f6b3d2f3325e5a72.tar.gz")

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server",
    "ray/core/src/ray/gcs/redis_module/libray_redis_module.so",
    "ray/core/src/plasma/plasma_store_server" + exe_suffix,
    "ray/_raylet" + pyd_suffix,
    "ray/core/src/ray/raylet/raylet_monitor" + exe_suffix,
    "ray/core/src/ray/gcs/gcs_server" + exe_suffix,
    "ray/core/src/ray/raylet/raylet" + exe_suffix,
    "ray/streaming/_streaming.so",
]

build_java = os.getenv("RAY_INSTALL_JAVA") == "1"
if build_java:
    ray_files.append("ray/jars/ray_dist.jar")

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/streaming/generated",
]

optional_ray_files = []

ray_autoscaler_files = [
    "ray/autoscaler/aws/example-full.yaml",
    "ray/autoscaler/azure/example-full.yaml",
    "ray/autoscaler/azure/azure-vm-template.json",
    "ray/autoscaler/azure/azure-config-template.json",
    "ray/autoscaler/gcp/example-full.yaml",
    "ray/autoscaler/local/example-full.yaml",
    "ray/autoscaler/kubernetes/example-full.yaml",
    "ray/autoscaler/kubernetes/kubectl-rsync.sh",
    "ray/autoscaler/ray-schema.json"
]

ray_project_files = [
    "ray/projects/schema.json", "ray/projects/templates/cluster_template.yaml",
    "ray/projects/templates/project_template.yaml",
    "ray/projects/templates/requirements.txt"
]

ray_dashboard_files = [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/client/build")
    for filename in filenames
]

optional_ray_files += ray_autoscaler_files
optional_ray_files += ray_project_files
optional_ray_files += ray_dashboard_files

if os.getenv("RAY_USE_NEW_GCS") == "on":
    ray_files += [
        "ray/core/src/credis/build/src/libmember.so",
        "ray/core/src/credis/build/src/libmaster.so",
        "ray/core/src/credis/redis/src/redis-server"
    ]

extras = {
    "debug": [],
    "dashboard": ["requests"],
    "serve": ["uvicorn", "pygments", "werkzeug", "flask", "pandas", "blist"],
    "tune": ["tabulate", "tensorboardX", "pandas"]
}

extras["rllib"] = extras["tune"] + [
    "atari_py",
    "dm_tree",
    "gym[atari]",
    "lz4",
    "opencv-python-headless",
    "pyyaml",
    "scipy",
]

extras["streaming"] = ["msgpack >= 0.6.2"]

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
        result = urllib.request.urlopen(url).read()
    return result


class build_ext(_build_ext.build_ext):
    def run(self):
        if tuple(sys.version_info[:2]) not in SUPPORTED_PYTHONS:
            msg = ("Detected Python version {}, which is not supported. "
                   "Only Python {} are supported.").format(
                ".".join(map(str, sys.version_info[:2])),
                ", ".join(".".join(map(str, v)) for v in SUPPORTED_PYTHONS))
            raise RuntimeError(msg)

        if is_invalid_windows_platform():
            msg = ("Please use official native CPython on Windows,"
                   " not Cygwin/MSYS/MSYS2/MinGW/etc.")
            raise OSError(msg)

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

        pickle5_dir = os.path.join(root_dir, "python", "ray", "pickle5_files")

        # Note: We are passing in sys.executable so that we use the same
        # version of Python to build packages inside the build.sh script. Note
        # that certain flags will not be passed along such as --user or sudo.
        # TODO(rkn): Fix this.
        if (3, 6) <= sys.version_info[:2] <= (3, 7):
            pickle5_file = os.path.basename(urllib.parse.unquote(
                urllib.parse.urlparse(pickle5_url).path))
            pickle5_name = os.path.basename(
                re.sub("\\.tar\\.gz$", ".tgz", pickle5_file, flags=re.I))
            with tempfile.TemporaryDirectory() as work_dir:
                tf = tarfile.open(None, "r", io.BytesIO(download(pickle5_url)))
                try:
                    tf.extractall(work_dir)
                finally:
                    tf.close()
                subprocess.check_call(
                    [sys.executable, "setup.py", "bdist_wheel"],
                    cwd=os.path.join(work_dir,
                                     os.path.splitext(pickle5_name)[0]))
                wheel_glob = os.path.join(work_dir, "dist", "*.whl")
                for wheel in glob.glob(wheel_glob):
                    wzf = zipfile.ZipFile(wheel, "r")
                    try:
                        wzf.extractall(pickle5_dir)
                    finally:
                        wzf.close()

        thirdparty_dir = os.path.join(
            root_dir, "python", "ray", "thirdparty_files")

        if not os.getenv("SKIP_THIRDPARTY_INSTALL"):
            pip_packages = ["psutil", "setproctitle"]
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-q",
                 "--target=" + thirdparty_dir] + pip_packages,
                env=dict(os.environ, CC="gcc"))

        bazel = os.getenv("BAZEL_EXECUTABLE", "bazel")
        bazel_targets = ["//:ray_pkg"]
        if build_java:
            bazel_targets += ["//java:ray_java_pkg"]
        subprocess.check_call(
            [bazel, "build", "--verbose_failures"] + bazel_targets,
            env=dict(os.environ, PYTHON3_BIN_PATH=sys.executable))

        # We also need to install pickle5 along with Ray, so make sure that the
        # relevant non-Python pickle5 files get copied.
        pickle5_files = self.walk_directory(os.path.join(pickle5_dir,
                                                         "pickle5"))

        thirdparty_files = self.walk_directory(thirdparty_dir)

        files_to_include = ray_files + pickle5_files + thirdparty_files

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

    def walk_directory(self, directory):
        file_list = []
        for (root, dirs, filenames) in os.walk(directory):
            for name in filenames:
                file_list.append(os.path.join(root, name))
        return file_list

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
            shutil.copy(source, destination, follow_symlinks=True)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


def find_version(*filepath):
    # Extract version information from filepath
    with open(os.path.join(root_dir, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


requires = [
    "aiohttp",
    "click",
    "colorama",
    "filelock",
    "google",
    "grpcio",
    "jsonschema",
    "msgpack >= 0.6.0, < 1.0.0",
    "numpy >= 1.16",
    "protobuf >= 3.8.0",
    "py-spy >= 0.2.0",
    "pyyaml",
    "redis >= 3.3.2",
]

setup(
    name="ray",
    version=find_version("ray", "__init__.py"),
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=("A system for parallel and distributed Python that unifies "
                 "the ML ecosystem."),
    long_description=open(os.path.join(root_dir, os.path.pardir, "README.rst"),
        "r").read(),
    url="https://github.com/ray-project/ray",
    keywords=("ray distributed parallel machine-learning "
              "reinforcement-learning deep-learning python"),
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=requires,
    setup_requires=["cython >= 0.29.14", "wheel"],
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
