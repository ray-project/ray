import argparse
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

import urllib.parse
import urllib.request

requests = None  # lazy-loaded (slow import)

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

SUPPORTED_PYTHONS = [(3, 5), (3, 6), (3, 7), (3, 8)]

ROOT_DIR = os.path.dirname(__file__)
BUILD_JAVA = os.getenv("RAY_INSTALL_JAVA") == "1"

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"

install_requires = [
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

setup_requires = [
    "cython >= 0.29.14",
    "wheel",
]

pickle5_url = ("https://github.com/suquark/pickle5-backport/archive/"
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

if BUILD_JAVA:
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
    global requests
    if requests is None:
        try:
            import requests
        except ImportError:
            requests = False
    if requests:
        result = requests.get(url).content
    else:
        result = urllib.request.urlopen(url).read()
    return result


def download_pickle5(pickle5_dir):
    pickle5_file = urllib.parse.unquote(
        urllib.parse.urlparse(pickle5_url).path)
    pickle5_name = re.sub("\\.tar\\.gz$", ".tgz", pickle5_file, flags=re.I)
    python = sys.executable
    with tempfile.TemporaryDirectory() as work_dir:
        tf = tarfile.open(None, "r", io.BytesIO(download(pickle5_url)))
        try:
            tf.extractall(work_dir)
        finally:
            tf.close()
        relpath = "-".join(os.path.splitext(pickle5_name)[0].split("/")[2::2])
        src_dir = os.path.join(work_dir, relpath)
        subprocess.check_call([python, "setup.py", "bdist_wheel"], cwd=src_dir)
        for wheel in glob.glob(os.path.join(src_dir, "dist", "*.whl")):
            wzf = zipfile.ZipFile(wheel, "r")
            try:
                wzf.extractall(pickle5_dir)
            finally:
                wzf.close()


def build(bazel_targets):
    if tuple(sys.version_info[:2]) not in SUPPORTED_PYTHONS:
        msg = ("Detected Python version {}, which is not supported. "
               "Only Python {} are supported.").format(
                   ".".join(map(str, sys.version_info[:2])),
                   ", ".join(".".join(map(str, v)) for v in SUPPORTED_PYTHONS))
        raise RuntimeError(msg)

    if is_invalid_windows_platform():
        msg = ("Please use official native CPython on Windows,"
               " not Cygwin/MSYS/MSYS2/MinGW/etc.\n" +
               "Detected: {}\n  at: {!r}".format(sys.version, sys.executable))
        raise OSError(msg)

    if is_native_windows_or_msys():
        BAZEL_SH = os.getenv("BAZEL_SH")
        SYSTEMROOT = os.getenv("SystemRoot")
        wsl_bash = os.path.join(SYSTEMROOT, "System32", "bash.exe")
        if (not BAZEL_SH) and SYSTEMROOT and os.path.isfile(wsl_bash):
            msg = ("You appear to have Bash from WSL,"
                   " which Bazel may invoke unexpectedly. "
                   "To avoid potential problems,"
                   " please explicitly set the {name!r}"
                   " environment variable for Bazel.").format(name="BAZEL_SH")
            raise RuntimeError(msg)

    download_pickle5(os.path.join(ROOT_DIR, "ray", "pickle5_files"))

    # Note: We are passing in sys.executable so that we use the same
    # version of Python to build packages inside the build.sh script. Note
    # that certain flags will not be passed along such as --user or sudo.
    # TODO(rkn): Fix this.
    if not os.getenv("SKIP_THIRDPARTY_INSTALL"):
        pip_packages = ["psutil", "setproctitle"]
        subprocess.check_call(
            [
                sys.executable, "-m", "pip", "install", "-q",
                "--target=" + os.path.join(ROOT_DIR, "ray", "thirdparty_files")
            ] + pip_packages,
            env=dict(os.environ, CC="gcc"))

    bazel = os.getenv("BAZEL_EXECUTABLE", "bazel")
    return subprocess.check_call(
        [bazel, "build", "--verbose_failures", "--"] + bazel_targets,
        env=dict(os.environ, PYTHON3_BIN_PATH=sys.executable))


def walk_directory(directory):
    file_list = []
    for (root, dirs, filenames) in os.walk(directory):
        for name in filenames:
            file_list.append(os.path.join(root, name))
    return file_list


def move_file(target_dir, filename):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    source = filename
    destination = os.path.join(target_dir, filename)
    # Create the target directory if it doesn't already exist.
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if not os.path.exists(destination):
        print("Copying {} to {}.".format(source, destination))
        shutil.copy(source, destination, follow_symlinks=True)


def find_version(*filepath):
    # Extract version information from filepath
    with open(os.path.join(ROOT_DIR, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


def pip_run(build_ext):
    build(["//:ray_pkg"] + (["//java:ray_java_pkg"] if BUILD_JAVA else []))

    files_to_include = list(ray_files)

    # We also need to install pickle5 along with Ray, so make sure that the
    # relevant non-Python pickle5 files get copied.
    pickle5_dir = os.path.join(ROOT_DIR, "ray", "pickle5_files")
    files_to_include += walk_directory(os.path.join(pickle5_dir, "pickle5"))

    thirdparty_dir = os.path.join(ROOT_DIR, "ray", "thirdparty_files")
    files_to_include += walk_directory(thirdparty_dir)

    # Copy over the autogenerated protobuf Python bindings.
    for directory in generated_python_directories:
        for filename in os.listdir(directory):
            if filename[-3:] == ".py":
                files_to_include.append(os.path.join(directory, filename))

    for filename in files_to_include:
        move_file(build_ext.build_lib, filename)

    # Try to copy over the optional files.
    for filename in optional_ray_files:
        try:
            move_file(build_ext.build_lib, filename)
        except Exception:
            print("Failed to copy optional file {}. This is ok."
                  .format(filename))


def api_main(program, *args):
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, choices=["build", "help"])
    parser.add_argument(
        "-l",
        "--language",
        default="python",
        type=str,
        help="A list of languages to build native libraries. "
        "Supported languages include \"python\" and \"java\". "
        "If not specified, only the Python library will be built.")
    parsed_args = parser.parse_args(args)

    result = None

    if parsed_args.command == "build":
        bazel_targets = []
        for lang in parsed_args.language.split(","):
            if "python" in lang:
                bazel_targets.append("//:ray_pkg")
            elif "java" in lang:
                bazel_targets.append("//java:ray_java_pkg")
            else:
                raise ValueError("invalid language: {!r}".format(lang))
        result = build(bazel_targets)
    elif parsed_args.command == "help":
        parser.print_help()
    else:
        raise ValueError("Invalid command: {!r}".format(parsed_args.command))

    return result


if __name__ == "__api__":
    api_main(*sys.argv)

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
        name="ray",
        version=find_version("ray", "__init__.py"),
        author="Ray Team",
        author_email="ray-dev@googlegroups.com",
        description=("A system for parallel and distributed Python that "
                     "unifies the ML ecosystem."),
        long_description=io.open(
            os.path.join(ROOT_DIR, os.path.pardir, "README.rst"),
            "r",
            encoding="utf-8").read(),
        url="https://github.com/ray-project/ray",
        keywords=("ray distributed parallel machine-learning "
                  "reinforcement-learning deep-learning python"),
        packages=setuptools.find_packages(),
        cmdclass={"build_ext": build_ext},
        # The BinaryDistribution argument triggers build_ext.
        distclass=BinaryDistribution,
        install_requires=install_requires,
        setup_requires=setup_requires,
        extras_require=extras,
        entry_points={
            "console_scripts": [
                "ray=ray.scripts.scripts:main",
                "rllib=ray.rllib.scripts:cli [rllib]",
                "tune=ray.tune.scripts:cli"
            ]
        },
        include_package_data=True,
        zip_safe=False,
        license="Apache 2.0")
