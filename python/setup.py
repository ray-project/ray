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

from itertools import chain
from itertools import takewhile

import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

SUPPORTED_PYTHONS = [(3, 6), (3, 7), (3, 8), (3, 9)]
SUPPORTED_BAZEL = (3, 2, 0)

ROOT_DIR = os.path.dirname(__file__)
BUILD_JAVA = os.getenv("RAY_INSTALL_JAVA") == "1"

PICKLE5_SUBDIR = os.path.join("ray", "pickle5_files")
THIRDPARTY_SUBDIR = os.path.join("ray", "thirdparty_files")

CLEANABLE_SUBDIRS = [PICKLE5_SUBDIR, THIRDPARTY_SUBDIR]

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"

pickle5_url = ("https://github.com/pitrou/pickle5-backport/archive/"
               "c0c1a158f59366696161e0dffdd10cfe17601372.tar.gz")

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server" + exe_suffix,
    "ray/core/src/ray/gcs/redis_module/libray_redis_module.so",
    "ray/_raylet" + pyd_suffix,
    "ray/core/src/ray/gcs/gcs_server" + exe_suffix,
    "ray/core/src/ray/raylet/raylet" + exe_suffix,
    "ray/streaming/_streaming.so",
]

if BUILD_JAVA or os.path.exists(
        os.path.join(ROOT_DIR, "ray/jars/ray_dist.jar")):
    ray_files.append("ray/jars/ray_dist.jar")

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/streaming/generated",
]

ray_files.append("ray/nightly-wheels.yaml")

# Autoscaler files.
ray_files += [
    "ray/autoscaler/aws/defaults.yaml",
    "ray/autoscaler/azure/defaults.yaml",
    "ray/autoscaler/_private/azure/azure-vm-template.json",
    "ray/autoscaler/_private/azure/azure-config-template.json",
    "ray/autoscaler/gcp/defaults.yaml",
    "ray/autoscaler/local/defaults.yaml",
    "ray/autoscaler/kubernetes/defaults.yaml",
    "ray/autoscaler/_private/_kubernetes/kubectl-rsync.sh",
    "ray/autoscaler/staroid/defaults.yaml",
    "ray/autoscaler/ray-schema.json",
]

# Dashboard files.
ray_files += [
    os.path.join(dirpath, filename) for dirpath, dirnames, filenames in
    os.walk("ray/new_dashboard/client/build") for filename in filenames
]

# If you're adding dependencies for ray extras, please
# also update the matching section of requirements/requirements.txt
# in this directory
extras = {
    "default": ["colorful"],
    "serve": ["uvicorn", "requests", "pydantic>=1.8", "starlette", "fastapi"],
    "tune": ["pandas", "tabulate", "tensorboardX"],
    "k8s": ["kubernetes"]
}

extras["rllib"] = extras["tune"] + [
    "dm_tree",
    "gym",
    "lz4",
    "opencv-python-headless<=4.3.0.36",
    "pyyaml",
    "scipy",
]

extras["all"] = list(set(chain.from_iterable(extras.values())))

# These are the main dependencies for users of ray. This list
# should be carefully curated. If you change it, please reflect
# the change in the matching section of requirements/requirements.txt
install_requires = [
    # TODO(alex) Pin the version once this PR is
    # included in the stable release.
    # https://github.com/aio-libs/aiohttp/pull/4556#issuecomment-679228562
    "aiohttp",
    "aiohttp_cors",
    "aioredis",
    "click >= 7.0",
    "colorama",
    "dataclasses; python_version < '3.7'",
    "filelock",
    "gpustat",
    "grpcio >= 1.28.1",
    "jsonschema",
    "msgpack >= 1.0.0, < 2.0.0",
    "numpy >= 1.16; python_version < '3.9'",
    "numpy >= 1.19.3; python_version >= '3.9'",
    "protobuf >= 3.15.3",
    "py-spy >= 0.2.0",
    "pyyaml",
    "requests",
    "redis >= 3.5.0",
    "opencensus",
    "prometheus_client >= 0.7.1",
]


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


def download(url):
    try:
        result = urllib.request.urlopen(url).read()
    except urllib.error.URLError:
        # This fallback is necessary on Python 3.5 on macOS due to TLS 1.2.
        curl_args = ["curl", "-s", "-L", "-f", "-o", "-", url]
        result = subprocess.check_output(curl_args)
    return result


# Installs pickle5-backport into the local subdirectory.
def download_pickle5(pickle5_dir):
    pickle5_file = urllib.parse.unquote(
        urllib.parse.urlparse(pickle5_url).path)
    pickle5_name = re.sub("\\.tar\\.gz$", ".tgz", pickle5_file, flags=re.I)
    url_path_parts = os.path.splitext(pickle5_name)[0].split("/")
    (project, commit) = (url_path_parts[2], url_path_parts[4])
    pickle5_archive = download(pickle5_url)
    with tempfile.TemporaryDirectory() as work_dir:
        tf = tarfile.open(None, "r", io.BytesIO(pickle5_archive))
        try:
            tf.extractall(work_dir)
        finally:
            tf.close()
        src_dir = os.path.join(work_dir, project + "-" + commit)
        args = [sys.executable, "setup.py", "-q", "bdist_wheel"]
        subprocess.check_call(args, cwd=src_dir)
        for wheel in glob.glob(os.path.join(src_dir, "dist", "*.whl")):
            wzf = zipfile.ZipFile(wheel, "r")
            try:
                wzf.extractall(pickle5_dir)
            finally:
                wzf.close()


def build(build_python, build_java):
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

    bazel_env = dict(os.environ, PYTHON3_BIN_PATH=sys.executable)

    if is_native_windows_or_msys():
        SHELL = bazel_env.get("SHELL")
        if SHELL:
            bazel_env.setdefault("BAZEL_SH", os.path.normpath(SHELL))
        BAZEL_SH = bazel_env["BAZEL_SH"]
        SYSTEMROOT = os.getenv("SystemRoot")
        wsl_bash = os.path.join(SYSTEMROOT, "System32", "bash.exe")
        if (not BAZEL_SH) and SYSTEMROOT and os.path.isfile(wsl_bash):
            msg = ("You appear to have Bash from WSL,"
                   " which Bazel may invoke unexpectedly. "
                   "To avoid potential problems,"
                   " please explicitly set the {name!r}"
                   " environment variable for Bazel.").format(name="BAZEL_SH")
            raise RuntimeError(msg)

    # Check if the current Python already has pickle5 (either comes with newer
    # Python versions, or has been installed by us before).
    pickle5 = None
    if sys.version_info >= (3, 8, 2):
        import pickle as pickle5
    else:
        try:
            import pickle5
        except ImportError:
            pass
    if not pickle5:
        download_pickle5(os.path.join(ROOT_DIR, PICKLE5_SUBDIR))

    # Note: We are passing in sys.executable so that we use the same
    # version of Python to build packages inside the build.sh script. Note
    # that certain flags will not be passed along such as --user or sudo.
    # TODO(rkn): Fix this.
    if not os.getenv("SKIP_THIRDPARTY_INSTALL"):
        pip_packages = ["psutil", "setproctitle==1.1.10"]
        subprocess.check_call(
            [
                sys.executable, "-m", "pip", "install", "-q",
                "--target=" + os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR)
            ] + pip_packages,
            env=dict(os.environ, CC="gcc"))

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

    bazel_targets = []
    bazel_targets += ["//:ray_pkg"] if build_python else []
    bazel_targets += ["//java:ray_java_pkg"] if build_java else []
    return bazel_invoke(
        subprocess.check_call,
        ["build", "--verbose_failures", "--"] + bazel_targets,
        env=bazel_env)


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
        if sys.platform == "win32":
            # Does not preserve file mode (needed to avoid read-only bit)
            shutil.copyfile(source, destination, follow_symlinks=True)
        else:
            # Preserves file mode (needed to copy executable bit)
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
    build(True, BUILD_JAVA)

    files_to_include = list(ray_files)

    # We also need to install pickle5 along with Ray, so make sure that the
    # relevant non-Python pickle5 files get copied.
    pickle5_dir = os.path.join(ROOT_DIR, PICKLE5_SUBDIR)
    files_to_include += walk_directory(os.path.join(pickle5_dir, "pickle5"))

    thirdparty_dir = os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR)
    files_to_include += walk_directory(thirdparty_dir)

    # Copy over the autogenerated protobuf Python bindings.
    for directory in generated_python_directories:
        for filename in os.listdir(directory):
            if filename[-3:] == ".py":
                files_to_include.append(os.path.join(directory, filename))

    for filename in files_to_include:
        move_file(build_ext.build_lib, filename)


def api_main(program, *args):
    parser = argparse.ArgumentParser()
    choices = ["build", "bazel_version", "python_versions", "clean", "help"]
    parser.add_argument("command", type=str, choices=choices)
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
        kwargs = dict(build_python=False, build_java=False)
        for lang in parsed_args.language.split(","):
            if "python" in lang:
                kwargs.update(build_python=True)
            elif "java" in lang:
                kwargs.update(build_java=True)
            else:
                raise ValueError("invalid language: {!r}".format(lang))
        result = build(**kwargs)
    elif parsed_args.command == "bazel_version":
        print(".".join(map(str, SUPPORTED_BAZEL)))
    elif parsed_args.command == "python_versions":
        for version in SUPPORTED_PYTHONS:
            # NOTE: On Windows this will print "\r\n" on the command line.
            # Strip it out by piping to tr -d "\r".
            print(".".join(map(str, version)))
    elif parsed_args.command == "clean":

        def onerror(function, path, excinfo):
            nonlocal result
            if excinfo[1].errno != errno.ENOENT:
                msg = excinfo[1].strerror
                logger.error("cannot remove {}: {}".format(path, msg))
                result = 1

        for subdir in CLEANABLE_SUBDIRS:
            shutil.rmtree(os.path.join(ROOT_DIR, subdir), onerror=onerror)
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
    description=("Ray provides a simple, universal API for building "
                 "distributed applications."),
    long_description=io.open(
        os.path.join(ROOT_DIR, os.path.pardir, "README.rst"),
        "r",
        encoding="utf-8").read(),
    url="https://github.com/ray-project/ray",
    keywords=("ray distributed parallel machine-learning hyperparameter-tuning"
              "reinforcement-learning deep-learning serving python"),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=setuptools.find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=install_requires,
    setup_requires=["cython >= 0.29.14", "wheel"],
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "ray=ray.scripts.scripts:main",
            "rllib=ray.rllib.scripts:cli [rllib]",
            "tune=ray.tune.scripts:cli",
            "ray-operator=ray.ray_operator.operator:main",
            "serve=ray.serve.scripts:cli",
        ]
    },
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0") if __name__ == "__main__" else None
