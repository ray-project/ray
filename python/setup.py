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
from enum import Enum

import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

SUPPORTED_PYTHONS = [(3, 6), (3, 7), (3, 8), (3, 9)]
# When the bazel version is updated, make sure to update it
# in WORKSPACE file as well.
SUPPORTED_BAZEL = (4, 2, 1)

ROOT_DIR = os.path.dirname(__file__)
BUILD_JAVA = os.getenv("RAY_INSTALL_JAVA") == "1"
SKIP_BAZEL_BUILD = os.getenv("SKIP_BAZEL_BUILD") == "1"
BAZEL_LIMIT_CPUS = os.getenv("BAZEL_LIMIT_CPUS")

PICKLE5_SUBDIR = os.path.join("ray", "pickle5_files")
THIRDPARTY_SUBDIR = os.path.join("ray", "thirdparty_files")

CLEANABLE_SUBDIRS = [PICKLE5_SUBDIR, THIRDPARTY_SUBDIR]

# In automated builds, we do a few adjustments before building. For instance,
# the bazel environment is set up slightly differently, and symlinks are
# replaced with junctions in Windows. This variable is set e.g. in our conda
# feedstock.
is_automated_build = bool(int(os.environ.get("IS_AUTOMATED_BUILD", "0")))

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"

pickle5_url = ("https://github.com/pitrou/pickle5-backport/archive/"
               "e6117502435aba2901585cc6c692fb9582545f08.tar.gz")


def find_version(*filepath):
    # Extract version information from filepath
    with open(os.path.join(ROOT_DIR, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


class SetupType(Enum):
    RAY = 1
    RAY_CPP = 2


class BuildType(Enum):
    DEFAULT = 1
    DEBUG = 2
    ASAN = 3
    TSAN = 4


class SetupSpec:
    def __init__(self, type: SetupType, name: str, description: str,
                 build_type: BuildType):
        self.type: SetupType = type
        self.name: str = name
        version = find_version("ray", "__init__.py")
        # add .dbg suffix if debug mode is on.
        if build_type == BuildType.DEBUG:
            self.version: str = f"{version}+dbg"
        elif build_type == BuildType.ASAN:
            self.version: str = f"{version}+asan"
        elif build_type == BuildType.TSAN:
            self.version: str = f"{version}+tsan"
        else:
            self.version = version
        self.description: str = description
        self.build_type: BuildType = build_type
        self.files_to_include: list = []
        self.install_requires: list = []
        self.extras: dict = {}

    def get_packages(self):
        if self.type == SetupType.RAY:
            return setuptools.find_packages()
        else:
            return []


build_type = os.getenv("RAY_DEBUG_BUILD")
if build_type == "debug":
    BUILD_TYPE = BuildType.DEBUG
elif build_type == "asan":
    BUILD_TYPE = BuildType.ASAN
elif build_type == "tsan":
    BUILD_TYPE = BuildType.TSAN
else:
    BUILD_TYPE = BuildType.DEFAULT

if os.getenv("RAY_INSTALL_CPP") == "1":
    # "ray-cpp" wheel package.
    setup_spec = SetupSpec(
        SetupType.RAY_CPP, "ray-cpp",
        "A subpackage of Ray which provides the Ray C++ API.", BUILD_TYPE)
else:
    # "ray" primary wheel package.
    setup_spec = SetupSpec(
        SetupType.RAY, "ray", "Ray provides a simple, "
        "universal API for building distributed applications.", BUILD_TYPE)
    RAY_EXTRA_CPP = True
    # Disable extra cpp for the development versions.
    if "dev" in setup_spec.version or os.getenv(
            "RAY_DISABLE_EXTRA_CPP") == "1":
        RAY_EXTRA_CPP = False

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server" + exe_suffix,
    "ray/_raylet" + pyd_suffix,
    "ray/core/src/ray/gcs/gcs_server" + exe_suffix,
    "ray/core/src/ray/raylet/raylet" + exe_suffix,
]

if BUILD_JAVA or os.path.exists(
        os.path.join(ROOT_DIR, "ray/jars/ray_dist.jar")):
    ray_files.append("ray/jars/ray_dist.jar")

if setup_spec.type == SetupType.RAY_CPP:
    setup_spec.files_to_include += ["ray/cpp/default_worker" + exe_suffix]
    # C++ API library and project template files.
    setup_spec.files_to_include += [
        os.path.join(dirpath, filename)
        for dirpath, dirnames, filenames in os.walk("ray/cpp")
        for filename in filenames
    ]

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/serve/generated",
]

ray_files.append("ray/nightly-wheels.yaml")

# Autoscaler files.
ray_files += [
    "ray/autoscaler/aws/defaults.yaml",
    "ray/autoscaler/aws/cloudwatch/prometheus.yml",
    "ray/autoscaler/aws/cloudwatch/ray_prometheus_waiter.sh",
    "ray/autoscaler/azure/defaults.yaml",
    "ray/autoscaler/_private/_azure/azure-vm-template.json",
    "ray/autoscaler/_private/_azure/azure-config-template.json",
    "ray/autoscaler/gcp/defaults.yaml",
    "ray/autoscaler/local/defaults.yaml",
    "ray/autoscaler/kubernetes/defaults.yaml",
    "ray/autoscaler/_private/_kubernetes/kubectl-rsync.sh",
    "ray/autoscaler/staroid/defaults.yaml",
    "ray/autoscaler/ray-schema.json",
]

# Dashboard files.
ray_files += [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/client/build")
    for filename in filenames
]

# If you're adding dependencies for ray extras, please
# also update the matching section of requirements/requirements.txt
# in this directory
if setup_spec.type == SetupType.RAY:
    setup_spec.extras = {
        "data": [
            "pandas",
            "pyarrow>=4.0.1",
            "fsspec",
        ],
        "default": [
            "aiohttp >= 3.7",
            "aiosignal",
            "aiohttp_cors",
            "aioredis < 2",
            "colorful",
            "frozenlist",
            "py-spy >= 0.2.0",
            "requests",
            "gpustat >= 1.0.0b1",  # for windows
            "opencensus",
            "prometheus_client >= 0.7.1",
            "smart_open"
        ],
        "serve": [
            "uvicorn==0.16.0", "requests", "starlette", "fastapi", "aiorwlock"
        ],
        "tune": ["pandas", "tabulate", "tensorboardX>=1.9", "requests"],
        "k8s": ["kubernetes", "urllib3"],
        "observability": [
            "opentelemetry-api==1.1.0", "opentelemetry-sdk==1.1.0",
            "opentelemetry-exporter-otlp==1.1.0"
        ],
    }

    if sys.version_info >= (3, 7):
        # Numpy dropped python 3.6 support in 1.20.
        setup_spec.extras["data"].append("numpy >= 1.20")
    else:
        setup_spec.extras["data"].append("numpy >= 1.19")

    # Ray Serve depends on the Ray dashboard components.
    setup_spec.extras["serve"] = list(
        set(setup_spec.extras["serve"] + setup_spec.extras["default"]))

    if RAY_EXTRA_CPP:
        setup_spec.extras["cpp"] = ["ray-cpp==" + setup_spec.version]

    if sys.version_info >= (3, 7, 0):
        setup_spec.extras["k8s"].append("kopf")

    setup_spec.extras["rllib"] = setup_spec.extras["tune"] + [
        "dm_tree",
        "gym",
        "lz4",
        # matplotlib (dependency of scikit-image) 3.4.3 breaks docker build
        # Todo: Remove this when safe?
        "matplotlib!=3.4.3",
        "scikit-image",
        "pyyaml",
        "scipy",
    ]

    setup_spec.extras["all"] = list(
        set(chain.from_iterable(setup_spec.extras.values())))

# These are the main dependencies for users of ray. This list
# should be carefully curated. If you change it, please reflect
# the change in the matching section of requirements/requirements.txt
if setup_spec.type == SetupType.RAY:
    setup_spec.install_requires = [
        "attrs",
        "click >= 7.0",
        "dataclasses; python_version < '3.7'",
        "filelock",
        "grpcio >= 1.28.1",
        "jsonschema",
        "msgpack >= 1.0.0, < 2.0.0",
        "numpy >= 1.16; python_version < '3.9'",
        "numpy >= 1.19.3; python_version >= '3.9'",
        "protobuf >= 3.15.3",
        "pyyaml",
        "redis >= 3.5.0",
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


def patch_isdir():
    """
    Python on Windows is having hard times at telling if a symlink is
    a directory - it can "guess" wrong at times, which bites when
    finding packages. Replace with a fixed version which unwraps links first.
    """
    orig_isdir = os.path.isdir

    def fixed_isdir(path):
        while os.path.islink(path):
            try:
                link = os.readlink(path)
            except OSError:
                break
            path = os.path.abspath(os.path.join(os.path.dirname(path), link))
        return orig_isdir(path)

    os.path.isdir = fixed_isdir


def replace_symlinks_with_junctions():
    """
    Per default Windows requires admin access to create symlinks, while
    junctions (which behave similarly) can be created by users.

    This function replaces symlinks (which might be broken when checked
    out without admin rights) with junctions so Ray can be built both
    with and without admin access.
    """
    assert is_native_windows_or_msys()

    # Update this list if new symlinks are introduced to the source tree
    _LINKS = {
        r"ray\dashboard": "../../dashboard",
        r"ray\rllib": "../../rllib",
    }
    root_dir = os.path.dirname(__file__)
    for link, default in _LINKS.items():
        path = os.path.join(root_dir, link)
        try:
            out = subprocess.check_output(
                "DIR /A:LD /B", shell=True, cwd=os.path.dirname(path))
        except subprocess.CalledProcessError:
            out = b""
        if os.path.basename(path) in out.decode("utf8").splitlines():
            logger.info(f"'{link}' is already converted to junction point")
        else:
            logger.info(f"Converting '{link}' to junction point...")
            if os.path.isfile(path):
                with open(path) as inp:
                    target = inp.read()
                os.unlink(path)
            elif os.path.isdir(path):
                target = default
                try:
                    # unlink() works on links as well as on regular files,
                    # and links to directories are considered directories now
                    os.unlink(path)
                except OSError as err:
                    # On Windows attempt to unlink a regular directory results
                    # in a PermissionError with errno set to errno.EACCES.
                    if err.errno != errno.EACCES:
                        raise
                    # For regular directories deletion is done with rmdir call.
                    os.rmdir(path)
            else:
                raise ValueError(f"Unexpected type of entry: '{path}'")
            target = os.path.abspath(
                os.path.join(os.path.dirname(path), target))
            logger.info("Setting {} -> {}".format(link, target))
            subprocess.check_call(
                f'MKLINK /J "{os.path.basename(link)}" "{target}"',
                shell=True,
                cwd=os.path.dirname(path))


if is_automated_build and is_native_windows_or_msys():
    # Automated replacements should only happen in automatic build
    # contexts for now
    patch_isdir()
    replace_symlinks_with_junctions()


def build(build_python, build_java, build_cpp):
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
        BAZEL_SH = bazel_env.get("BAZEL_SH", "")
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
        pip_packages = ["psutil", "setproctitle==1.2.2", "colorama"]
        subprocess.check_call(
            [
                sys.executable, "-m", "pip", "install", "-q",
                "--target=" + os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR)
            ] + pip_packages,
            env=dict(os.environ, CC="gcc"))

    bazel_flags = ["--verbose_failures"]
    if BAZEL_LIMIT_CPUS:
        n = int(BAZEL_LIMIT_CPUS)  # the value must be an int
        bazel_flags.append(f"--local_cpu_resources={n}")

    if not is_automated_build:
        bazel_precmd_flags = []
    if is_automated_build:
        root_dir = os.path.join(
            os.path.abspath(os.environ["SRC_DIR"]), "..", "bazel-root")
        out_dir = os.path.join(
            os.path.abspath(os.environ["SRC_DIR"]), "..", "b-o")

        for d in (root_dir, out_dir):
            if not os.path.exists(d):
                os.makedirs(d)

        bazel_precmd_flags = [
            "--output_user_root=" + root_dir, "--output_base=" + out_dir
        ]

        if is_native_windows_or_msys():
            bazel_flags.append("--enable_runfiles=false")

    bazel_targets = []
    bazel_targets += ["//:ray_pkg"] if build_python else []
    bazel_targets += ["//cpp:ray_cpp_pkg"] if build_cpp else []
    bazel_targets += ["//java:ray_java_pkg"] if build_java else []

    if setup_spec.build_type == BuildType.DEBUG:
        bazel_flags.extend(["--config", "debug"])
    if setup_spec.build_type == BuildType.ASAN:
        bazel_flags.extend(["--config=asan-build"])
    if setup_spec.build_type == BuildType.TSAN:
        bazel_flags.extend(["--config=tsan"])

    return bazel_invoke(
        subprocess.check_call,
        bazel_precmd_flags + ["build"] + bazel_flags + ["--"] + bazel_targets,
        env=bazel_env)


def walk_directory(directory):
    file_list = []
    for (root, dirs, filenames) in os.walk(directory):
        for name in filenames:
            file_list.append(os.path.join(root, name))
    return file_list


def copy_file(target_dir, filename, rootdir):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    # File names can be absolute paths, e.g. from walk_directory().
    source = os.path.relpath(filename, rootdir)
    destination = os.path.join(target_dir, source)
    # Create the target directory if it doesn't already exist.
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if not os.path.exists(destination):
        if sys.platform == "win32":
            # Does not preserve file mode (needed to avoid read-only bit)
            shutil.copyfile(source, destination, follow_symlinks=True)
        else:
            # Preserves file mode (needed to copy executable bit)
            shutil.copy(source, destination, follow_symlinks=True)
        return 1
    return 0


def add_system_dlls(dlls, target_dir):
    """
    Copy any required dlls required by the c-extension module and not already
    provided by python. They will end up in the wheel next to the c-extension
    module which will guarentee they are available at runtime.
    """
    for dll in dlls:
        # Installing Visual Studio will copy the runtime dlls to system32
        src = os.path.join(r"c:\Windows\system32", dll)
        assert os.path.exists(src)
        shutil.copy(src, target_dir)


def pip_run(build_ext):
    if SKIP_BAZEL_BUILD:
        build(False, False, False)
    else:
        build(True, BUILD_JAVA, True)

    if setup_spec.type == SetupType.RAY:
        setup_spec.files_to_include += ray_files
        # We also need to install pickle5 along with Ray, so make sure that the
        # relevant non-Python pickle5 files get copied.
        pickle5_dir = os.path.join(ROOT_DIR, PICKLE5_SUBDIR)
        setup_spec.files_to_include += walk_directory(
            os.path.join(pickle5_dir, "pickle5"))

        thirdparty_dir = os.path.join(ROOT_DIR, THIRDPARTY_SUBDIR)
        setup_spec.files_to_include += walk_directory(thirdparty_dir)

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    setup_spec.files_to_include.append(
                        os.path.join(directory, filename))

    copied_files = 0
    for filename in setup_spec.files_to_include:
        copied_files += copy_file(build_ext.build_lib, filename, ROOT_DIR)
    if sys.platform == "win32":
        # _raylet.pyd links to some MSVC runtime DLLS, this one may not be
        # present on a user's machine. While vcruntime140.dll and
        # vcruntime140_1.dll are also required, they are provided by CPython.
        runtime_dlls = ["msvcp140.dll"]
        add_system_dlls(runtime_dlls, os.path.join(build_ext.build_lib, "ray"))
        copied_files += len(runtime_dlls)
    print("# of files copied to {}: {}".format(build_ext.build_lib,
                                               copied_files))


def api_main(program, *args):
    parser = argparse.ArgumentParser()
    choices = ["build", "bazel_version", "python_versions", "clean", "help"]
    parser.add_argument("command", type=str, choices=choices)
    parser.add_argument(
        "-l",
        "--language",
        default="python,cpp",
        type=str,
        help="A list of languages to build native libraries. "
        "Supported languages include \"python\" and \"java\". "
        "If not specified, only the Python library will be built.")
    parsed_args = parser.parse_args(args)

    result = None

    if parsed_args.command == "build":
        kwargs = dict(build_python=False, build_java=False, build_cpp=False)
        for lang in parsed_args.language.split(","):
            if "python" in lang:
                kwargs.update(build_python=True)
            elif "java" in lang:
                kwargs.update(build_java=True)
            elif "cpp" in lang:
                kwargs.update(build_cpp=True)
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


# Ensure no remaining lib files.
build_dir = os.path.join(ROOT_DIR, "build")
if os.path.isdir(build_dir):
    shutil.rmtree(build_dir)

setuptools.setup(
    name=setup_spec.name,
    version=setup_spec.version,
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=(setup_spec.description),
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
    packages=setup_spec.get_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=setup_spec.install_requires,
    setup_requires=["cython >= 0.29.26", "wheel"],
    extras_require=setup_spec.extras,
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
