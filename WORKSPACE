workspace(name = "io_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "platforms",
    sha256 = "5eda539c841265031c2f82d8ae7a3a6490bd62176e0c038fc469eabf91f6149b",
    urls = [
        "https://github.com/bazelbuild/platforms/releases/download/0.0.9/platforms-0.0.9.tar.gz",
    ],
)

http_archive(
    name = "rules_java",
    sha256 = "302bcd9592377bf9befc8e41aa97ec02df12813d47af9979e4764f3ffdcc5da8",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/7.12.4/rules_java-7.12.4.tar.gz",
    ],
)

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup()

load("//bazel:ray_deps_build_all.bzl", "ray_deps_build_all")

ray_deps_build_all()

# This needs to be run after grpc_deps() in ray_deps_build_all() to make
# sure all the packages loaded by grpc_deps() are available. However a
# load() statement cannot be in a function so we put it here.
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@bazel_skylib//lib:versions.bzl", "versions")

# Please keep this in sync with the .bazelversion file.
versions.check(
    maximum_bazel_version = "7.5.0",
    minimum_bazel_version = "7.5.0",
)

load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python3_10",
    python_version = "3.10",
    register_toolchains = False,
)

load("@python3_10//:defs.bzl", python310 = "interpreter")
load("@rules_python//python/pip_install:repositories.bzl", "pip_install_dependencies")

# The pip that whl_library shells out to, overridden ahead of the one rules_python
# brings.
#
# rules_python arrives here transitively -- protobuf's protobuf_deps() declares it,
# pinned at 0.14.0 -- and that version bundles pip 22.2.1, one release before pip could
# read a PEP 691 JSON simple index. A JSON index page is therefore skipped rather than
# parsed, and the resolve fails as though the package did not exist:
#
#   Skipping page http://.../simple/pygments/ because the GET request got Content-Type:
#   application/vnd.pypi.simple.v1+json. The only supported Content-Type is text/html
#   ERROR: Could not find a version that satisfies the requirement pygments==2.16.1
#   (from versions: none)
#
# Which is what any index whose pages are not HTML produces here. The CI mirror caches
# an index page keyed on URL while PyPI answers `Vary: Accept`, so whichever client
# warms an entry picks the representation every later reader gets -- and a pip this old
# can only read one of the two. A pip that understands JSON understands HTML as well, so
# moving it forward makes the representation stop mattering in either direction, rather
# than depending on every producer asking for the same one.
#
# Declared before pip_install_dependencies() deliberately: that function declares its
# deps through maybe(), which skips any repository that already exists, so this wins.
# The build file has to keep the `lib` target name, because rules_python resolves these
# as @pypi__pip//:lib.
#
# 23.3.2 rather than the newest: it is comfortably past 22.3, and pairing a 2022-era
# whl_library with a much later pip CLI buys drift for no benefit here.
http_archive(
    name = "pypi__pip",
    build_file_content = """\
package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "lib",
    srcs = glob(["**/*.py"]),
    data = glob(["**/*"], exclude = [
        "**/*.py",
        "**/*.pyc",
        "**/* *",
        "**/*.dist-info/RECORD",
        "BUILD",
        "WORKSPACE",
    ]),
    imports = ["."],
)
""",
    sha256 = "5052d7889c1f9d05224cd41741acb7c5d6fa735ab34e339624a614eaaa7e7d76",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/15/aa/3f4c7bcee2057a76562a5b33ecbd199be08cdb4443a02e26bd2c3cf6fc39/pip-23.3.2-py3-none-any.whl",
)

pip_install_dependencies()

load("@rules_python//python:pip.bzl", "pip_parse")

# For CI scripts use only; not for ray testing.
pip_parse(
    name = "py_deps_py310",
    python_interpreter_target = python310,
    requirements_lock = "//release:requirements_py310.txt",
)

load("@py_deps_py310//:requirements.bzl", install_py_deps_py310 = "install_deps")

install_py_deps_py310()

register_toolchains("//bazel:py310_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//bazel:py310_platform",
)

http_archive(
    name = "crane_linux_x86_64",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "daa629648e1d1d10fc8bde5e6ce4176cbc0cd48a32211b28c3fd806e0fa5f29b",
    urls = ["https://github.com/google/go-containerregistry/releases/download/v0.19.0/go-containerregistry_Linux_x86_64.tar.gz"],
)

http_archive(
    name = "registry_x86_64",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "61c9a2c0d5981a78482025b6b69728521fbc78506d68b223d4a2eb825de5ca3d",
    urls = ["https://github.com/distribution/distribution/releases/download/v3.0.0/registry_3.0.0_linux_amd64.tar.gz"],
)

http_archive(
    name = "uv_x86_64-linux",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "30ccbf0a66dc8727a02b0e245c583ee970bdafecf3a443c1686e1b30ec4939e8",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.9.26/uv-x86_64-unknown-linux-gnu.tar.gz"],
)

http_archive(
    name = "uv_aarch64-darwin",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "fcf0a9ea6599c6ae28a4c854ac6da76f2c889354d7c36ce136ef071f7ab9721f",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.9.26/uv-aarch64-apple-darwin.tar.gz"],
)

http_archive(
    name = "com_github_storypku_bazel_iwyu",
    sha256 = "aa78c331a2cb139f73f7d74eeb4d5ab29794af82023ef5d6d5194f76b7d37449",
    strip_prefix = "bazel_iwyu-0.19.2",
    urls = [
        "https://github.com/storypku/bazel_iwyu/archive/0.19.2.tar.gz",
    ],
)

http_archive(
    name = "redis_linux_x86_64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "4ae33c10059ed52202a12929d269deea46fac81b8e02e722d30cb22ceb3ed678",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-linux-x86_64.tar.gz"],
)

http_archive(
    name = "redis_linux_arm64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "2d1085a4f69477e1f44cbddd531e593f0712532b1ade9beab0b221a0cb01f298",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-linux-arm64.tar.gz"],
)

http_archive(
    name = "redis_osx_arm64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "74b76099c3600b538252cdd1731278e087e8e85eecc6c64318c860f3e9462506",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-osx-arm64.tar.gz"],
)

load("@com_github_storypku_bazel_iwyu//bazel:dependencies.bzl", "bazel_iwyu_dependencies")

bazel_iwyu_dependencies()
