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
    maximum_bazel_version = "6.5.0",
    minimum_bazel_version = "6.5.0",
)

load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

http_archive(
    name = "rules_python",
    sha256 = "c68bdc4fbec25de5b5493b8819cfc877c4ea299c0dcb15c244c5a00208cde311",
    strip_prefix = "rules_python-0.31.0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.31.0/rules_python-0.31.0.tar.gz",
)

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python3_9",
    python_version = "3.9",
    register_toolchains = False,
)

load("@python3_9//:defs.bzl", python39 = "interpreter")
load("@rules_python//python/pip_install:repositories.bzl", "pip_install_dependencies")

pip_install_dependencies()

load("@rules_python//python:pip.bzl", "pip_parse")

pip_parse(
    name = "py_deps_buildkite",
    python_interpreter_target = python39,
    requirements_lock = "//release:requirements_buildkite.txt",
)

load("@py_deps_buildkite//:requirements.bzl", install_py_deps_buildkite = "install_deps")

install_py_deps_buildkite()

register_toolchains("//bazel:py39_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//:hermetic_python_platform",
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
    sha256 = "920cbcaad514cc185634f6f0dcd71df5e8f4ee4456d440a22e0f8c0f142a8203",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.8.17/uv-x86_64-unknown-linux-gnu.tar.gz"],
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
    sha256 = "e4d4859d7726298daa4c12e114f269ff282b2cfc2b415dc0b2ca44ae2dbd358e",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.8.17/uv-aarch64-apple-darwin.tar.gz"],
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
