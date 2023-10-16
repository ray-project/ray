workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
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

# TODO (shrekris-anyscale): Update the min version to 4.2.2 once Windows uses
# it in CI.

# Please keep this in sync with the .bazeliskrc file.
versions.check(minimum_bazel_version = "5.4.1")

# Tools to generate `compile_commands.json` to enable awesome tooling of the C language family.
# Just run `bazel run @hedron_compile_commands//:refresh_all`
load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

http_archive(
    name = "rules_python",
    sha256 = "94750828b18044533e98a129003b6a68001204038dc4749f40b195b24c38f49f",
    strip_prefix = "rules_python-0.21.0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.21.0/rules_python-0.21.0.tar.gz",
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

pip_parse(
    name = "py_deps_ray_ci",
    python_interpreter_target = python39,
    requirements_lock = "//ci/ray_ci:requirements.txt",
)

pip_parse(
    name = "py_deps_compile_py_proto",
    python_interpreter_target = python39,
    requirements_lock = "//ci/compile_py_proto:requirements_compile_py_proto.txt",
)

load("@py_deps_buildkite//:requirements.bzl", install_py_deps_buildkite = "install_deps")
load("@py_deps_ray_ci//:requirements.bzl", install_py_deps_ray_ci = "install_deps")
load("@py_deps_compile_py_proto//:requirements.bzl", install_py_deps_compile_py_proto = "install_deps")

install_py_deps_buildkite()

install_py_deps_ray_ci()

install_py_deps_compile_py_proto()

register_toolchains("//:python_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//:hermetic_python_platform",
)
