workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup()

# In wasm worker, we used rust. So we need to load rust rules.
load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies", "rust_register_toolchains")

rules_rust_dependencies()
rust_register_toolchains(edition = "2021")

load("@rules_rust//crate_universe:defs.bzl", "crate", "crates_repository", "render_config")

crates_repository(
    name = "wasm_crate_index",
    cargo_lockfile = "//wasm:Cargo.lock",
    lockfile = "//wasm:Cargo.Bazel.lock",
    manifests = ["//wasm:Cargo.toml"],
    quiet = False,
    render_config = render_config(
        default_package_name = ""
    ),
    annotations = {
        "wasmedge-sys": [crate.annotation(
            build_script_data = [
                "@//wasm:wasmedge_pkg",
            ],
            build_script_env = {
                "WASMEDGE_INCLUDE_DIR": "$${pwd}/../../../../../execroot/com_github_ray_project_ray/python/ray/wasm/wasmedge/include",
                "WASMEDGE_LIB_DIR": "$${pwd}/../../../../../execroot/com_github_ray_project_ray/python/ray/wasm/wasmedge/lib64",
            },
        )],
    },
)

load("@wasm_crate_index//:defs.bzl", "crate_repositories")

crate_repositories()

load("@rules_rust//proto:repositories.bzl", "rust_proto_repositories")

# protobuf support in rust
rust_proto_repositories()

load("@rules_rust//proto:transitive_repositories.bzl", "rust_proto_transitive_repositories")

rust_proto_transitive_repositories()

# build all dependencies
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

# When the bazel version is updated, make sure to update it
# in setup.py as well.
versions.check(minimum_bazel_version = "5.4.0")

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

load("@python3_9//:defs.bzl", bk_python = "interpreter")
load("@rules_python//python/pip_install:repositories.bzl", "pip_install_dependencies")

pip_install_dependencies()

load("@rules_python//python:pip.bzl", "pip_parse")

pip_parse(
    name = "py_deps_buildkite",
    python_interpreter_target = bk_python,
    requirements_lock = "//release:requirements_buildkite.txt",
)

load("@py_deps_buildkite//:requirements.bzl", install_py_deps_buildkite = "install_deps")

install_py_deps_buildkite()

register_toolchains("//release:python_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//release:hermetic_python_platform",
)
