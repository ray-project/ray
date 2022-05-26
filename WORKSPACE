workspace(name = "com_github_ray_project_ray")

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

# When the bazel version is updated, make sure to update it
# in setup.py as well.
versions.check(minimum_bazel_version = "4.2.1")

# Tools to generate `compile_commands.json` to enable awesome tooling of the C language family.
# Just run `bazel run @hedron_compile_commands//:refresh_all`
load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()
