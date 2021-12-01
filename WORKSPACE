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

versions.check(minimum_bazel_version = "3.4.0")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_rust",
    sha256 = "4d6aa4554eaf5c7bf6da1dd1371b1455b3234676b234a299791635c50c61df91",
    strip_prefix = "rules_rust-238b998f108a099e5a227dbe312526406dda1f2d",
    urls = [
        # Main branch as of 2021-10-01
        "https://github.com/bazelbuild/rules_rust/archive/238b998f108a099e5a227dbe312526406dda1f2d.tar.gz",
    ],
)

load("@rules_rust//rust:repositories.bzl", "rust_repositories")

rust_repositories()

http_archive(
    name = "cxx.rs",
    strip_prefix = "cxx-master",
    sha256 = "5cfef94527cd1a0326a3e7a9c92671768c2dc02e336c3eee55f37cd445ca5560",
    urls = [
        "https://github.com/jon-chuang/cxx/archive/refs/heads/master.zip",
    ]
)

load("@cxx.rs//tools/bazel:vendor.bzl", "vendor")

RUST_VERSION = "1.55.0"

vendor(
    name = "third-party",
    lockfile = "@cxx.rs//third-party:Cargo.lock",
    cargo_version = RUST_VERSION,
)
