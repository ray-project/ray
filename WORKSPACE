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

rust_repositories(version = "nightly", iso_date = "2021-06-16", edition="2018")

load("//bazel:crate_universe_defaults.bzl", "DEFAULT_URL_TEMPLATE", "DEFAULT_SHA256_CHECKSUMS")

load("@rules_rust//crate_universe:defs.bzl", "crate", "crate_universe")

crate_universe(
    name = "ray-rs-toml",
    cargo_toml_files = [
        "//rust/ray-rs-sys:Cargo.toml",
        "//rust/ray-rs:Cargo.toml",
        # "//rust/ray-rs:Cargo.toml",
    ],
    resolver_download_url_template = DEFAULT_URL_TEMPLATE,
    resolver_sha256s = DEFAULT_SHA256_CHECKSUMS,
    # leave unset for default multi-platform support
    supported_targets = [
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
    ],
)

load("@ray-rs-toml//:defs.bzl", "pinned_rust_install")

pinned_rust_install()

load("@rules_rust//bindgen:repositories.bzl", "rust_bindgen_repositories")

rust_bindgen_repositories()

# When the bazel version is updated, make sure to update it
# in setup.py as wellrustc
versions.check(minimum_bazel_version = "4.2.1")
