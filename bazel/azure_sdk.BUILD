# Builds the azure-core and azure-identity components of azure-sdk-for-cpp with
# CMake (via rules_foreign_cc). azure-identity supplies the Entra ID credential
# classes (managed identity, workload identity federation, environment
# credentials, ...) used to authenticate Ray's GCS connection to an external
# Redis; azure-core provides the runtime and libcurl-based HTTP transport it
# builds on.
#
# NOTE: azure-sdk-for-cpp normally resolves its dependencies through vcpkg. Here
# we instead supply libcurl and OpenSSL as Bazel-built deps and let CMake's
# find_package() locate them. The cache entries below may need tuning during CI
# bring-up (transport flags, dependency discovery).

load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cmake(
    name = "azure-identity",
    build_args = [
        "--",
        "-j4",
    ],
    cache_entries = {
        # Only build the runtime libraries, not tests/samples.
        "BUILD_TESTING": "OFF",
        "BUILD_SAMPLES": "OFF",
        # azure-sdk-for-cpp treats warnings as errors by default; disable so a
        # newer toolchain than the SDK was validated against does not break us.
        "WARNINGS_AS_ERRORS": "OFF",
        # Use the libcurl transport (the default on Linux/macOS) rather than
        # WinHTTP, and build it into azure-core.
        "BUILD_TRANSPORT_CURL": "ON",
        "CMAKE_BUILD_TYPE": "Release",
    },
    lib_source = ":all_srcs",
    # azure-identity depends on azure-core; build and install both.
    out_static_libs = [
        "libazure-identity.a",
        "libazure-core.a",
    ],
    targets = [
        "azure-core",
        "azure-identity",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_curl_curl//:curl",
        "@openssl//:openssl",
    ],
)
