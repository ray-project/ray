workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("//java:repo.bzl", "java_repositories")

java_repositories()

git_repository(
    name = "com_github_checkstyle_java",
    commit = "85f37871ca03b9d3fee63c69c8107f167e24e77b",
    remote = "https://github.com/ruifangChen/checkstyle_java",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "6d6fd834281cb8f8e758dd9ad76df86304bf1869",
    remote = "https://github.com/nelhage/rules_boost",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()

git_repository(
    name = "com_github_google_flatbuffers",
    commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
    remote = "https://github.com/google/flatbuffers.git",
)

git_repository(
    name = "com_google_googletest",
    commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
    remote = "https://github.com/google/googletest",
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    tag = "v2.2.2",
)

new_git_repository(
    name = "com_github_google_glog",
    build_file = "@//bazel:BUILD.glog",
    commit = "5c576f78c49b28d89b23fbb1fc80f54c879ec02e",
    remote = "https://github.com/google/glog",
)

new_git_repository(
    name = "plasma",
    build_file = "@//bazel:BUILD.plasma",
    commit = "d00497b38be84fd77c40cbf77f3422f2a81c44f9",
    remote = "https://github.com/apache/arrow",
)

new_git_repository(
    name = "cython",
    build_file = "@//bazel:BUILD.cython",
    commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
    remote = "https://github.com/cython/cython",
)

load("@//bazel:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

http_archive(
    name = "io_opencensus_cpp",
    strip_prefix = "opencensus-cpp-0.3.0",
    urls = ["https://github.com/census-instrumentation/opencensus-cpp/archive/v0.3.0.zip"],
)

# OpenCensus depends on Abseil so we have to explicitly to pull it in.
# This is how diamond dependencies are prevented.
git_repository(
    name = "com_google_absl",
    commit = "e75672f6afc7e8f23ee7b532e86d1b3b9be3984e",
    remote = "https://github.com/abseil/abseil-cpp.git",
)

# OpenCensus depends on jupp0r/prometheus-cpp
http_archive(
    name = "com_github_jupp0r_prometheus_cpp",
    strip_prefix = "prometheus-cpp-master",

    # TODO(qwang): We should use the repository of `jupp0r` here when this PR
    # `https://github.com/jupp0r/prometheus-cpp/pull/225` getting merged.
    urls = ["https://github.com/jovany-wang/prometheus-cpp/archive/master.zip"],
)

load("@com_github_jupp0r_prometheus_cpp//:repositories.bzl", "prometheus_cpp_repositories")

prometheus_cpp_repositories()
