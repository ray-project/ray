load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "6d6fd834281cb8f8e758dd9ad76df86304bf1869",
    remote = "https://github.com/nelhage/rules_boost",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()

git_repository(
    name = "com_github_google_flatbuffers",
    remote = "https://github.com/google/flatbuffers.git",
    commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
)

git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest",
    commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
)

new_git_repository(
    name = "plasma",
    build_file = "@//bazel:BUILD.plasma",
    remote = "https://github.com/ray-project/arrow",
    commit = "f5d1be2fed69899aea636bd074aaeaa4149acc79",
)

new_git_repository(
    name="cython",
    build_file="@//bazel:BUILD.cython",
    remote = "https://github.com/cython/cython",
    commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
)

load("@//bazel:python_configure.bzl", "python_configure")
python_configure(name="local_config_python")
