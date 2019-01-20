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
    remote = "https://github.com/ray-project/flatbuffers.git",
    commit = "4ae340b698b7dd768e8c5a155731c8dee9fce016"
)

git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest",
    commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
)

new_git_repository(
    name = "plasma",
    build_file = "@//bazel:BUILD.plasma",
    remote = "https://github.com/pcmoritz/arrow",
    commit = "27bf57e5519086b0374f203de0051479d966e78b"
)
