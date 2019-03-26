load("//java/third_party:workspace.bzl", "maven_dependencies")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_jar")

def create_if_needed(rule, name, **kwargs):
    if name not in native.existing_rules():
        rule(name = name, **kwargs)

def bazel_deps():
    create_if_needed(
        http_jar,
        name = "bazel_deps",
        sha256 = "98b05c2826f2248f70e7356dc6c78bc52395904bb932fbb409a5abf5416e4292",
        urls = ["https://github.com/oferb/startupos-binaries/releases/download/0.1.01/bazel_deps.jar"],
    )

def java_repositories():
    maven_dependencies()
    bazel_deps()
