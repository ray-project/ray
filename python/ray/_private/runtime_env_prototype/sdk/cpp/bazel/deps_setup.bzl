load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def deps_setup():

    http_archive(
        name = "nlohmann_json",
        strip_prefix = "json-3.9.1",
        urls = ["https://github.com/nlohmann/json/archive/v3.9.1.tar.gz"],
        sha256 = "4cf0df69731494668bdd6460ed8cb269b68de9c19ad8c27abc24cd72605b2d5b",
        build_file = "@com_github_runtime_env//bazel:BUILD.nlohmann_json",
    )
