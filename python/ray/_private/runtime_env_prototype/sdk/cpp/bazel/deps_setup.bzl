load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def deps_setup():

    http_archive(
        name = "nlohmann_json",
        strip_prefix = "json-3.9.1",
        urls = ["https://github.com/nlohmann/json/archive/v3.9.1.tar.gz"],
        sha256 = "4cf0df69731494668bdd6460ed8cb269b68de9c19ad8c27abc24cd72605b2d5b",
        build_file = "@com_github_runtime_env//bazel:BUILD.nlohmann_json",
    )

    http_archive(
        name = "json_schema_validator",
        strip_prefix = "json-schema-validator-2.1.0",
        urls = ["https://github.com/pboettch/json-schema-validator/archive/refs/tags/2.1.0.tar.gz"],
        sha256 = "83f61d8112f485e0d3f1e72d51610ba3924b179926a8376aef3c038770faf202",
        build_file = "@com_github_runtime_env//bazel:BUILD.json_schema_validator",
    )
