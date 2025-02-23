load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "nlohmann_json",
    hdrs = glob([
        "single_include/**/*.hpp",
    ]),
    includes = ["single_include"],
    visibility = ["//visibility:public"],
    alwayslink = 1,
)
