filegroup(
    name = "nlohmann_json_hdrs",
    srcs = glob([
        "single_include/**/*.hpp",
    ]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "nlohmann_json",
    hdrs = [":nlohmann_json_hdrs"],
    includes = ["single_include"],
    visibility = ["//visibility:public"],
    alwayslink = 1,
)
