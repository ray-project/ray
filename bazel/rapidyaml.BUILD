filegroup(
    name = "rapidyaml_hdrs",
    srcs = glob([
        "src/**/*.hpp",
    ]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "rapidyaml",
    hdrs = [":rapidyaml_hdrs"],
    includes = ["src"],
    visibility = ["//visibility:public"],
    alwayslink = 1,
)