load("@rules_cc//cc:defs.bzl", "cc_library")

COPTS = ["-DSPDLOG_COMPILED_LIB"]

cc_library(
    name = "spdlog",
    srcs = glob([
        "src/*.cpp",
    ]),
    hdrs = glob([
        "include/spdlog/*.h",
        "include/spdlog/cfg/*.h",
        "include/spdlog/details/*.h",
        "include/spdlog/fmt/*.h",
        "include/spdlog/fmt/bundled/*.h",
        "include/spdlog/sinks/*.h",
    ]),
    copts = COPTS,
    includes = [
        "include/",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
    ],
)
