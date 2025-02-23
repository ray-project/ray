load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "license",
    srcs = ["license.txt"],
)

cc_library(
    name = "rapidjson",
    hdrs = glob([
        "include/rapidjson/*.h",
        "include/rapidjson/*/*.h",
    ]),
    copts = [
        "-Wno-non-virtual-dtor",
        "-Wno-unused-variable",
        "-Wno-implicit-fallthrough",
    ],
    includes = ["include"],
)
