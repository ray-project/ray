load("//bazel:ray.bzl", "ray_cc_library", "ray_cc_test")

package(default_visibility = ["//visibility:public"])

ray_cc_library(
    name = "type_printer",
    hdrs = ["type_printer.h"],
    deps = [
        ":get_type_name",
        "@boost//:core",
    ],
)

ray_cc_library(
    name = "std_printer",
    hdrs = ["std_printer.h"],
    deps = [
        ":ostream_printer",
    ],
)

ray_cc_test(
    name = "std_printer_test",
    srcs = ["std_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":std_printer",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "debug_string_printer",
    hdrs = ["debug_string_printer.h"],
)

ray_cc_test(
    name = "debug_string_printer_test",
    srcs = ["debug_string_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":debug_string_printer",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "absl_stringify_printer",
    hdrs = ["absl_stringify_printer.h"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_test(
    name = "absl_stringify_printer_test",
    srcs = ["absl_stringify_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":absl_stringify_printer",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "container_printer",
    hdrs = ["container_printer.h"],
    deps = [
        ":ostream_printer",
        "//src/ray/util:type_traits",
    ],
)

ray_cc_test(
    name = "container_printer_test",
    srcs = ["container_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":container_printer",
        "//src/ray/util:type_traits",
        "//src/ray/util:visitor",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "ostream_printer",
    hdrs = ["ostream_printer.h"],
)

ray_cc_test(
    name = "ostream_printer_test",
    srcs = ["ostream_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":ostream_printer",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "enum_printer",
    hdrs = ["enum_printer.h"],
)

ray_cc_test(
    name = "enum_printer_test",
    srcs = ["enum_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":enum_printer",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "debug_printer",
    hdrs = ["debug_printer.h"],
    deps = [
        ":absl_stringify_printer",
        ":container_printer",
        ":debug_string_printer",
        ":enum_printer",
        ":ostream_printer",
        ":std_printer",
        ":type_printer",
        "//src/ray/util:visitor",
    ],
)

ray_cc_test(
    name = "debug_printer_test",
    srcs = ["debug_printer_test.cc"],
    tags = ["team:core"],
    deps = [
        ":debug_printer",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "get_type_name",
    hdrs = ["get_type_name.h"],
)

ray_cc_test(
    name = "get_type_name_test",
    srcs = ["get_type_name_test.cc"],
    tags = ["team:core"],
    deps = [
        ":get_type_name",
        "@com_google_googletest//:gtest_main",
    ],
)
