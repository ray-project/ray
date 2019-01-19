load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_cc_library")

cc_library(
    name = "plasma",
    visibility = ["//visibility:public"],
    hdrs = [
        "cpp/src/plasma/client.h",
        "cpp/src/plasma/common.h",
        "cpp/src/plasma/compat.h",
        "cpp/src/plasma/events.h",
        "cpp/src/arrow/buffer.h",
        "cpp/src/arrow/memory_pool.h",
        "cpp/src/arrow/status.h",
        "cpp/src/arrow/util/macros.h",
        "cpp/src/arrow/util/visibility.h",
        "cpp/src/arrow/util/string_builder.h",
        # private headers
        "cpp/src/plasma/common_generated.h",
        "cpp/src/plasma/plasma_generated.h",
        "cpp/src/plasma/eviction_policy.h",
        "cpp/src/plasma/fling.h",
        "cpp/src/plasma/io.h",
        "cpp/src/plasma/malloc.h",
        "cpp/src/plasma/plasma.h",
        "cpp/src/plasma/protocol.h",
        "cpp/src/plasma/store.h",
        "cpp/src/plasma/thirdparty/dlmalloc.c",
        "cpp/src/plasma/thirdparty/ae/ae.h",
        "cpp/src/arrow/io/interfaces.h",
        "cpp/src/arrow/util/io-util.h",
        "cpp/src/arrow/util/logging.h",
        "cpp/src/arrow/util/string_view.h",
        "cpp/src/arrow/vendored/string_view.hpp",
        "cpp/src/arrow/util/thread-pool.h",
        "cpp/src/arrow/vendored/xxhash/xxhash.h",
        "cpp/src/arrow/vendored/xxhash/xxhash.c",
        "cpp/src/arrow/util/windows_compatibility.h"
    ],
    srcs = glob([
      "cpp/src/plasma/*.cc",
      "cpp/src/plasma/thirdparty/ae/ae.c",
      "cpp/src/arrow/buffer.cc",
      "cpp/src/arrow/memory_pool.cc",
      "cpp/src/arrow/status.cc",
      "cpp/src/arrow/util/logging.cc",
      "cpp/src/arrow/util/thread-pool.cc",
      "cpp/src/arrow/util/io-util.cc",
    ]),
    deps = [":common_fbs", ":plasma_fbs"],
    strip_include_prefix = "cpp/src",
)

FLATC_ARGS = [
    "--gen-object-api",
    "--gen-mutable",
    "--scoped-enums",
]

flatbuffer_cc_library(
    name="common_fbs",
    srcs=["cpp/src/plasma/format/common.fbs"],
    flatc_args=FLATC_ARGS,
    out_prefix="cpp/src/plasma/"
)

flatbuffer_cc_library(
    name="plasma_fbs",
    srcs=["cpp/src/plasma/format/plasma.fbs"],
    flatc_args=FLATC_ARGS,
    out_prefix="cpp/src/plasma/",
    includes = ["cpp/src/plasma/format/common.fbs"]
)

exports_files(["cpp/src/plasma/format/common.fbs"])
