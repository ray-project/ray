cc_library(
    name = "blake3_sse2",
    srcs = ["c/blake3_sse2.c"],
    hdrs = [
        "c/blake3.h",
        "c/blake3_impl.h",
    ],
    copts = ["-msse2"],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "blake3_sse41",
    srcs = ["c/blake3_sse41.c"],
    hdrs = [
        "c/blake3.h",
        "c/blake3_impl.h",
    ],
    copts = ["-msse4.1"],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "blake3_avx2",
    srcs = ["c/blake3_avx2.c"],
    hdrs = [
        "c/blake3.h",
        "c/blake3_impl.h",
    ],
    copts = ["-mavx2"],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "blake3_avx512",
    srcs = ["c/blake3_avx512.c"],
    hdrs = [
        "c/blake3.h",
        "c/blake3_impl.h",
    ],
    copts = ["-mavx512f", "-mavx512vl"],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "blake3",
    srcs = [
        "c/blake3.c",
        "c/blake3_dispatch.c",
        "c/blake3_portable.c",
    ],
    hdrs = [
        "c/blake3.h",
        "c/blake3_impl.h",
    ],
    deps = [
        ":blake3_sse2",
        ":blake3_sse41",
        ":blake3_avx2",
        ":blake3_avx512",
    ],
    visibility=["//visibility:public"],
)
