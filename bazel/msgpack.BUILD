filegroup(
    name = "msgpack_hdrs",
    srcs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    visibility = ["//visibility:public"],
)

# This library is for internal use, because the library assumes a
# different include prefix for itself than external libraries do.
cc_library(
    name = "_msgpack",
    hdrs = [":msgpack_hdrs"],
    strip_include_prefix = "include",
)

cc_library(
    name = "msgpack",
    srcs = [
        "src/objectc.c",
        "src/unpack.c",
        "src/version.c",
        "src/vrefbuffer.c",
        "src/zone.c",
    ],
    hdrs = [
        "include/msgpack.h",
        "include/msgpack.hpp",
    ],
    includes = [
        "include/",
    ],
    strip_include_prefix = "include",
    copts = select({
        "@platforms//os:windows": [],
        # Ray doesn't control third-party libraries' implementation, simply ignores certain warning errors.
        "//conditions:default": [
            "-Wno-shadow",
        ],
    }),
    deps = [
        ":_msgpack",
    ],
    visibility = ["//visibility:public"],
)
