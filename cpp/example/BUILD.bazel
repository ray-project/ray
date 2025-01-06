cc_binary(
    name = "example",
    srcs = ["example.cc"],
    data = [
        "//cpp:libray_api.so",
    ],
    deps = [
        "//cpp:ray_api_lib",
    ],
    linkstatic = True,
)
