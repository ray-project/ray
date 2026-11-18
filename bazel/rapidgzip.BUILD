# rapidgzip, from librapidarchive: a header-only C++ library that decodes one
# gzip stream on many cores. Ray Sandbox's erofs-pull uses it, through its own
# small C API (//src/ray/sandbox/erofs_pull:gzip_shim). Built without ISA-L
# and rpmalloc, so only zlib is needed.
cc_library(
    name = "rapidgzip",
    hdrs = glob([
        # Precomputed lookup tables, #included by the headers next to them.
        "src/rapidgzip/**/*.csv",
        "src/core/**/*.hpp",
        "src/filereader/**/*.hpp",
        "src/huffman/**/*.hpp",
        # rapidgzip decodes bzip2 chunks too, with indexed_bzip2.
        "src/indexed_bzip2/**/*.hpp",
        "src/rapidgzip/**/*.hpp",
    ]),
    includes = ["src"],
    visibility = ["@io_ray//src/ray/sandbox/erofs_pull:__pkg__"],
    deps = ["@zlib"],
)
