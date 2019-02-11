load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_library_public")

def flatbuffer_py_library(name, srcs, outs, out_prefix, includes = [], include_paths = []):
    flatbuffer_library_public(
        name = name,
        srcs = srcs,
        outs = outs,
        language_flag = "-p",
        out_prefix = out_prefix,
        include_paths = include_paths,
        includes = includes,
    )
