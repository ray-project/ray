load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

def ray_go_testplugin(name, out, importpath, deps):
    """Generates a go_library + go_binary(linkmode = "plugin") pair for a test plugin.

    All test plugins under testplugins/ share the same build shape: a private
    go_library compiling main.go plus a public plugin-mode go_binary. This macro
    keeps the per-plugin BUILD.bazel files free of duplication.
    """
    lib_name = name + "_lib"
    go_library(
        name = lib_name,
        srcs = ["main.go"],
        importpath = importpath,
        visibility = ["//visibility:private"],
        deps = deps,
    )
    go_binary(
        name = name,
        out = out,
        embed = [":" + lib_name],
        linkmode = "plugin",
        visibility = ["//visibility:public"],
    )
