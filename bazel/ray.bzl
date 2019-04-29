load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_library_public")
load("@com_github_checkstyle_java//checkstyle:checkstyle.bzl", "checkstyle_test")
load("@bazel_common//tools/maven:pom_file.bzl", "pom_file")

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

def flatbuffer_java_library(name, srcs, outs, out_prefix, includes = [], include_paths = []):
    flatbuffer_library_public(
        name = name,
        srcs = srcs,
        outs = outs,
        language_flag = "-j",
        out_prefix = out_prefix,
        include_paths = include_paths,
        includes = includes,
    )

def define_java_module(name, additional_srcs = [], additional_resources = [], define_test_lib = False, test_deps = [], **kwargs):
    native.java_library(
        name = "org_ray_ray_" + name,
        srcs = additional_srcs + native.glob([name + "/src/main/java/**/*.java"]),
        resources = native.glob([name + "/src/main/resources/**"]) + additional_resources,
        **kwargs
    )
    checkstyle_test(
        name = "org_ray_ray_" + name + "-checkstyle",
        target = "//java:org_ray_ray_" + name,
        config = "//java:checkstyle.xml",
        suppressions = "//java:checkstyle-suppressions.xml",
        size = "small",
        tags = ["checkstyle"],
    )
    if define_test_lib:
        native.java_library(
            name = "org_ray_ray_" + name + "_test",
            srcs = native.glob([name + "/src/test/java/**/*.java"]),
            deps = test_deps,
        )
        checkstyle_test(
            name = "org_ray_ray_" + name + "_test-checkstyle",
            target = "//java:org_ray_ray_" + name + "_test",
            config = "//java:checkstyle.xml",
            suppressions = "//java:checkstyle-suppressions.xml",
            size = "small",
            tags = ["checkstyle"],
        )

def gen_java_pom_file(name):
    pom_file(
        name = "org_ray_ray_" + name + "_pom",
        targets = [
            ":org_ray_ray_" + name,
        ],
        template_file = name + "/pom_template.xml",
    )
