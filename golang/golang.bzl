load("@bazel_skylib//rules:copy_file.bzl", "copy_file")

def _generate_cpp_only_build_file(repository_ctx, cpu_value, paths):
    repository_ctx.template(
        "BUILD",
        paths["@bazel_tools//tools/cpp:BUILD.toolchains.tpl"],
        {"%{name}": cpu_value},
    )

def go_proto_link_impl(ctx, **kwargs):
    print("Copying generated files for proto library %s" % ctx.label)
    output_file = ctx.actions.declare_directory(ctx.attr.dir)
    generated = ctx.attr.src[OutputGroupInfo].go_generated_srcs.to_list()
    content = ""
    # cp -f %s %s/;\n"
    for f in generated:
       line = "cp -f %s %s/;\n" % (f.path, ctx.attr.dir)
       content += line
    ctx.actions.run_shell(
        outputs = [output_file],
        command = content,
        execution_requirements = {
            "no-sandbox": "1",
            "no-remote": "1",
            "local": "1",
        },
    )
    return [DefaultInfo(files = depset([output_file]))]

go_proto_link = rule(
    implementation = go_proto_link_impl,
    attrs = {
        "dir": attr.string(),
        "src": attr.label(),
    }
)

def native_go_library(name, native_library_name):
    """Copy native library file to different path based on operating systems"""
    copy_file(
        name = name + "_darwin",
        src = native_library_name,
        out = "pkg/ray/packaged/lib/darwin-amd64/libcore_worker_library_go.so",
    )

    copy_file(
        name = name + "_linux",
        src = native_library_name,
        out = "pkg/ray/packaged/lib/linux-amd64/libcore_worker_library_go.so",
    )

    native.filegroup(
        name = name,
        srcs = select({
            "@bazel_tools//src/conditions:darwin": [name + "_darwin"],
            "@bazel_tools//src/conditions:windows": [],
            "//conditions:default": [name + "_linux"],
        }),
        visibility = ["//visibility:public"],
    )
