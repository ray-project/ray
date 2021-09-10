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
        inputs = ctx.attr.src.files,
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

def go_library_link_impl(ctx, **kwargs):
    output_file = ctx.actions.declare_directory(ctx.attr.dst)
    generated = ctx.attr.src.files
    content = ""
    # cp -f %s %s/;\n"
    for f in generated.to_list():
       line = "cp -f %s %s/;\n" % (f.path, ctx.attr.dst)
       content += line
    ctx.actions.run_shell(
        outputs = [output_file],
        inputs = ctx.attr.src.files,
        command = content,
        progress_message = "Copying golang worker library %{label}",
        execution_requirements = {
            "no-sandbox": "1",
            "no-remote": "1",
            "no-cache": "1",
            "local": "1",
        },
    )
    return [DefaultInfo(files = depset([output_file]))]

go_library_link = rule(
    implementation = go_library_link_impl,
    attrs = {
        "src": attr.label(),
        "dst": attr.string()
    }
)
