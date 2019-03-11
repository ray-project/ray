#Structure representing info about Java source files
JavaSourceFiles = provider(
    fields = {
        'files' : 'java source files'
    }
)

# This aspect is responsible for collecting Java sources for target
# When applied to target, `JavaSourceFiles` struct will be attached to
# target info
def collect_sources_impl(target, ctx):
    files = []
    if hasattr(ctx.rule.attr, 'srcs'):
        for src in ctx.rule.attr.srcs:
            for file in src.files:
                if file.extension == 'java':
                    files.append(file)
    return [JavaSourceFiles(files = files)]


collect_sources = aspect(
    implementation = collect_sources_impl,
)


# `ctx` is rule context: https://docs.bazel.build/versions/master/skylark/lib/ctx.html
def _checkstyle_test_impl(ctx):
    # verify target name matches naming conventions
    if "{}-checkstyle".format(ctx.attr.target.label.name) != ctx.attr.name:
        fail("target should follow `{java_library target name}-checkstyle` pattern")

    suppressions = ctx.file.suppressions
    opts = ctx.attr.opts
    sopts = ctx.attr.string_opts

    # Checkstyle and its dependencies
    checkstyle_dependencies = ctx.attr._checkstyle.java.transitive_runtime_deps
    classpath = ":".join([file.path for file in checkstyle_dependencies])

    args = ""
    inputs = []
    if ctx.file.config:
      args += " -c %s" % ctx.file.config.path
      inputs.append(ctx.file.config)
    if suppressions:
      inputs.append(suppressions)

    # Build command to run Checkstyle test
    cmd = " ".join(
        ["java -cp %s com.puppycrawl.tools.checkstyle.Main" % classpath] +
        [args] +
        ["--%s" % x for x in opts] +
        ["--%s %s" % (k, sopts[k]) for k in sopts] +
        [file.path for file in ctx.attr.target[JavaSourceFiles].files]
    )

    # Wrap checkstyle command in a shell script so allow_failure is supported
    ctx.actions.expand_template(
        template = ctx.file._checkstyle_sh_template,
        output = ctx.outputs.checkstyle_script,
        substitutions = {
            "{command}" : cmd,
            "{allow_failure}": str(int(ctx.attr.allow_failure)),
        },
        is_executable = True,
    )

    files = [ctx.outputs.checkstyle_script, ctx.file.license] + ctx.attr.target[JavaSourceFiles].files + checkstyle_dependencies.to_list() + inputs
    runfiles = ctx.runfiles(
        files = files,
        collect_data = True
    )
    return DefaultInfo(
        executable = ctx.outputs.checkstyle_script,
        files = depset(files),
        runfiles = runfiles,
    )

checkstyle_test = rule(
    implementation = _checkstyle_test_impl,
    test = True,
    attrs = {
        "config": attr.label(
            allow_single_file=True,
            doc = "A checkstyle configuration file",
            default = "//java/tools/checkstyle:checkstyle.xml",
        ),
        "suppressions": attr.label(
            allow_single_file=True,
            doc = ("A file for specifying files and lines " +
                   "that should be suppressed from checks." +
                   "Example: https://github.com/checkstyle/checkstyle/blob/master/config/suppressions.xml"),
            default = "//java/tools/checkstyle:checkstyle-suppressions.xml",
        ),
        "license": attr.label(
            allow_single_file=True,
            doc = "A license file that can be used with the checkstyle license target",
            default = "//java/tools/checkstyle:license-header.txt",
        ),
        "opts": attr.string_list(
            doc = "Options to be passed on the command line that have no argument"
        ),
        "string_opts": attr.string_dict(
            doc = "Options to be passed on the command line that have an argument"
        ),
        "target": attr.label(
            doc = "The java_library target to check sources on",
            aspects = [collect_sources],
            mandatory = True
        ),
        "allow_failure": attr.bool(
            default = False,
            doc = "Successfully finish the test even if checkstyle failed"
        ),
        "_checkstyle_sh_template": attr.label(
             allow_single_file = True,
             default = "//java/tools/checkstyle:checkstyle.sh"
        ),
        "_checkstyle": attr.label(
            default = "//java/third_party/com/puppycrawl/tools:checkstyle"
        ),
    },
    outputs = {
        "checkstyle_script": "%{name}.sh",
    },
)

