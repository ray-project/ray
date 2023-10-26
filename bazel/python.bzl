load("@bazel_skylib//lib:paths.bzl", "paths")

# py_test_module_list creates a py_test target for each
# Python file in `files`


def doctest(files, gpu = False, name="doctest", deps=[], srcs=[], data=[], args=[], size="medium", tags=[], **kwargs):
    # NOTE: If you run `pytest` on `__init__.py`, it tries to test all files in that
    # package. We don't want that, so we exclude it from the list of input files.
    files = native.glob(include=files, exclude=["__init__.py"])
    if gpu:
        name += "[gpu]"
        tags = tags + ["gpu"]
    else:
        tags = tags + ["cpu"]

    native.py_test(
        name = name,
        srcs = ["//bazel:pytest_wrapper.py"] + srcs,
        main = "//bazel:pytest_wrapper.py",
        size = size,
        args = [
            "--doctest-modules",
            "--doctest-glob='*.md'",
            "-c=$(location //bazel:conftest.py)",
            "--disable-warnings",
            "-v"
        ] + args + ["$(location :%s)" % file for file in files],
        data = ["//bazel:conftest.py"] + files + data,
        python_version = "PY3",
        srcs_version = "PY3",
        tags = ["doctest"] + tags,
        deps = ["//:ray_lib"] + deps,
        **kwargs
    )


def py_test_module_list(files, size, deps, extra_srcs=[], name_suffix="", **kwargs):
    for file in files:
        # remove .py
        name = paths.split_extension(file)[0] + name_suffix
        if name == file:
            basename = basename + "_test"
        native.py_test(
            name = name,
            size = size,
            main = file,
            srcs = extra_srcs + [file],
            deps = deps,
            **kwargs
        )

def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude, allow_empty=False):
        basename = paths.split_extension(file)[0]
        if basename == file:
            basename = basename + "_test"
        native.py_test(
            name = basename,
            srcs = extra_srcs + [file],
            **kwargs
        )

# Runs all included notebooks as py_test targets, by first converting them to .py files with "test_myst_doc.py".
def py_test_run_all_notebooks(include, exclude, allow_empty=False, **kwargs):
    for file in native.glob(include = include, exclude = exclude, allow_empty=allow_empty):
        print(file)
        basename = paths.split_extension(file)[0]
        if basename == file:
            basename = basename + "_test"
        native.py_test(
            name = basename,
            main = "test_myst_doc.py",
            srcs = ["//doc:test_myst_doc.py"],
            # --find-recursively will look for file in all
            # directories inside cwd recursively if it cannot
            # find it right away. This allows to deal with
            # mismatches between `name` and `data` args.
            args = ["--find-recursively", "--path", file],
            **kwargs
        )
