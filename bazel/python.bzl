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
            "-v"
        ] + args + ["$(location :%s)" % file for file in files],
        data = ["//bazel:conftest.py"] + files + data,
        python_version = "PY3",
        srcs_version = "PY3",
        tags = ["doctest"] + tags,
        deps = ["//:ray_lib"] + deps,
        **kwargs
    )


def py_test_module_list(files, size, deps, tags = [], env = {}, extra_srcs=[], name_suffix="", **kwargs):
    default_shard_num = 1

    if size == "large":
        default_shard_num = 8

    if size == "medium":
        default_shard_num = 4

    if size == "small":
        default_shard_num = 2

    for file in files:
        shard_num = default_shard_num
        test_tags = list(tags)

        if type(file) != "string":
            file, extra_tags = file[0], list(file[1:])
            test_tags += extra_tags

        # remove .py
        name = paths.split_extension(file)[0] + name_suffix

        if "exclusive" in test_tags:
            shard_num = 1
            test_tags += ["no-sandbox"]
        print(test_tags)
        env_items = env.items()
        if shard_num != 1:
            env_items += [("RAY_CI_PYTEST_SHARD_NUM", str(shard_num))]
        if name == file:
            basename = basename + "_test"
        for shard_id in range(shard_num):
            test_name = name
            test_env = env_items
            if shard_num != 1:
                test_name = name + "@" + str(shard_id + 1) + "_" + str(shard_num)
                test_env += [("RAY_CI_PYTEST_SHARD_ID", str(shard_id))]

            native.py_test(
                name = test_name,
                size = size,
                main = file,
                tags = test_tags,
                srcs = extra_srcs + [file],
                deps = deps,
                env=dict(test_env),
                **kwargs,
            )


def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude, allow_empty=False):
        print(file)
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
