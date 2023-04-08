load("@bazel_skylib//lib:paths.bzl", "paths")

# py_test_module_list creates a py_test target for each
# Python file in `files`

def py_test_module_list(files, size, deps, env = {}, extra_srcs=[], name_suffix="", **kwargs):
    shard_num = 1

    if size == "large":
        shard_num = 3


    if size == "medium":
        shard_num = 2

    env_items = env.items() + [("RAY_CI_PYTEST_SHARD_NUM", str(shard_num))]

    for file in files:
        # remove .py
        name = paths.split_extension(file)[0] + name_suffix
        if name == file:
            basename = basename + "_test"
        for shard_id in range(shard_num):
            test_name = name
            test_env = []
            if shard_num != 1:
                test_name = name + "@" + str(shard_id + 1) + "_" + str(shard_num)
                test_env = env_items + [("RAY_CI_PYTEST_SHARD_ID", str(shard_id))]
            native.py_test(
                name = test_name,
                size = size,
                main = file,
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
