load("@bazel_skylib//lib:paths.bzl", "paths")

# py_test_module_list creates a py_test target for each
# Python file in `files`

def _convert_target_to_import_path(t):
    """Get a Python import path for the provided bazel file target."""
    if not t.startswith("//"):
      fail("Must be an absolute target starting in '//'.")
    if not t.endswith(".py"):
      fail("Must end in '.py'.")

    # 1) Strip known prefix and suffix (validated above).
    t = t[len("//"):-len(".py")]
    # 2) Normalize separators to '/'.
    t = t.replace(":", "/")
    # 3) Replace '/' with '.' to form an import path.
    return t.replace("/", ".")

def doctest_each(files, gpu = False, deps=[], srcs=[], data=[], args=[], size="medium", tags=[], pytest_plugin_file="//bazel:default_doctest_pytest_plugin.py", **kwargs):
    # Unlike the `doctest` macro, `doctest_each` runs `pytest` on each file separately.
    # This is useful to run tests in parallel and more clearly report the test results.
    for file in files:
        doctest(
            files = [file],
            gpu = gpu,
            name = paths.split_extension(file)[0],
            deps = deps,
            srcs = srcs,
            data = data,
            args = args,
            size = size,
            tags = tags,
            pytest_plugin_file = pytest_plugin_file,
            **kwargs
        )

def doctest(files, gpu = False, name="doctest", deps=[], srcs=[], data=[], args=[], size="medium", tags=[], pytest_plugin_file="//bazel:default_doctest_pytest_plugin.py", **kwargs):
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
            "--disable-warnings",
            "-v",
            # Don't pick up the global pytest.ini for doctests.
            "-c", "NO_PYTEST_CONFIG",
            # Pass the provided pytest plugin as a Python import path.
            "-p", _convert_target_to_import_path(pytest_plugin_file),
        ] + args + ["$(location :%s)" % file for file in files],
        data = [pytest_plugin_file] + files + data,
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

def py_test_module_list_with_env_variants(files, env_variants, size="medium", **kwargs):
    """Create multiple py_test_module_list targets with different environment variable configurations.

    Args:
        files: List of test files to run
        env_variants: Dict where keys are variant names and values are dicts containing
                     'env' and 'name_suffix' keys
        size: Test size
        **kwargs: Additional arguments passed to py_test_module_list
    """
    for variant_name, variant_config in env_variants.items():
        py_test_module_list(
            size = size,
            files = files,
            env = variant_config.get("env", {}),
            name_suffix = variant_config.get("name_suffix", "_{}".format(variant_name)),
            **kwargs
        )
