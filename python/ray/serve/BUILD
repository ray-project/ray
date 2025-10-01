load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:python.bzl", "doctest")

doctest(
    size = "small",
    files = glob(
        ["**/*.py"],
        exclude = [
            "tests/**",
            # FIXME: Add the llm tests back with a diff tag.
            "llm/**",
            # FIXME: Failing on Windows
            "gradio_integrations.py",
            "_private/benchmarks/**",
        ],
    ),
    tags = ["team:serve"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/**/*.py"],
    ),
    visibility = [
        "//python/ray/serve:__pkg__",
        "//python/ray/serve:__subpackages__",
        "//release:__pkg__",
    ],
)
