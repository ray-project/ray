load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = [
            "**/internal_thirdparty_files/**",
            "**/_private/internal_third_party.py",
        ],
    ),
    tags = ["team:core"],
)
