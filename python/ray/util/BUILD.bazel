load("//bazel:python.bzl", "doctest")

# NOTE: `iter.py`, `iter_metrics.py`, and `client_connect.py` only contain deprecated
# APIs, so we're excluding them.
doctest(
    files = glob(
        ["*.py"],
        exclude = [
            "client_connect.py",
            "iter.py",
            "iter_metrics.py",
        ],
    ),
    tags = ["team:core"],
)
