import sys

if sys.version_info < (3, 11):
    raise RuntimeError(
        "ray[llm] requires Python 3.11 or higher. "
        f"Current version: {sys.version_info.major}.{sys.version_info.minor}"
    )
