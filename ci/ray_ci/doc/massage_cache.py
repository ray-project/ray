"""Strip build-environment-local dependencies from a Sphinx environment pickle.

Run this with the Python that built the docs (the doc-build image's interpreter),
which is typically a newer Sphinx than ci/ray_ci's Bazel-pinned one. A pickled
Sphinx environment can only be unpickled by a matching Sphinx; loading a Sphinx
8.x pickle under an older Sphinx raises e.g. ``ModuleNotFoundError: No module
named 'sphinx.util._files'``. This script imports only the standard library so
any interpreter can run it (see ``BuildCache._massage_cache``).

Usage:
    python massage_cache.py <environment_pickle_path>
"""

import pickle
import sys


def strip_local_dependencies(environment_cache_path: str) -> None:
    with open(environment_cache_path, "rb") as f:
        environment_cache = pickle.load(f)

    for doc, dependencies in environment_cache.dependencies.items():
        # site-packages paths are local to the build machine and would mark every
        # doc that imports them as outdated when the cache is restored elsewhere.
        environment_cache.dependencies[doc] = type(dependencies)(
            d for d in dependencies if "site-packages" not in d
        )

    with open(environment_cache_path, "wb") as f:
        pickle.dump(environment_cache, f, pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    strip_local_dependencies(sys.argv[1])
