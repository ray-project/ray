#!/usr/bin/env python
#
# This file contains utilities for understanding dependencies between python
# source files and tests.
#
# Utils are assumed to be used from top level ray/ folder, since that is how
# our tests are defined today.
#
# Example usage:
#     To find all circular dependencies under ray/python/:
#         python ci/travis/py_dep_analysis.py --mode=circular-dep
#     To find all the RLlib tests that depend on a file:
#         python ci/travis/py_dep_analysis.py --mode=test-dep \
#             --file=python/ray/tune/tune.py
#     For testing, add --smoke-test to any commands, so it doesn't spend
#     tons of time querying for available RLlib tests.

import argparse
import ast
import os
import re
import subprocess
import sys
from typing import Dict, List, Tuple


class DepGraph(object):
    def __init__(self):
        self.edges: Dict[str, Dict[str, bool]] = {}
        self.ids: Dict[str, int] = {}
        self.inv_ids: Dict[int, str] = {}


def _run_shell(args: List[str]) -> str:
    return subprocess.check_output(args).decode(sys.stdout.encoding)


def list_rllib_tests(n: int = -1, test: str = None) -> Tuple[str, List[str]]:
    """List RLlib tests.

    Args:
        n: return at most n tests. all tests if n = -1.
        test: only return information about a specific test.
    """
    tests_res = _run_shell(
        ["bazel", "query", "tests(//python/ray/rllib:*)", "--output", "label"]
    )

    all_tests = []

    # Strip, also skip any empty lines
    tests = [t.strip() for t in tests_res.splitlines() if t.strip()]
    for t in tests:
        if test and t != test:
            continue

        src_out = _run_shell(
            [
                "bazel",
                "query",
                'kind("source file", deps({}))'.format(t),
                "--output",
                "label",
            ]
        )

        srcs = [f.strip() for f in src_out.splitlines()]
        srcs = [f for f in srcs if f.startswith("//python") and f.endswith(".py")]
        if srcs:
            all_tests.append((t, srcs))

        # Break early if smoke test.
        if n > 0 and len(all_tests) >= n:
            break

    return all_tests


def _new_dep(graph: DepGraph, src_module: str, dep: str):
    """Create a new dependency between src_module and dep."""
    if dep not in graph.ids:
        graph.ids[dep] = len(graph.ids)

    src_id = graph.ids[src_module]
    dep_id = graph.ids[dep]

    if src_id not in graph.edges:
        graph.edges[src_id] = {}
    graph.edges[src_id][dep_id] = True


def _new_import(graph: DepGraph, src_module: str, dep_module: str):
    """Process a new import statement in src_module."""
    # We don't care about system imports.
    if not dep_module.startswith("ray"):
        return

    _new_dep(graph, src_module, dep_module)


def _is_path_module(module: str, name: str, _base_dir: str) -> bool:
    """Figure out if base.sub is a python module or not."""
    # Special handling for _raylet, which is a C++ lib.
    if module == "ray._raylet":
        return False

    bps = ["python"] + module.split(".")
    path = os.path.join(_base_dir, os.path.join(*bps), name + ".py")
    if os.path.isfile(path):
        return True  # file module
    return False


def _new_from_import(
    graph: DepGraph, src_module: str, dep_module: str, dep_name: str, _base_dir: str
):
    """Process a new "from ... import ..." statement in src_module."""
    # We don't care about imports outside of ray package.
    if not dep_module or not dep_module.startswith("ray"):
        return

    if _is_path_module(dep_module, dep_name, _base_dir):
        # dep_module.dep_name points to a file.
        _new_dep(graph, src_module, _full_module_path(dep_module, dep_name))
    else:
        # sub is an obj on base dir/file.
        _new_dep(graph, src_module, dep_module)


def _process_file(graph: DepGraph, src_path: str, src_module: str, _base_dir=""):
    """Create dependencies from src_module to all the valid imports in src_path.

    Args:
        graph: the DepGraph to be added to.
        src_path: .py file to be processed.
        src_module: full module path of the source file.
        _base_dir: use a different base dir than current dir. For unit testing.
    """
    with open(os.path.join(_base_dir, src_path), "r") as in_f:
        tree = ast.parse(in_f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    _new_import(graph, src_module, alias.name)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    _new_from_import(
                        graph, src_module, node.module, alias.name, _base_dir
                    )


def build_dep_graph() -> DepGraph:
    """Build index from py files to their immediate dependees."""
    graph = DepGraph()

    # Assuming we run from root /ray directory.
    # Follow links since rllib is linked to /rllib.
    for root, sub_dirs, files in os.walk("python", followlinks=True):
        if _should_skip(root):
            continue

        module = _bazel_path_to_module_path(root)

        # Process files first.
        for f in files:
            if not f.endswith(".py"):
                continue

            full = _full_module_path(module, f)

            if full not in graph.ids:
                graph.ids[full] = len(graph.ids)

            # Process file:
            _process_file(graph, os.path.join(root, f), full)

    # Build reverse index for convenience.
    graph.inv_ids = {v: k for k, v in graph.ids.items()}

    return graph


def _full_module_path(module, f) -> str:
    if f == "__init__.py":
        # __init__ file for this module.
        # Full path is the same as the module name.
        return module

    fn = re.sub(r"\.py$", "", f)

    if not module:
        return fn
    return module + "." + fn


def _should_skip(d: str) -> bool:
    """Skip directories that should not contain py sources."""
    if d.startswith("python/.eggs/"):
        return True
    if d.startswith("python/."):
        return True
    if d.startswith("python/build"):
        return True
    if d.startswith("python/ray/cpp"):
        return True
    return False


def _bazel_path_to_module_path(d: str) -> str:
    """Convert a Bazel file path to python module path.

    Example: //python/ray/rllib:xxx/yyy/dd -> ray.rllib.xxx.yyy.dd
    """
    # Do this in 3 steps, so all of 'python:', 'python/', or '//python', etc
    # will get stripped.
    d = re.sub(r"^\/\/", "", d)
    d = re.sub(r"^python", "", d)
    d = re.sub(r"^[\/:]", "", d)
    return d.replace("/", ".").replace(":", ".")


def _file_path_to_module_path(f: str) -> str:
    """Return the corresponding module path for a .py file."""
    dir, fn = os.path.split(f)
    return _full_module_path(_bazel_path_to_module_path(dir), fn)


def _depends(
    graph: DepGraph, visited: Dict[int, bool], tid: int, qid: int
) -> List[int]:
    """Whether there is a dependency path from module tid to module qid.

    Given graph, and without going through visited.
    """
    if tid not in graph.edges or qid not in graph.edges:
        return []
    if qid in graph.edges[tid]:
        # tid directly depends on qid.
        return [tid, qid]
    for c in graph.edges[tid]:
        if c in visited:
            continue
        visited[c] = True
        # Reduce to a question of whether there is a path from c to qid.
        ds = _depends(graph, visited, c, qid)
        if ds:
            # From tid -> c -> qid.
            return [tid] + ds
    return []


def test_depends_on_file(
    graph: DepGraph, test: Tuple[str, Tuple[str]], path: str
) -> List[int]:
    """Give dependency graph, check if a test depends on a specific .py file.

    Args:
        graph: the dependency graph.
        test: information about a test, in the format of:
            [test_name, (src files for the test)]
    """
    query = _file_path_to_module_path(path)
    if query not in graph.ids:
        # Not a file that we care about.
        return []

    t, srcs = test

    # Skip tuned_examples/ and examples/ tests.
    if t.startswith("//python/ray/rllib:examples/"):
        return []

    for src in srcs:
        if src == "ray.rllib.tests.run_regression_tests":
            return []

        tid = _file_path_to_module_path(src)
        if tid not in graph.ids:
            # Not a test that we care about.
            # TODO(jungong): What tests are these?????
            continue

        branch = _depends(graph, {}, graph.ids[tid], graph.ids[query])
        if branch:
            return branch

    # Does not depend on file.
    return []


def _find_circular_dep_impl(graph: DepGraph, id: str, branch: str) -> bool:
    if id not in graph.edges:
        return False
    for c in graph.edges[id]:
        if c in branch:
            # Found a circle.
            branch.append(c)
            return True
        branch.append(c)
        if _find_circular_dep_impl(graph, c, branch):
            return True
        branch.pop()
    return False


def find_circular_dep(graph: DepGraph) -> Dict[str, List[int]]:
    """Find circular dependencies among a dependency graph."""
    known = {}
    circles = {}
    for m, id in graph.ids.items():
        branch = []
        if _find_circular_dep_impl(graph, id, branch):
            if branch[-1] in known:
                # Already knew, skip.
                continue
            # Since this is a cycle dependency, any step along the circle
            # will form a different circle.
            # So we mark every entry on this circle known.
            for n in branch:
                known[n] = True
            # Mark that module m contains a potential circular dep.
            circles[m] = branch
    return circles


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        type=str,
        default="test-dep",
        help=(
            "test-dep: find dependencies for a specified test. "
            "circular-dep: find circular dependencies in "
            "the specific codebase."
        ),
    )
    parser.add_argument(
        "--file", type=str, help="Path of a .py source file relative to --base_dir."
    )
    parser.add_argument("--test", type=str, help="Specific test to check.")
    parser.add_argument(
        "--smoke-test", action="store_true", help="Load only a few tests for testing."
    )

    args = parser.parse_args()

    print("building dep graph ...")
    graph = build_dep_graph()
    print(
        "done. total {} files, {} of which have dependencies.".format(
            len(graph.ids), len(graph.edges)
        )
    )

    if args.mode == "circular-dep":
        circles = find_circular_dep(graph)
        print("Found following circular dependencies: \n")
        for m, b in circles.items():
            print(m)
            for n in b:
                print("    ", graph.inv_ids[n])
            print()

    if args.mode == "test-dep":
        assert args.file, "Must specify --file for the query."

        # Only support RLlib tests for now.
        # The way Tune tests are defined, they all depend on
        # the entire tune codebase.
        tests = list_rllib_tests(5 if args.smoke_test else -1, args.test)
        print("Total # of tests: ", len(tests))

        for t in tests:
            branch = test_depends_on_file(graph, t, args.file)
            if branch:
                print("{} depends on {}".format(t[0], args.file))
                # Print some debugging info.
                for n in branch:
                    print("    ", graph.inv_ids[n])
            else:
                print("{} does not depend on {}".format(t[0], args.file))
