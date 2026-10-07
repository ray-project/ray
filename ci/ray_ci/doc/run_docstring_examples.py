"""Run the `>>>` docstring examples in the files a pull request changed.

Used by the "docstring-only" label's per-library steps, such as
"data: docstring examples" in .buildkite/data.rayci.yml. It runs only the
examples in the changed files, not a library's whole doctest target.

For each changed module, it imports the module, extracts the `>>>` examples
from every docstring in the file (module, classes, functions, and methods), and
runs each docstring's examples in order as plain Python, in a copy of the
module's globals. A docstring fails if any of its examples raises. Expected
output lines are ignored: this checks that the example code runs, not what it
prints.

Examples marked `# doctest: +SKIP` are skipped, and a docstring with
`# doctest: +SKIP_EXAMPLE` on any example is skipped whole, matching the
render-only convention in bazel/default_doctest_pytest_plugin.py. The standard
library's doctest parser is used only to read the `>>>` and `...` prompts and
those directives; no doctest runner or output checker is involved.

Usage:
  python ci/ray_ci/doc/run_docstring_examples.py --source-dir python/ray/data
  python ci/ray_ci/doc/run_docstring_examples.py python/ray/data/aggregate.py
  python ci/ray_ci/doc/run_docstring_examples.py --source-dir python/ray/data \
      --list-changed

Without explicit files, it fetches the pull request's base branch and takes
the modified .py files under --source-dir, excluding tests/ and examples/.
--list-changed prints that list and exits, for ci/ray_ci/doc/
run_docstring_examples.sh, which lists files on the CI host and runs them in
the library's image.
"""

import argparse
import ast
import dataclasses
import doctest
import importlib
import os
import re
import subprocess
import sys
import traceback
from typing import Dict, List, Optional

SKIP_EXAMPLE = doctest.register_optionflag("SKIP_EXAMPLE")

_EXCLUDED_DIRS = re.compile(r"(^|/)(tests|examples)/")


@dataclasses.dataclass
class DocstringExamples:
    # Dotted name inside the module, such as "Count" or "Dataset.map".
    qualname: str
    lineno: int
    examples: List[doctest.Example]


def extract(source: str, filename: str) -> List[DocstringExamples]:
    """Return every docstring in `source` that has `>>>` examples."""
    parser = doctest.DocTestParser()
    found = []

    def visit(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{prefix}{child.name}"
                add(child, name)
                visit(child, name + ".")

    def add(node: ast.AST, name: str) -> None:
        docstring = ast.get_docstring(node, clean=True)
        if not docstring:
            return
        examples = parser.get_examples(docstring, name=f"{filename}:{name}")
        if examples:
            lineno = node.body[0].lineno if hasattr(node, "body") else 1
            found.append(DocstringExamples(name, lineno, examples))

    tree = ast.parse(source, filename=filename)
    add(tree, "<module>")
    visit(tree, "")
    return found


def is_skipped(docstring: DocstringExamples) -> bool:
    return any(example.options.get(SKIP_EXAMPLE) for example in docstring.examples)


def run_docstring(
    docstring: DocstringExamples, module_globals: Dict, filename: str
) -> Optional[str]:
    """Run one docstring's examples in order. Return a traceback on failure."""
    namespace = dict(module_globals)
    for example in docstring.examples:
        if example.options.get(doctest.SKIP):
            continue
        # example.lineno counts from the docstring's first line.
        line = docstring.lineno + example.lineno
        location = f"{filename}:{line} ({docstring.qualname})"
        try:
            code = compile(example.source, location, "exec")
            exec(code, namespace)
        except BaseException:  # noqa: BLE001 - report any failure, including SystemExit.
            return f"{location}\n{example.source}{traceback.format_exc()}"
    return None


def module_name(path: str) -> str:
    """Map python/ray/data/aggregate.py to ray.data.aggregate."""
    parts = path[len("python/") :] if path.startswith("python/") else path
    parts = parts[: -len(".py")].split("/")
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def changed_files(source_dir: str) -> List[str]:
    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH", "master")
    # Diff against FETCH_HEAD, as the docs example opt-in does: a clone with a
    # restricted refspec may never create a local origin/<base>.
    subprocess.check_call(["git", "fetch", "-q", "origin", base_branch])
    output = subprocess.check_output(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=M",
            "FETCH_HEAD...HEAD",
            "--",
            source_dir.rstrip("/") + "/",
        ],
        text=True,
    )
    return [
        path
        for path in output.split()
        if path.endswith(".py") and not _EXCLUDED_DIRS.search(path)
    ]


def _shutdown_ray() -> None:
    ray = sys.modules.get("ray")
    if ray is not None and ray.is_initialized():
        ray.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="*", help="Repo-relative .py files.")
    parser.add_argument(
        "--source-dir",
        help="Without files, run the modified .py files under this directory.",
    )
    parser.add_argument(
        "--list-changed",
        action="store_true",
        help="Print the modified files under --source-dir and exit.",
    )
    args = parser.parse_args()

    files = args.files
    if not files:
        if not args.source_dir:
            parser.error("pass files or --source-dir")
        files = changed_files(args.source_dir)
    if args.list_changed:
        print("\n".join(files))
        return 0
    if not files:
        print("No modules changed; no docstring examples to run.")
        return 0

    failures = []
    ran = skipped = 0
    for path in files:
        with open(path) as f:
            docstrings = extract(f.read(), path)
        if not docstrings:
            print(f"{path}: no >>> examples")
            continue
        module = importlib.import_module(module_name(path))
        for docstring in docstrings:
            if is_skipped(docstring):
                skipped += 1
                print(f"SKIP {path}:{docstring.qualname} (+SKIP_EXAMPLE)")
                continue
            ran += 1
            failure = run_docstring(docstring, vars(module), path)
            status = "FAIL" if failure else "PASS"
            print(f"{status} {path}:{docstring.qualname}")
            if failure:
                failures.append(failure)
        _shutdown_ray()

    print(
        f"\n{ran} docstring(s) run, {len(failures)} failed, {skipped} skipped, "
        f"in {len(files)} file(s)."
    )
    for failure in failures:
        print("\n" + failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
