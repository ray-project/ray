"""Run the `>>>` docstring examples in the files a pull request changed.

Used by the "docstring-only" label's "doc: docstring examples" step in
.buildkite/others.rayci.yml, for every library. It runs only the examples in
the changed files, not a library's whole doctest target, and only in files
that a CPU doctest() target covers, so it never runs an example CI doesn't
already run.

For each changed module, it imports the module, extracts the `>>>` examples
from every docstring in the file (module, classes, functions, and methods), and
runs each docstring's examples in order, in a copy of the module's globals,
the way an interactive session would. A docstring fails if an example raises,
or if an example that shows output prints something else. An example that shows
no output only has to run.

Output matching follows Ray's doctest targets: `...` matches any text, the
pytest default of ELLIPSIS, and per-example directives such as
`# doctest: +NORMALIZE_WHITESPACE` apply. An example that shows a traceback
passes when it raises an exception whose type and message match.

Examples marked `# doctest: +SKIP` are skipped, and a docstring with
`# doctest: +SKIP_EXAMPLE` on any example is skipped whole, matching the
render-only convention in bazel/default_doctest_pytest_plugin.py. When a
skipped docstring holds an example that shows expected output, the skip line
counts those examples, since that output is never checked.

Ray Data modules run with the DataContext settings that Ray Data's doctest
plugin, python/ray/data/tests/doctest_pytest_plugin.py, applies to each
docstring, so an order-dependent example behaves as it does in its target.

The examples image leaves out heavy libraries such as torch and TensorFlow
(see OMITTED_LIBRARIES). A docstring that fails only because one of them isn't
installed is reported as skipped, with the library named, rather than failed,
and so is every docstring in a module that can't be imported without one. Those
examples run in their library's doctest() target instead: in premerge for
libraries whose suites the label doesn't skip, and postmerge for Ray Data. The
step prints the skipped examples in an expanded section of the job log. Any
other import
failure fails the run, so a typo in an import isn't hidden. The standard
library's doctest module supplies the `>>>` parser and the output comparison;
there's no doctest runner, pytest collection, or Bazel doctest target.

Usage:
  python ci/ray_ci/doc/run_docstring_examples.py --source-dir python/ray
  python ci/ray_ci/doc/run_docstring_examples.py python/ray/data/aggregate.py
  python ci/ray_ci/doc/run_docstring_examples.py --source-dir python/ray \
      --list-changed --doctest-labels labels.txt

Without explicit files, it fetches the pull request's base branch from origin
and takes the modified .py files under --source-dir, excluding tests/ and
examples/. --base diffs against a ref you name instead, such as
upstream/master in a clone where origin is a fork.
--doctest-labels keeps only the files listed in a file of Bazel labels, one
per line, such as the output of the `bazel query` in run_docstring_examples.sh
for the files CPU doctest() targets cover, and names the files it drops.
--list-changed prints the list and exits, for ci/ray_ci/doc/
run_docstring_examples.sh, which lists files on the CI host and runs them in
the docstringbuild image.
"""

import argparse
import ast
import collections
import contextlib
import dataclasses
import doctest
import importlib
import importlib.util
import io
import os
import re
import subprocess
import sys
import traceback
from typing import Dict, List, Optional, Tuple

SKIP_EXAMPLE = doctest.register_optionflag("SKIP_EXAMPLE")

_EXCLUDED_DIRS = re.compile(r"(^|/)(tests|examples)/")

# Ray's doctest targets run pytest without a config file, so pytest's default
# doctest_optionflags of ELLIPSIS applies. Match it.
DEFAULT_OPTIONFLAGS = doctest.ELLIPSIS

# Libraries the examples image, ci/docker/docstring.build.Dockerfile, leaves
# out, keyed by top-level module name. Each value lists other names an error
# message may use for the library, such as RLlib's "PyTorch was specified as
# the framework", which raises a bare ImportError with no module name.
OMITTED_LIBRARIES = {
    "torch": ("pytorch",),
    "torchvision": (),
    "torchaudio": (),
    "tensorflow": (),
    "tensorflow_datasets": (),
    "keras": (),
    "jax": (),
    "transformers": (),
    "lightning": (),
    "pytorch_lightning": (),
    # Dataset.join needs it. It's about 195 MB installed, larger than pyarrow,
    # for one docstring, and Ray Data's own doctest image installs it through
    # test-requirements.txt.
    "polars": (),
    # Dataset.write_lance needs it, from the pylance distribution. It's about
    # 138 MB installed on Linux aarch64, for one docstring, and Ray Data's own
    # doctest image installs pylance through data-test-requirements.txt.
    "lance": (),
    # The LightGBM and XGBoost integrations in Ray Train and Ray Tune import
    # these at module level, but none of their examples runs here: Tune's are
    # render-only (+SKIP_EXAMPLE), and Train's are still testcode blocks. The
    # ML images install both through
    # python/requirements/ml/core-requirements.txt, and xgboost pulls
    # nvidia-nccl-cu12 on Linux x86_64.
    "lightgbm": (),
    "xgboost": (),
    # The Azure node provider needs azure-cli-core to import, and its only
    # runnable example is a single private import. "azure" is a namespace
    # package, so if any azure-* distribution reaches the image, _installed
    # reports it as installed and the provider module fails rather than skips.
    "azure": (),
}


class MissingLibrary(Exception):
    """An example needs a library in OMITTED_LIBRARIES that isn't installed."""

    def __init__(self, library: str):
        super().__init__(library)
        self.library = library


def _installed(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def missing_library(error: BaseException) -> Optional[str]:
    """Return the omitted, uninstalled library an import error reports, if any.

    Follows the exception chain for a ModuleNotFoundError's module name, since
    Ray Data re-raises it as an ImportError with install advice. Falls back to
    the message for a bare ImportError. Either way, a library that's installed
    never matches, so a full image can't skip anything.

    The chain stops at `raise ... from None`. An implicit context can't be
    told apart from Ray Data's re-raise, though, so a different ImportError
    raised while handling a missing omitted library is skipped rather than
    failed. That's rare, and it errs toward skipping.
    """
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ImportError) and current.name:
            top = current.name.split(".")[0]
            if top in OMITTED_LIBRARIES and not _installed(top):
                return top
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            current = None
        else:
            current = current.__context__
    if isinstance(error, ImportError):
        message = str(error).lower()
        for library, aliases in OMITTED_LIBRARIES.items():
            names = (library, *aliases)
            if not _installed(library) and any(
                re.search(rf"\b{re.escape(name)}\b", message) for name in names
            ):
                return library
    return None


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


def hidden_outputs(docstring: DocstringExamples) -> int:
    """Count the examples with expected output that +SKIP_EXAMPLE leaves unchecked."""
    return sum(1 for example in docstring.examples if example.want.strip())


@contextlib.contextmanager
def library_settings(module: str):
    """Apply the per-docstring settings a module's doctest target applies.

    Mirrors python/ray/data/tests/doctest_pytest_plugin.py for ray.data
    modules. Keep the two in sync. Other modules run as they are.
    """
    if module != "ray.data" and not module.startswith("ray.data."):
        yield
        return
    from ray.data.context import DataContext

    os.environ.setdefault("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    context = DataContext.get_current()
    preserve_order = context.execution_options.preserve_order
    print_on_execution_start = context.print_on_execution_start
    context.execution_options.preserve_order = True
    context.print_on_execution_start = False
    try:
        yield
    finally:
        context.execution_options.preserve_order = preserve_order
        context.print_on_execution_start = print_on_execution_start


def _optionflags(example: doctest.Example) -> int:
    flags = DEFAULT_OPTIONFLAGS
    for flag, enabled in example.options.items():
        flags = flags | flag if enabled else flags & ~flag
    return flags


def _exception_message(e: BaseException) -> str:
    """Format an exception the way the standard library's doctest compares it.

    That's every line format_exception_only returns, so notes added with
    add_note count, minus the source line and caret a SyntaxError prints first.
    """
    lines = traceback.format_exception_only(type(e), e)
    if isinstance(e, SyntaxError):
        prefix = f"{type(e).__qualname__}:"
        lines = lines[next(i for i, l in enumerate(lines) if l.startswith(prefix)) :]
    return "".join(lines)


def run_docstring(
    docstring: DocstringExamples, module_globals: Dict, filename: str
) -> Optional[str]:
    """Run one docstring's examples in order. Return a report on failure.

    Raises MissingLibrary when an example fails only because a library in
    OMITTED_LIBRARIES isn't installed.
    """
    checker = doctest.OutputChecker()
    namespace = dict(module_globals)
    for example in docstring.examples:
        if example.options.get(doctest.SKIP):
            continue
        # example.lineno counts from the docstring's first line.
        line = docstring.lineno + example.lineno
        location = f"{filename}:{line} ({docstring.qualname})"
        flags = _optionflags(example)
        stdout = io.StringIO()
        try:
            # "single" mode echoes an expression's value, as the interactive
            # prompt the example imitates does.
            code = compile(example.source, location, "single")
            with contextlib.redirect_stdout(stdout):
                exec(code, namespace)
        except BaseException as e:  # noqa: BLE001 - report any failure.
            if example.exc_msg is not None:
                got = _exception_message(e)
                if checker.check_output(example.exc_msg, got, flags):
                    continue
                return (
                    f"{location}\n{example.source}Expected exception:\n"
                    f"    {example.exc_msg}Got:\n    {got}"
                )
            library = missing_library(e)
            if library is not None:
                raise MissingLibrary(library) from e
            return f"{location}\n{example.source}{traceback.format_exc()}"
        if example.exc_msg is not None:
            return (
                f"{location}\n{example.source}Expected exception:\n"
                f"    {example.exc_msg}Got: no exception"
            )
        if example.want and not checker.check_output(
            example.want, stdout.getvalue(), flags
        ):
            diff = checker.output_difference(example, stdout.getvalue(), flags)
            return f"{location}\n{example.source}{diff}"
    return None


def label_path(label: str) -> str:
    """Map a Bazel file label such as //python/ray/data:aggregate.py to a path."""
    package, _, name = label.lstrip("/").partition(":")
    return f"{package}/{name}" if package else name


def covered_files(files: List[str], labels: List[str]) -> Tuple[List[str], List[str]]:
    """Split files into those a label covers and those none does."""
    covered = {label_path(label.strip()) for label in labels if label.strip()}
    kept = [path for path in files if path in covered]
    dropped = [path for path in files if path not in covered]
    return kept, dropped


def module_name(path: str) -> str:
    """Map python/ray/data/aggregate.py to ray.data.aggregate."""
    parts = path[len("python/") :] if path.startswith("python/") else path
    parts = parts[: -len(".py")].split("/")
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def changed_files(source_dir: str, base: Optional[str] = None) -> List[str]:
    if base is None:
        base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH", "master")
        # Diff against FETCH_HEAD, as the docs example opt-in does: a clone with
        # a restricted refspec may never create a local origin/<base>.
        subprocess.check_call(["git", "fetch", "-q", "origin", base_branch])
        base = "FETCH_HEAD"
    output = subprocess.check_output(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=M",
            f"{base}...HEAD",
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
    parser.add_argument(
        "--base",
        help="With --source-dir, diff HEAD against this ref instead of fetching "
        "the base branch from origin.",
    )
    parser.add_argument(
        "--doctest-labels",
        help="A file of Bazel file labels, one per line. Keep only the files "
        "it lists.",
    )
    args = parser.parse_args()

    files = args.files
    if not files:
        if not args.source_dir:
            parser.error("pass files or --source-dir")
        files = changed_files(args.source_dir, args.base)
    if args.doctest_labels:
        with open(args.doctest_labels, encoding="utf-8") as f:
            files, dropped = covered_files(files, f.readlines())
        for path in dropped:
            # stderr, so --list-changed output stays a plain file list.
            print(
                f"{path}: no CPU doctest() target covers it; not run", file=sys.stderr
            )
    if args.list_changed:
        print("\n".join(files))
        return 0
    if not files:
        print("No modules changed; no docstring examples to run.")
        return 0

    failures = []
    ran = skipped = 0
    # Docstrings skipped for a missing library, by library.
    needs = collections.Counter()
    for path in files:
        with open(path, encoding="utf-8") as f:
            docstrings = extract(f.read(), path)
        if not docstrings:
            print(f"{path}: no >>> examples")
            continue
        runnable = [d for d in docstrings if not is_skipped(d)]
        try:
            module = importlib.import_module(module_name(path))
        except Exception as e:  # noqa: BLE001 - report any import failure.
            library = missing_library(e)
            if library is None:
                failures.append(
                    f"{path}: importing {module_name(path)} failed\n"
                    f"{traceback.format_exc()}"
                )
                print(f"FAIL {path} (import)")
            else:
                needs[library] += len(runnable)
                print(f"SKIP {path} (module needs {library})")
            skipped += len(docstrings) - len(runnable)
            continue
        for docstring in docstrings:
            if is_skipped(docstring):
                skipped += 1
                hidden = hidden_outputs(docstring)
                note = (
                    f", leaves {hidden} example(s) with expected output unchecked"
                    if hidden
                    else ""
                )
                print(f"SKIP {path}:{docstring.qualname} (+SKIP_EXAMPLE{note})")
                continue
            try:
                with library_settings(module_name(path)):
                    failure = run_docstring(docstring, vars(module), path)
            except MissingLibrary as e:
                needs[e.library] += 1
                print(f"SKIP {path}:{docstring.qualname} (needs {e.library})")
                continue
            ran += 1
            status = "FAIL" if failure else "PASS"
            print(f"{status} {path}:{docstring.qualname}")
            if failure:
                failures.append(failure)
        _shutdown_ray()

    missing = ""
    if needs:
        detail = ", ".join(f"{n} need {lib}" for lib, n in sorted(needs.items()))
        missing = (
            f", {sum(needs.values())} skipped for a library this image omits "
            f"({detail})"
        )
    print(
        f"\n{ran} docstring(s) run, {len(failures)} failed, {skipped} skipped"
        f"{missing}, in {len(files)} file(s)."
    )
    for failure in failures:
        print("\n" + failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
