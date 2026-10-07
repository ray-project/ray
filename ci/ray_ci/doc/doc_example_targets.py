"""Map changed documentation files to the exact doc example tests they feed.

This backs the `docs-example-test` pull-request label. A doc-side edit runs no
docs example tests by default. With the label, the opt-in step in
.buildkite/others.rayci.yml runs only the Bazel test targets that name a
changed file directly, for every team, in one environment. It never falls back
to a library's whole docs example suite.

A changed file maps to a target only through a direct reference:

  - `srcs` or `data` that names the file itself. A file a target reaches only
    through a filegroup, such as a directory of notebooks, doesn't count.
  - The `--path` argument of a notebook test, which is how notebook targets
    select their one notebook out of a shared filegroup.

Three kinds of match are reported instead of run:

  - Library-wide doctest targets, which take every prose file of a library in
    one `doctest()` target. Running one means running the whole library's prose
    doctests. Only per-file `doctest_each()` targets are run.
  - GPU targets. Docs examples never need GPU tests in premerge or microcheck.
    GPU coverage for docs examples is the postmerge build.
  - Targets carrying a tag the opt-in step passes to test_in_docker as
    --except-tags. test_in_docker would drop them after selection, so they're
    reported here instead of logged as running.
"""

import dataclasses
import os
import xml.etree.ElementTree as ET
from typing import Dict, FrozenSet, Iterable, List, Optional, Set

# Only files under these prefixes are doc example inputs. Root-level files
# under doc/, such as the shared notebook runner doc/test_myst_doc.py, are test
# harness rather than examples, and BUILD files define targets rather than feed
# them. Both keep their default test routing in .buildkite/test.rules.txt.
DOC_EXAMPLE_PREFIXES = ("doc/source/", "doc/external/")

GPU_TAGS = frozenset({"gpu", "multi_gpu", "multi_gpu_4", "custom_vllm_plugin"})

_PROSE_SUFFIXES = (".md", ".rst")


@dataclasses.dataclass(frozen=True)
class DocTestTarget:
    label: str
    tags: FrozenSet[str]
    # Repo-relative paths of the files this target names directly.
    inputs: FrozenSet[str]

    @property
    def team(self) -> Optional[str]:
        for tag in self.tags:
            if tag.startswith("team:"):
                return tag[len("team:") :]
        return None

    @property
    def is_gpu(self) -> bool:
        return bool(self.tags & GPU_TAGS)

    @property
    def is_library_wide_doctest(self) -> bool:
        prose = [path for path in self.inputs if path.endswith(_PROSE_SUFFIXES)]
        return "doctest" in self.tags and len(prose) > 1


@dataclasses.dataclass
class Selection:
    # Labels to run, keyed by team.
    runnable: Dict[str, List[str]]
    # Changed file -> labels skipped because they need a GPU.
    gpu: Dict[str, List[str]]
    # Changed file -> library-wide doctest labels that take it.
    library_wide: Dict[str, List[str]]
    # Changed file -> "label (tags)" entries skipped by --except-tags.
    excluded: Dict[str, List[str]]
    # Changed doc example inputs that no test names directly.
    unmatched: List[str]


def label_to_path(label: str) -> Optional[str]:
    """Convert a main-repo label such as //doc:source/a.py to doc/source/a.py.

    Returns None for external labels, which can't be a changed file.
    """
    if not label.startswith("//"):
        return None
    package, _, name = label[len("//") :].partition(":")
    if not name:
        name = package.rsplit("/", 1)[-1]
    return f"{package}/{name}" if package else name


def _notebook_path(package: str, args: List[str]) -> Optional[str]:
    """Return the repo-relative notebook a test_myst_doc.py target runs."""
    if "--path" not in args:
        return None
    index = args.index("--path")
    if index + 1 >= len(args):
        return None
    path = args[index + 1]
    # doc/BUILD.bazel passes repo-relative paths. py_test_run_all_notebooks
    # passes paths relative to the package that defines the target.
    if path.startswith("doc/"):
        return path
    return f"{package}/{path}"


def parse_query_xml(xml_text: str) -> List[DocTestTarget]:
    """Parse `bazel query --output=xml` output for test rules."""
    targets = []
    for rule in ET.fromstring(xml_text).iter("rule"):
        label = rule.get("name")
        package = label[len("//") :].partition(":")[0]
        lists = {
            node.get("name"): [child.get("value") for child in node]
            for node in rule.findall("list")
        }
        inputs = set()
        for attr in ("srcs", "data"):
            for value in lists.get(attr, []):
                path = label_to_path(value)
                if path:
                    inputs.add(path)
        notebook = _notebook_path(package, lists.get("args", []))
        if notebook:
            inputs.add(notebook)
        targets.append(
            DocTestTarget(
                label=label,
                tags=frozenset(lists.get("tags", [])),
                inputs=frozenset(inputs),
            )
        )
    return targets


def is_doc_example_input(path: str) -> bool:
    return path.startswith(DOC_EXAMPLE_PREFIXES) and os.path.basename(path) not in (
        "BUILD",
        "BUILD.bazel",
    )


def select(
    changed_files: Iterable[str],
    targets: Iterable[DocTestTarget],
    except_tags: Iterable[str] = (),
) -> Selection:
    """Pick the targets that name each changed doc example input directly.

    `except_tags` is the --except-tags list the opt-in step passes to
    test_in_docker. A target carrying one of them is reported, not run.
    """
    targets = list(targets)
    except_tags = frozenset(except_tags)
    runnable: Dict[str, Set[str]] = {}
    gpu: Dict[str, List[str]] = {}
    library_wide: Dict[str, List[str]] = {}
    excluded: Dict[str, List[str]] = {}
    unmatched: List[str] = []

    for path in sorted(set(changed_files)):
        if not is_doc_example_input(path):
            continue
        consumers = [target for target in targets if path in target.inputs]
        if not consumers:
            unmatched.append(path)
            continue
        for target in consumers:
            if target.is_gpu:
                gpu.setdefault(path, []).append(target.label)
            elif target.is_library_wide_doctest:
                library_wide.setdefault(path, []).append(target.label)
            elif target.tags & except_tags:
                tags = ", ".join(sorted(target.tags & except_tags))
                excluded.setdefault(path, []).append(f"{target.label} ({tags})")
            else:
                team = target.team or "none"
                runnable.setdefault(team, set()).add(target.label)

    return Selection(
        runnable={team: sorted(labels) for team, labels in runnable.items()},
        gpu=gpu,
        library_wide=library_wide,
        excluded=excluded,
        unmatched=unmatched,
    )


def format_report(selection: Selection) -> str:
    """Describe what runs, what doesn't, and why, for the build log."""
    lines = []
    for team, labels in sorted(selection.runnable.items()):
        lines.append(f"Running ({team}): " + ", ".join(labels))
    for path, labels in sorted(selection.gpu.items()):
        lines.append(
            f"Not run, GPU only: {path} -> {', '.join(labels)}. Docs examples "
            "never run GPU tests in premerge or microcheck; postmerge covers them."
        )
    for path, labels in sorted(selection.library_wide.items()):
        lines.append(
            f"Not run, no per-file test: {path} is tested only by the "
            f"library-wide target {', '.join(labels)}."
        )
    for path, labels in sorted(selection.excluded.items()):
        lines.append(
            f"Not run, excluded by tag: {path} -> {', '.join(labels)}. The "
            "opt-in step passes these tags to test_in_docker as --except-tags."
        )
    for path in selection.unmatched:
        lines.append(f"No test names this file: {path}")
    return "\n".join(lines)
