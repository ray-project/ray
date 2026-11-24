"""Fail when doc/source/ gains a notebook or a doc_code script no page shows.

Two checks run against the tracked files of the whole tree, not a diff, so a
page edit that drops the last `literalinclude` of a script fails too:

1. Every `.ipynb` under doc/source/ must be on the notebook allowlist. New
   pages are MyST Markdown (`.md`).
2. Every `.py` under a `doc_code/` directory in doc/source/ must be shown to
   readers, or be on the doc_code allowlist. A script counts as shown when any
   of the following holds:
   - A page under doc/source/ pulls it in with `literalinclude` or `include`.
   - A shown script imports it.
   - It's a test wrapper: it imports a script a page includes, or names, as a
     string literal, a file a page includes, such as a script that loads and
     deploys an included YAML config.

Both allowlists hold existing debt that's being burned down. The check also
fails on a stale entry, a listed file that's gone or now shown, so each list
can only shrink. When a list is empty, delete it.

Run `python doc/test_doc_assets.py --update` to rewrite both allowlists from
the current tree. Use it to re-seed after a rebase, never to wave a new file
through.
"""

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE = "doc/source"
ALLOWLIST_DIR = "doc/asset_allowlists"
NOTEBOOK_ALLOWLIST = f"{ALLOWLIST_DIR}/notebooks.txt"
DOC_CODE_ALLOWLIST = f"{ALLOWLIST_DIR}/doc_code.txt"

PAGE_SUFFIXES = (".md", ".rst", ".txt", ".ipynb")

# RST `.. literalinclude:: path` and `.. include:: path`, and the MyST fence
# and colon-fence forms ```{literalinclude} path and :::{include} path.
INCLUDE_RE = re.compile(
    r"(?:\.\.\s+(?:literalinclude|include)::"
    r"|(?:`{3,}|:{3,})\s*\{(?:literalinclude|include)\})"
    r"[ \t]*(\S+)"
)


def _tracked_files() -> List[str]:
    out = subprocess.check_output(
        [
            "git",
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            SOURCE,
        ],
        cwd=REPO_ROOT,
        text=True,
        encoding="utf-8",
    )
    # --cached still lists a tracked file deleted from the working tree.
    return sorted(p for p in out.split("\0") if p and (REPO_ROOT / p).exists())


def _norm(path: str) -> str:
    return os.path.normpath(path).replace(os.sep, "/")


def _resolve(page: str, target: str) -> str:
    """Resolve an include target the way Sphinx does: `/` is doc/source/."""
    target = target.strip().strip("\"'")
    if target.startswith("/"):
        return _norm(f"{SOURCE}/{target.lstrip('/')}")
    return _norm(f"{os.path.dirname(page)}/{target}")


def _page_text(path: str) -> str:
    text = (REPO_ROOT / path).read_text(encoding="utf-8", errors="replace")
    if not path.endswith(".ipynb"):
        return text
    try:
        cells = json.loads(text).get("cells", [])
    except ValueError:
        return ""
    return "\n".join(
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in cells
        if c.get("cell_type") == "markdown"
    )


def _included_files(pages: Iterable[str]) -> Set[str]:
    included = set()
    for page in pages:
        for match in INCLUDE_RE.finditer(_page_text(page)):
            included.add(_resolve(page, match.group(1)))
    return included


def _module_candidates(importer: str, module: str, level: int) -> List[str]:
    """Paths a module name might refer to, relative to the importing file.

    Doc scripts run with their own directory on sys.path, so an absolute
    import resolves against the importer's directory or one of its ancestors
    up to doc/source/. A relative import resolves against its package.
    """
    parts = module.split(".") if module else []
    here = os.path.dirname(importer)
    if level:
        bases = [_norm(os.path.join(here, *[".."] * (level - 1)))]
    else:
        bases = []
        d = here
        while d.startswith(SOURCE):
            bases.append(d)
            d = os.path.dirname(d)
    out = []
    for base in bases:
        stem = "/".join([base] + parts)
        out += [f"{stem}.py", f"{stem}/__init__.py"]
    return out


def _references(path: str) -> Tuple[Set[str], Set[str]]:
    """Return the files a script imports and the string literals it holds."""
    try:
        tree = ast.parse(
            (REPO_ROOT / path).read_text(encoding="utf-8", errors="replace")
        )
    except SyntaxError:
        return set(), set()
    imports, strings = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.update(_module_candidates(path, alias.name, 0))
        elif isinstance(node, ast.ImportFrom):
            imports.update(_module_candidates(path, node.module or "", node.level))
            # `from pkg import submodule` imports pkg/submodule.py.
            for alias in node.names:
                sub = f"{node.module}.{alias.name}" if node.module else alias.name
                imports.update(_module_candidates(path, sub, node.level))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            strings.add(node.value)
    return imports, strings


def _shown_doc_code(scripts: List[str], included: Set[str]) -> Set[str]:
    refs = {s: _references(s) for s in scripts}
    shown = {s for s in scripts if s in included}
    # Test wrappers: the script imports a file a page includes, or names one
    # in a string literal that resolves from the script's directory.
    for s, (imports, strings) in refs.items():
        here = os.path.dirname(s)
        if imports & included or any(
            _norm(f"{here}/{lit}") in included
            for lit in strings
            if lit and "\n" not in lit and len(lit) < 200
        ):
            shown.add(s)
    # Anything a shown script imports is shown, transitively.
    frontier = list(shown)
    while frontier:
        imports, _ = refs.get(frontier.pop(), (set(), set()))
        for target in imports:
            if target in refs and target not in shown:
                shown.add(target)
                frontier.append(target)
    return shown


def _read_allowlist(path: str) -> Dict[str, str]:
    """Map each listed path to its full line, so --update keeps comments."""
    entries = {}
    full = REPO_ROOT / path
    if not full.exists():
        return entries
    for line in full.read_text(encoding="utf-8").splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            entries[entry] = line
    return entries


def _write_allowlist(path: str, debt: Iterable[str], header: str) -> None:
    full = REPO_ROOT / path
    debt = sorted(debt)
    # A deleted list is paid off, so don't recreate it empty.
    if not debt and not full.exists():
        return
    old = _read_allowlist(path)
    lines = [header.rstrip("\n"), ""] if header else []
    lines += [old.get(p, p) for p in debt]
    full.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _header(path: str) -> str:
    """The comment block at the top of an allowlist, kept across --update."""
    full = REPO_ROOT / path
    if not full.exists():
        return ""
    head = []
    for line in full.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or not line.strip():
            head.append(line)
        else:
            break
    return "\n".join(head).rstrip("\n")


def _compare(name: str, debt: Set[str], allowlist: str, new_msg: str) -> List[str]:
    listed = set(_read_allowlist(allowlist))
    errors = []
    new = sorted(debt - listed)
    if new:
        errors.append(f"{new_msg}\n" + "\n".join(f"  {p}" for p in new))
    stale = sorted(listed - debt)
    if stale:
        errors.append(
            f"These {name} entries in {allowlist} are no longer debt: the file is "
            "gone or a page now shows it. Delete these lines so the list only "
            "shrinks:\n" + "\n".join(f"  {p}" for p in stale)
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--update",
        action="store_true",
        help="Rewrite both allowlists from the current tree instead of checking.",
    )
    args = parser.parse_args()

    tracked = _tracked_files()
    notebooks = {p for p in tracked if p.endswith(".ipynb")}
    scripts = [p for p in tracked if p.endswith(".py") and "/doc_code/" in p]
    included = _included_files(p for p in tracked if p.endswith(PAGE_SUFFIXES))
    orphans = set(scripts) - _shown_doc_code(scripts, included)

    if args.update:
        _write_allowlist(NOTEBOOK_ALLOWLIST, notebooks, _header(NOTEBOOK_ALLOWLIST))
        _write_allowlist(DOC_CODE_ALLOWLIST, orphans, _header(DOC_CODE_ALLOWLIST))
        print(
            f"Wrote {len(notebooks)} notebooks to {NOTEBOOK_ALLOWLIST} and "
            f"{len(orphans)} scripts to {DOC_CODE_ALLOWLIST}."
        )
        return 0

    errors = _compare(
        "notebook",
        notebooks,
        NOTEBOOK_ALLOWLIST,
        "New .ipynb notebooks under doc/source/ aren't allowed. Write the page "
        "as MyST Markdown (.md) instead:",
    )
    errors += _compare(
        "doc_code",
        orphans,
        DOC_CODE_ALLOWLIST,
        "These doc_code scripts aren't shown on any page. Pull each into a page "
        "with literalinclude, or delete it. Tested code that no page shows "
        "belongs in the library's own test suite:",
    )
    for error in errors:
        print(error + "\n", file=sys.stderr)
    if errors:
        print(
            "See 'The doc asset check' in doc/source/ray-contribute/ci.md.",
            file=sys.stderr,
        )
        return 1
    print(
        f"OK: {len(notebooks)} allowlisted notebooks, "
        f"{len(orphans)} allowlisted doc_code scripts, nothing new."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
