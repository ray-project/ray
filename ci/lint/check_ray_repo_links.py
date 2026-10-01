#!/usr/bin/env python3
"""Check that links to files on Ray's master branch point at files that exist.

Docs, READMEs, and docstrings often link to a file in this repo by its GitHub
URL, such as ``https://github.com/ray-project/ray/blob/master/<path>`` or
``https://raw.githubusercontent.com/ray-project/ray/master/<path>``. GitHub
doesn't redirect those URLs when a file moves or is deleted, and it doesn't
resolve them through symlinked directories, so the link 404s once the change
merges.

A network check can't catch this on a pull request: the PR's new links point at
paths that don't exist on master until the PR merges. But whether such a link
is correct only depends on whether ``<path>`` exists in the tree being
committed, so this check resolves every link against ``git ls-files`` with no
network access. It scans the whole repository, not only changed files, because
moving a file breaks links in files the change never touched.

Links to other branches, tags, and commit SHAs are pinned and out of scope.
Line anchors such as ``#L42`` aren't checked.

Usage:
    python ci/lint/check_ray_repo_links.py
"""

import re
import subprocess
import sys
import urllib.parse

# Cheap prefilter for `git grep`. The precise match happens in LINK below.
GREP_PATTERN = r"ray-project/ray/(blob|tree|raw|refs|master)"

LINK = re.compile(
    r"https?://"
    r"(?:github\.com/ray-project/ray/(?:blob|tree|raw)|raw\.githubusercontent\.com/ray-project/ray)"
    r"/(?:refs/heads/)?master/"
    # A path runs until whitespace or a character that ends a URL in Markdown,
    # rST, HTML, Python strings, or JSON-encoded notebook text.
    r"([^\s\"'`()<>\[\]{}|\\^]+)"
)

# Characters that mark a templated path, such as an f-string or a placeholder,
# rather than a literal one.
TEMPLATE_CHARS = set("$*")


def tracked_paths() -> tuple:
    """Return the tracked files and every directory that contains one.

    Returns:
        A ``(files, dirs)`` pair of sets of repo-relative paths.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z"], check=True, capture_output=True
    ).stdout.decode()
    files = {path for path in out.split("\0") if path}
    dirs = set()
    for path in files:
        parts = path.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            dirs.add("/".join(parts[:i]))
    return files, dirs


def candidate_lines() -> list:
    """Return ``(file, line_number, text)`` for each tracked line that might link.

    Returns:
        Lines from text files that mention a ray-project/ray branch URL.
    """
    result = subprocess.run(
        ["git", "grep", "-n", "-I", "-E", "-z", GREP_PATTERN],
        capture_output=True,
    )
    # git grep exits 1 when nothing matches.
    if result.returncode not in (0, 1):
        sys.stderr.write(result.stderr.decode())
        sys.exit(2)
    lines = []
    for record in result.stdout.decode("utf-8", "replace").splitlines():
        # With -z, the separators after the file name and line number are NULs.
        path, lineno, text = record.split("\0", 2)
        lines.append((path, int(lineno), text))
    return lines


def normalize(raw: str) -> str:
    """Return the repo path a link's path segment refers to.

    Args:
        raw: The text after ``master/`` in the URL.

    Returns:
        The path with any fragment, query, and trailing punctuation removed.
    """
    path = re.split(r"[#?]", raw, maxsplit=1)[0]
    path = path.rstrip(".,;:!")
    return urllib.parse.unquote(path).rstrip("/")


def find_broken() -> list:
    """Return ``(file, line_number, url, path)`` for each link to a missing path."""
    files, dirs = tracked_paths()
    broken = []
    for source, lineno, text in candidate_lines():
        for match in LINK.finditer(text):
            path = normalize(match.group(1))
            if not path or TEMPLATE_CHARS & set(path):
                continue
            if path not in files and path not in dirs:
                broken.append((source, lineno, match.group(0), path))
    return broken


def main() -> int:
    broken = find_broken()
    for source, lineno, url, path in broken:
        print(f"{source}:{lineno}: {url}\n    {path} doesn't exist in this tree.")
    if broken:
        print(
            f"\n{len(broken)} link(s) point at files that don't exist on master "
            "once this change lands. Point each one at the file's current path, "
            "or remove it if the file is gone."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
