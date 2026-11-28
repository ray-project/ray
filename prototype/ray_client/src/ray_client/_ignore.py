"""gitignore-syntax matching for `working_dir` packaging.

Implements the pattern semantics of https://git-scm.com/docs/gitignore for the
caller's `excludes` list and for the .gitignore/.rayignore files found inside
`working_dir`. The module has no dependency on the rest of the package, so a
fork that wants simpler exclusion rules only needs to replace this file.
"""

import re
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# `[:class:]` members git's wildmatch accepts inside brackets; git tests them
# with its own ASCII-only ctype table, so these are plain ASCII ranges.
_POSIX_CLASSES = {
    "alnum": "0-9A-Za-z",
    "alpha": "A-Za-z",
    "blank": r" \t",
    "cntrl": r"\x00-\x1f\x7f",
    "digit": "0-9",
    "graph": r"\x21-\x7e",
    "lower": "a-z",
    "print": r"\x20-\x7e",
    "punct": r"!-/:-@\[-`{-~",
    "space": r" \t\n\r",
    "upper": "A-Z",
    "xdigit": "0-9A-Fa-f",
}


def _is_escaped(pattern: str, index: int) -> bool:
    """True if `pattern[index]` is preceded by an odd number of backslashes."""
    count = 0
    while index - count - 1 >= 0 and pattern[index - count - 1] == "\\":
        count += 1
    return count % 2 == 1


def _bracket_to_regex(pattern: str, start: int) -> Tuple[Optional[str], int]:
    """Translates the bracket expression opening at `pattern[start]`.

    Returns `(regex, index_after_closing_bracket)`, or `(None, len(pattern))`
    when git's wildmatch aborts the whole match: an unterminated set or an
    unknown `[:class:]`. Like wildmatch, `!` or `^` negates, a leading `]` is a
    literal member, `\\` escapes, a range whose end sorts before its start keeps
    only the start character, and a set never matches `/`.
    """
    n = len(pattern)
    i = start + 1
    negated = i < n and pattern[i] in "!^"
    if negated:
        i += 1
    members: List[str] = []
    prev: Optional[str] = None  # last literal member, usable as a range start
    first = True
    while True:
        if i >= n:
            return None, n
        ch = pattern[i]
        if ch == "]" and not first:
            break
        first = False
        if ch == "\\":
            if i + 1 >= n:
                return None, n
            prev = pattern[i + 1]
            members.append(re.escape(prev))
            i += 2
        elif ch == "-" and prev is not None and i + 1 < n and pattern[i + 1] != "]":
            end = pattern[i + 1]
            i += 2
            if end == "\\":
                if i >= n:
                    return None, n
                end = pattern[i]
                i += 1
            if end >= prev:
                members.append(f"-{re.escape(end)}")
            prev = None
        elif ch == "[" and pattern.startswith(":", i + 1):
            close = pattern.find("]", i + 2)
            if close < 0:
                return None, n
            name = pattern[i + 2 : close]
            if not name.endswith(":"):
                members.append(re.escape("["))  # no `:]`: a literal `[`
                prev = "["
                i += 1
                continue
            ranges = _POSIX_CLASSES.get(name[:-1])
            if ranges is None:
                return None, n
            members.append(ranges)
            prev = None
            i = close + 1
        else:
            members.append(re.escape(ch))
            prev = ch
            i += 1
    body = "".join(members)
    return (f"[^/{body}]" if negated else f"(?!/)[{body}]"), i + 1


def _glob_to_regex(pattern: str) -> str:
    """Translates a gitignore glob to a regex: `*` and `?` stop at `/`, `**` does not."""
    out: List[str] = []
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "\\" and i + 1 < n:
            out.append(re.escape(pattern[i + 1]))
            i += 2
        elif ch == "*":
            j = i
            while j < n and pattern[j] == "*":
                j += 1
            bounded = (i == 0 or pattern[i - 1] == "/") and (
                j == n or pattern[j] == "/"
            )
            if j - i >= 2 and bounded and j < n:
                out.append("(?:.*/)?")  # `**/`: zero or more directories
                j += 1
            elif j - i >= 2 and bounded:
                out.append(".*")  # trailing `/**`: everything inside
            else:
                out.append("[^/]*")  # any other run of asterisks is a plain `*`
            i = j
        elif ch == "?":
            out.append("[^/]")
            i += 1
        elif ch == "[":
            fragment, i = _bracket_to_regex(pattern, i)
            if fragment is None:
                return "(?!)"  # git matches nothing for the whole pattern
            out.append(fragment)
        else:
            out.append(re.escape(ch))
            i += 1
    return "".join(out)


class _IgnoreRule:
    """One gitignore-syntax pattern, scoped to the directory (`base`) that defined it."""

    __slots__ = ("regex", "negated", "dir_only", "anchored", "base")

    def __init__(
        self,
        regex: "re.Pattern[str]",
        negated: bool,
        dir_only: bool,
        anchored: bool,
        base: str,
    ):
        self.regex = regex
        self.negated = negated
        self.dir_only = dir_only
        self.anchored = anchored
        self.base = base

    def matches(self, arcname: str, is_dir: bool) -> bool:
        if (self.dir_only and not is_dir) or not arcname.startswith(self.base):
            return False
        rel = arcname[len(self.base) :]
        # A pattern without a slash matches the name at any depth below `base`.
        target = rel if self.anchored else rel.rpartition("/")[2]
        return self.regex.fullmatch(target) is not None


def _parse_ignore_rule(line: str, base: str) -> Optional[_IgnoreRule]:
    """Compiles one line of gitignore syntax (https://git-scm.com/docs/gitignore);
    returns None for blank lines, comments, and patterns that match nothing."""
    pattern = line.rstrip("\r")
    end = len(pattern)
    while end and pattern[end - 1] == " " and not _is_escaped(pattern, end - 1):
        end -= 1  # unescaped trailing spaces are not part of the pattern
    pattern = pattern[:end]
    if not pattern or pattern.startswith("#"):
        return None
    negated = pattern.startswith("!")
    if negated:
        pattern = pattern[1:]
    # Git removes exactly one slash at each end; any slash left over is matched
    # literally, so `foo//` and `//foo` can never match a path.
    dir_only = pattern.endswith("/")
    if dir_only:
        pattern = pattern[:-1]
    anchored = "/" in pattern
    if pattern.startswith("/"):
        pattern = pattern[1:]
    if not pattern:
        return None
    return _IgnoreRule(
        re.compile(_glob_to_regex(pattern)), negated, dir_only, anchored, base
    )


def _compile_ignore_rules(lines: Sequence[str], base: str) -> List[_IgnoreRule]:
    rules = (_parse_ignore_rule(line, base) for line in lines)
    return [rule for rule in rules if rule is not None]


def _ignore_rules_in(
    directory: Path, base: str, include_gitignore: bool
) -> List[_IgnoreRule]:
    """Rules from `directory`'s ignore files; .rayignore comes last so it can
    override .gitignore with `!` patterns."""
    rules: List[_IgnoreRule] = []
    for name in (".gitignore", ".rayignore") if include_gitignore else (".rayignore",):
        ignore_file = directory / name
        if ignore_file.is_file():
            # utf-8-sig drops a UTF-8 BOM, which git skips as well.
            text = ignore_file.read_text(encoding="utf-8-sig", errors="surrogateescape")
            rules.extend(_compile_ignore_rules(text.split("\n"), base))
    return rules


def _is_ignored(arcname: str, is_dir: bool, rules: Sequence[_IgnoreRule]) -> bool:
    """The last matching rule decides, as in git."""
    ignored = False
    for rule in rules:
        if rule.matches(arcname, is_dir):
            ignored = not rule.negated
    return ignored
