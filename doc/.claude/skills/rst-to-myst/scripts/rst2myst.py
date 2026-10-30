#!/usr/bin/env python3
"""Faithful, format-only RST -> MyST converter for Ray prose pages.

Implements the mapping in the rst-to-myst skill (../SKILL.md) for the constructs
that convert mechanically, and reports everything else on stderr as FLAG lines
for a human to resolve. It never guesses at a construct it doesn't model: a
FLAG means "look at this line", not "this line is wrong".

Converts:
  - `.. meta::` and a leading `:orphan:` field -> YAML frontmatter
  - `.. _label:` -> `(label)=`
  - headings, leveled by order of first appearance of each adornment style
  - container directives (admonitions, tab-set/tab-item, dropdown, grid, card,
    div) -> colon fences, outer fences one colon wider than what they hold
  - leaf directives (literalinclude, toctree, figure, image, list-table,
    testcode/testoutput/doctest, raw, ...) -> backtick fences, options as
    `:key: val` lines
  - code-block/code/sourcecode -> plain ```lang fences
  - `::` literal blocks -> plain ``` fences, with docutils' `::` rewriting
  - RST line blocks (`| text`) -> Markdown hard line breaks
  - indented block quotes -> `>` quotes (list-item continuations stay indented)
  - RST comments -> HTML comments, or MyST `%` comments when the text holds
    `-->`, which would end an HTML comment early
  - inline: ``literal``, :role:`x`, `text <url>`_ links, `<url>`_ links with no
    text, bare URLs (linkify is off in Ray's conf.py, so bare URLs are wrapped
    in <...>)
  - text MyST's `replacements` extension would rewrite and docutils doesn't:
    (c) (tm) (r), `+-`, and a `..` after text are escaped
  - paragraph lines that start with `1.`, `1)`, `-`, `*`, or `+` at the
    paragraph's own text column, with no blank line before them: RST renders
    these inline, CommonMark would start a list, so the marker is escaped
  - a one-line paragraph at column 0 over a 2-space-indented line: RST's
    definition list, converted to deflist `: ` syntax (and flagged)
  - backslash escapes that RST processes and Markdown shows literally: inside
    default-role `code`, `\\ ` (escaped space), and `\\s` after inline markup
  - backticks inside **bold**, literal in RST (no nested markup): escaped
  - prose lines that start with `#` and a space: RST renders them as text,
    CommonMark as a heading, so the `#` is escaped
  - prose lines that start with `>`: RST renders them as text, CommonMark as
    a block quote, even mid-paragraph, so the `>` is escaped
  - a list-item continuation indented past the item's content column: RST
    renders it in a block quote inside the item, so it gets a `>` prefix
  - directive option values that MyST's YAML-style option parser would change
    (a leading `#`, quote, `|`, or `>`, or a ` #` comment) are double-quoted
  - tabs, expanded to 8-column stops before anything else, as docutils does
  - toctree entries -> extensionless
  - any fence whose body holds a `>>>` prompt ends with a blank line before
    its closing backticks. The doctest macro's --doctest-glob='*.md' runs
    pytest's stock parser on the raw .md, which reads expected output up to
    the next blank line and would otherwise swallow the fence

Flags (not converted): relative .html links, named hyperlink references,
substitutions, RST simple and grid tables, other definition lists, field
lists, autodoc and unknown directives (wrapped in {eval-rst}), labels directly
above a directive that ends up inside {eval-rst}, a line indented less than a
list item's content column (RST ends the list there), doctest directives
that end up inside a `>` quote (pytest-sphinx won't collect them), a doctest
directive whose body starts with no blank line (docutils drops it), and
replacements this converter doesn't escape (4+ dots, `????`, `!!!!`, `,,`).

Keeps the source's line wrapping, so commit 1 of a conversion PR stays
line-aligned with the .rst. Soft-wrap afterwards with Ray's ray-soft-wrap skill.

Usage:
    rst2myst.py in.rst > out.md          # convert; FLAG lines go to stderr
    rst2myst.py --selftest               # run the built-in regression cases
"""
import re
import sys

ADMONITIONS = {
    "note",
    "tip",
    "warning",
    "important",
    "caution",
    "danger",
    "attention",
    "hint",
    "error",
    "seealso",
}
COLON_DIRECTIVES = ADMONITIONS | {
    "admonition",
    "tab-set",
    "tab-item",
    "dropdown",
    "grid",
    "grid-item",
    "grid-item-card",
    "card",
    "div",
}
RAW_BODY_DIRECTIVES = {
    "testcode",
    "testoutput",
    "doctest",
    "testsetup",
    "testcleanup",
    "math",
    "raw",
}
BACKTICK_DIRECTIVES = RAW_BODY_DIRECTIVES | {
    "literalinclude",
    "toctree",
    "contents",
    "figure",
    "image",
    "list-table",
    "button-ref",
    "button-link",
    "versionadded",
    "versionchanged",
    "deprecated",
    "rubric",
    "centered",
    "only",
}
CODE_DIRECTIVES = {"code-block", "code", "sourcecode"}
AUTODOC_RE = re.compile(
    r"^auto(function|class|method|attribute|data|module|"
    r"exception|summary|pydantic_model)$"
)

DIRECTIVE_RE = re.compile(r"^\.\. ([A-Za-z][\w:-]*)::(?:\s+(.*))?$")
LABEL_RE = re.compile(r"^\.\. _([^:`]+):\s*$")
OPTION_RE = re.compile(r"^:([\w-]+):(?:\s+(.*))?$")
ADORN_RE = re.compile(r"^([=\-~^\"'`#*+<>:.])\1{2,}\s*$")
LIST_MARK_RE = re.compile(r"^(?:[-*+]|\d+[.)]|#\.)\s+")
ITEM_MARK_RE = re.compile(r"^(?:[-*+]|\d+[.)]|#\.)(?:\s+|$)")  # also an empty item
PROSE = "\x01"  # marks a prose line awaiting paragraph-level inline conversion
# Leading whitespace allowed: a table indented under a list item or in a block
# quote must be flagged too (ray-core/cross-language nests two in a quoted list).
# pytest's stock doctest parser accepts an indented prompt too.
PROMPT_RE = re.compile(r"^\s*>>>")
TABLE_RE = re.compile(r"^\s*(\+[-=]+)+\+\s*$|^\s*=+( +=+)+\s*$")


class Converter:
    def __init__(self):
        self.heading_styles = []
        self.in_item = 0  # depth of list-item continuation blocks
        self.meta = {}
        self.orphan = False
        self.flags = []
        self.prompt_fences = 0
        self.targets = set()  # lowercased names of `.. _name: url` targets

    def flag(self, msg):
        self.flags.append(msg)

    def quote_doctest_flag(self, inner):
        if any(
            re.match(
                r"^`{3,}\{(testcode|testoutput|doctest|testsetup|testcleanup)\}", l
            )
            for l in inner
        ):
            self.flag(
                "doctest directive inside a > block quote: pytest-sphinx "
                "won't collect it; check where it belongs"
            )

    def fence_body(self, lines):
        """End a fence body that holds `>>>` prompts with a blank line, so the
        stock doctest parser doesn't read the closing fence as expected output."""
        if not any(PROMPT_RE.match(l) for l in lines):
            return lines
        self.prompt_fences += 1
        return lines + [""] if lines and lines[-1].strip() else lines

    # ---------------------------------------------------------------- inline
    def inline(self, text):
        stash = []

        def keep(s):
            stash.append(s)
            return f"\x00{len(stash) - 1}\x00"

        # RST inline markup doesn't nest: backticks inside **bold** are literal.
        text = re.sub(
            r"(?<![\w*])\*\*(?=\S)([^*\n]*`[^*\n]*?)(?<=\S)\*\*(?![\w*])",
            lambda m: keep("**" + m.group(1).replace("`", "\\`") + "**"),
            text,
        )
        text = re.sub(
            r"``([^`\n](?:[^`]|`(?!`))*?)``", lambda m: keep(f"`{m.group(1)}`"), text
        )
        text = re.sub(
            r"(?<![\w{]):((?:[a-z]+:)?[a-z][\w-]*):`([^`]+)`",
            lambda m: keep(f"{{{m.group(1)}}}`{m.group(2)}`"),
            text,
        )

        def link(m):
            label, target = m.group(1).strip(), m.group(2).strip()
            if not re.match(r"[a-z][a-z0-9+.-]*:", target):
                self.flag(
                    f"relative link needs a {{doc}}/{{ref}} role, not a "
                    f"transcribed target: `{label} <{target}>`"
                )
            return keep(f"[{self.replacements(label)}]({target})")

        def uri_only(m):
            # `<url>`_: an embedded URI with no text renders the URI as its text.
            target = m.group(1).strip()
            if not re.match(r"[a-z][a-z0-9+.-]*:", target):
                self.flag(
                    f"relative link needs a {{doc}}/{{ref}} role, not a "
                    f"transcribed target: `<{target}>`"
                )
                return keep(f"[{target}]({target})")
            return keep(f"<{target}>")

        text = re.sub(r"`<([^>`]+)>`__?", uri_only, text)
        text = re.sub(r"`([^`<]+?)\s*<([^>`]+)>`__?", link, text)
        refs = re.findall(r"`[^`]+`__?(?![\w`])", text)
        if refs:
            self.flag(
                "named hyperlink reference (inline the target): "
                + ", ".join(r[:60] for r in refs)
            )
        # The bare-word form (`Modin_`) needs no backticks. Flag it only for a
        # name this document defines as a target, so `snake_case_` text doesn't.
        bare = [
            m.group(0)
            for m in re.finditer(r"(?<![\w`])([A-Za-z][\w.-]*?)_(?![\w`])", text)
            if m.group(1).lower() in self.targets
        ]
        if bare:
            self.flag(
                "named hyperlink reference (inline the target): " + ", ".join(bare)
            )
        if re.search(r"(?<![\w|])\|[\w][\w -]*\|(?![\w|])", text):
            self.flag(
                f"possible substitution (no MyST equivalent; inline it): "
                f"{text.strip()[:80]}"
            )
        # Bare URLs; trailing sentence punctuation stays outside the link,
        # as docutils' standalone-URI recognizer leaves it. A URL in parentheses
        # is a link in docutils too. One directly after `*` sits inside emphasis
        # or bold, where RST markup doesn't nest, so it renders as plain text.
        text = re.sub(
            r"(?<![<\[`\x00\w*])(?<!\]\()(https?://[^\s)>\]`]*[^\s)>\]`.,;:!?])",
            lambda m: keep(f"<{m.group(1)}>"),
            text,
        )
        text = self.replacements(text)
        # Single-backtick text uses the default role (code), which processes
        # backslash escapes; a Markdown code span keeps them literally.
        text = re.sub(
            r"(?<![`\w\x00])`([^`\n]*\\[^`\n]*)`(?![`_])",
            lambda m: keep("`" + re.sub(r"\\(.)", r"\1", m.group(1)) + "`"),
            text,
        )
        text = text.replace("\\ ", "")  # RST escaped whitespace renders as nothing
        # RST has no inline HTML, so `<REGION>` is text. CommonMark reads a valid
        # tag name as raw HTML, which swallows the rest of the paragraph. Code
        # spans and autolinks are stashed, so only prose reaches this.
        parts = re.split(r"(`[^`\n]*`)", text)  # single-backtick spans stay literal
        for k in range(0, len(parts), 2):
            parts[k] = re.sub(
                r"<(/?[A-Za-z][A-Za-z0-9-]*)(?=\s[^<>]*>|/?>)", r"\\<\1", parts[k]
            )
        text = "".join(parts)
        text = re.sub(r"\x00(\d+)\x00", lambda m: stash[int(m.group(1))], text)
        # `x`\s: RST's escaped character after inline markup renders without
        # the backslash, and an escaped space renders as nothing.
        text = re.sub(r"`\\([A-Za-z])", r"`\1", text)
        return re.sub(r"`\\ ", "`", text)

    def replacements(self, text):
        """MyST's `replacements` extension (on in Ray's conf.py) rewrites some
        text that docutils leaves alone: (c) (tm) (r) become symbols, `+-`
        becomes ±, and any run of 2+ dots becomes one ellipsis. docutils'
        smartquotes already converts `...` and dashes, so those match and are
        left alone. Escape the rest outside code spans and <scheme:...> autolinks. Runs of 4+ dots,
        `????`, `!!!!`, and `,,` are flagged, not escaped: unverified."""

        def escape(line):
            # A grid-table border is passed through for hand conversion, so
            # leave it intact.
            if TABLE_RE.match(line):
                return line
            line = re.sub(r"\((c|tm|r)\)", r"\\(\1)", line, flags=re.IGNORECASE)
            line = line.replace("+-", "+\\-")
            # Only `..` that follows text (`etc..`, `0..100`). A `..` opening a
            # word is leftover RST (`- .. image::`) for a human to convert, and
            # an escape would hide it from the residual-RST check.
            line = re.sub(r"(?<=[^\s.])\.\.(?!\.)", r".\\.", line)
            m = re.search(r"\.{4,}|[?!]{4,}|,,", line)
            if m:
                self.flag(
                    f"MyST's replacements extension rewrites {m.group()!r}, which "
                    f"docutils may not: check the render"
                )
            return line

        parts = re.split(r"(`[^`\n]*`|<[a-z][a-z0-9+.-]*:[^>\s]*>)", text)
        for k in range(0, len(parts), 2):
            parts[k] = "\n".join(escape(l) for l in parts[k].split("\n"))
        return "".join(parts)

    def heading_level(self, style):
        if style not in self.heading_styles:
            self.heading_styles.append(style)
        return self.heading_styles.index(style) + 1

    # ---------------------------------------------------------------- blocks
    def convert(self, lines):
        """Convert a dedented block of RST lines. Returns (md_lines, colons)."""
        out, depth, i, n = [], 0, 0, len(lines)
        para_start = None  # first line of the current prose paragraph
        list_col = None  # content column of the most recent list item
        literal_next = False  # previous paragraph ended with `::`

        def end_para():
            nonlocal para_start
            para_start = None

        while i < n:
            line = lines[i].rstrip()

            if not line.strip():
                out.append("")
                end_para()
                i += 1
                continue

            ind = indent_of(line)
            if ind == 0 and para_start is None and not LIST_MARK_RE.match(line):
                list_col = None

            # ---- indented block: literal block, list continuation, or quote
            if ind > 0 and (i == 0 or not lines[i - 1].strip()):
                j = block_end(lines, i)
                blk, trail = split_trailing_blanks(lines[i:j])
                if literal_next:
                    out += (
                        ["```"] + self.fence_body(dedent(blk)) + ["```"] + [""] * trail
                    )
                    literal_next = False
                elif list_col is not None and ind >= list_col:
                    k = next(
                        (
                            x
                            for x in range(i, j)
                            if lines[x].strip() and indent_of(lines[x]) < list_col
                        ),
                        None,
                    )
                    if k is not None:
                        # RST ends the list at the shallower line and renders it
                        # in a block quote after the list, then restarts the list.
                        self.flag(
                            f"line indented less than the list item's content "
                            f"column ends the list in RST (block quote, then a "
                            f"restarted list): {lines[k].strip()[:60]}"
                        )
                        j = k
                        blk, trail = split_trailing_blanks(lines[i:j])
                    # Continuation of a list item: convert it like any block,
                    # then indent it back to the item's content column so a
                    # nested fence stays inside the item.
                    self.in_item += 1
                    inner, d = self.convert(dedent(blk))
                    self.in_item -= 1
                    depth = max(depth, d)
                    if ind > list_col:
                        # RST: a block indented past the item's content column
                        # is a block quote inside the item.
                        self.quote_doctest_flag(inner)
                        inner = [("> " + l) if l else ">" for l in inner]
                    out += [(" " * list_col + l) if l else "" for l in inner] + [
                        ""
                    ] * trail
                else:
                    inner, d = self.convert(dedent(blk))
                    depth = max(depth, d)
                    self.quote_doctest_flag(inner)
                    out += [("> " + l) if l else ">" for l in inner] + [""] * trail
                    list_col = None
                end_para()
                i = j
                continue
            deflist_open = False
            if ind > 0 and para_start is not None and list_col is None:
                if LIST_MARK_RE.match(lines[i - 1].strip()) is None:
                    if (
                        ind == 2
                        and lines[i - 1] is para_start
                        and indent_of(para_start) == 0
                    ):
                        # RST: a one-line paragraph followed by an indented line
                        # with no blank line is a definition list. MyST's deflist
                        # opens the definition with `: `; its content column is 2.
                        deflist_open = True
                        self.flag(
                            f"definition list converted (`: `); check the render: "
                            f"{para_start.strip()[:50]}"
                        )
                    else:
                        self.flag(
                            f"indented line right after prose (definition "
                            f"list?): {line.strip()[:60]}"
                        )
            elif (
                ind > 0
                and list_col is not None
                and ind > list_col
                and LIST_MARK_RE.match(lines[i - 1].lstrip())
            ):
                # RST: a line indented past the item's text with no blank line
                # makes the item a definition list (term = the item text).
                self.flag(
                    f"indented line right after a list item (definition "
                    f"list inside the item; use deflist `: `): {line.strip()[:60]}"
                )
            literal_next = False

            # ---- grid table: convert when every cell is one line, else pass
            # through verbatim (a `|` row must never reach the line-block branch).
            if line.startswith("+-") and para_start is None:
                j = i
                while j < n and lines[j].rstrip()[:1] in ("+", "|"):
                    j += 1
                block = [l.rstrip() for l in lines[i:j]]
                grid = self.grid_table(block)
                if grid:
                    out += grid
                    self.flag(
                        f"grid table -> {{list-table}}; check it in the render "
                        f"diff: {line[:40]}"
                    )
                else:
                    out += block
                    self.flag(
                        f"RST grid table with multi-line or spanning cells, "
                        f"passed through (convert to {{list-table}} by hand): {line[:40]}"
                    )
                end_para()
                i = j
                continue
            # ---- tables and field lists: flag, pass through
            if TABLE_RE.match(line):
                self.flag(f"RST table (convert to {{list-table}}): {line[:40]}")
            if para_start is None and OPTION_RE.match(line) and ind == 0:
                self.flag(f"field list line: {line[:60]}")

            # ---- overline + title + underline
            if (
                ADORN_RE.match(line)
                and i + 2 < n
                and lines[i + 1].strip()
                and ADORN_RE.match(lines[i + 2].rstrip())
                and lines[i + 2].strip()[0] == line[0]
            ):
                lvl = self.heading_level(("over", line[0]))
                out.append("#" * lvl + " " + self.inline(lines[i + 1].strip()))
                i += 3
                end_para()
                list_col = None
                continue
            # ---- title + underline
            if (
                ind == 0
                and i + 1 < n
                and ADORN_RE.match(lines[i + 1].rstrip())
                and len(lines[i + 1].rstrip()) >= len(line) - 1
                and not DIRECTIVE_RE.match(line)
                and para_start is None
            ):
                lvl = self.heading_level(("under", lines[i + 1].strip()[0]))
                out.append("#" * lvl + " " + self.inline(line.strip()))
                i += 2
                end_para()
                list_col = None
                continue
            # ---- label
            m = LABEL_RE.match(line)
            if m:
                out.append(f"({m.group(1)})=")
                nxt = next((l for l in lines[i + 1 :] if l.strip()), "")
                dm = DIRECTIVE_RE.match(nxt.rstrip())
                if nxt.startswith(".. ") and not dm and not LABEL_RE.match(nxt):
                    self.flag(
                        f"label ({m.group(1)}) sits above a comment, which can't "
                        f"carry its id: move the comment above the label"
                    )
                if dm and self.directive_kind(dm.group(1)) == "eval-rst":
                    self.flag(
                        f"label ({m.group(1)}) sits above a directive that "
                        f"becomes eval-rst: move it inside the block"
                    )
                i += 1
                continue
            # ---- directive
            m = DIRECTIVE_RE.match(line)
            if m and ind == 0:
                j = block_end(lines, i + 1)
                body, trail = split_trailing_blanks(lines[i + 1 : j])
                block, d = self.directive(
                    m.group(1), (m.group(2) or "").strip(), dedent(body)
                )
                out += block + [""] * trail
                depth = max(depth, d)
                end_para()
                list_col = None
                i = j
                continue
            # ---- line block
            if re.match(r"^\|( |$)", line):
                j = i
                lb = []
                while j < n and re.match(r"^\|( |$)", lines[j].rstrip()):
                    lb.append(self.inline(lines[j].rstrip()[2:].strip()))
                    j += 1
                out += [l + "\\" for l in lb[:-1]] + [lb[-1]]
                self.flag(
                    "line block -> Markdown hard breaks: renders <p>+<br>, "
                    "not div.line-block (an explainable render diff)"
                )
                i = j
                continue
            # ---- named external target (`.. _name: url`): no MyST form. Uses
            # are flagged separately; drop the definition, which renders nothing.
            tm = re.match(r"^\.\. _(`[^`]+`|[^:`]+): *(\S+)\s*$", line)
            if tm:
                self.flag(
                    f"named target definition dropped (inline its uses): {line.strip()}"
                )
                i += 1
                continue
            # ---- RST comment
            if line.startswith(".. ") or line == "..":
                j = block_end(lines, i + 1)
                com, trail = split_trailing_blanks(lines[i:j])
                first = com[0][3:].strip()
                rest = [l.rstrip() for l in com[1:]]
                # An HTML comment in a list item is a raw node, which makes
                # docutils drop the list's `simple` class. `%` renders nothing.
                if any("-->" in l for l in com) or self.in_item:
                    # `-->` would end an HTML comment early and leak the rest onto
                    # the page. MyST `%` comments render nothing, like RST's.
                    out += [f"% {first}".rstrip()] + [
                        f"% {l}".rstrip() if l.strip() else "%" for l in dedent(rest)
                    ]
                elif rest:
                    out += ["<!-- " + first] + rest[:-1] + [rest[-1] + " -->"]
                else:
                    out.append(f"<!-- {first} -->")
                out += [""] * trail
                i = j
                continue

            # ---- prose line
            text = line
            # RST: `#.` is an auto-numbered enumerator. CommonMark has no such
            # marker and renders `#.` as text, so number it `1.`: a run of `1.`
            # items renders 1, 2, 3, the same <ol> docutils emits.
            text = re.sub(r"^(\s*)#\.(\s|$)", r"\g<1>1.\2", text)
            # RST: an enumerator opening a paragraph whose next line is neither
            # indented, blank, nor another enumerator is paragraph text, not a
            # list. CommonMark: any `N.` after a blank line starts a list.
            em = (
                re.match(r"^(\s*)(\d+)([.)])(\s)", text) if para_start is None else None
            )
            nxt = lines[i + 1].rstrip() if i + 1 < n else ""
            if (
                em
                and nxt.strip()
                and indent_of(nxt) <= ind
                and not ITEM_MARK_RE.match(nxt.lstrip())
            ):
                text = em.group(1) + em.group(2) + "\\" + text[em.end(2) :]
                self.flag(
                    f"enumerator with an unindented next line is paragraph text in RST; "
                    f"escaped: {line.strip()[:40]}"
                )
            elif LIST_MARK_RE.match(text.lstrip()):
                # Marker columns of the paragraph's first line (`* - A` has two),
                # and the column its text starts at.
                marks, cont_col = [], None
                if para_start is not None:
                    cont_col = indent_of(para_start)
                    while mk := ITEM_MARK_RE.match(para_start[cont_col:]):
                        marks.append(cont_col)
                        cont_col += mk.end()
                if cont_col is not None and ind == cont_col and ind not in marks:
                    # RST: a marker at the paragraph's own text column, with no
                    # blank line before it, is paragraph text. CommonMark: `-`,
                    # `*`, `+`, `1.`, `1)` start a list, so escape the marker.
                    # Every bullet can interrupt a paragraph; only a `1` can.
                    mm = re.match(r"^(\s*)(?:([-*+])|(1)([.)]))(\s)", text)
                    if mm:
                        mark = mm.group(2) or (mm.group(3) + mm.group(4))
                        esc = (
                            ("\\" + mark)
                            if mm.group(2)
                            else (mark[:-1] + "\\" + mark[-1])
                        )
                        text = mm.group(1) + esc + text[mm.end(1) + len(mark) :]
                elif cont_col is not None and ind > cont_col:
                    pass  # a definition list (term = the paragraph); flagged above
                elif ind == 0 or list_col is not None:
                    list_col = ind + LIST_MARK_RE.match(text.lstrip()).end()
                    para_start = line  # a sibling item starts its own paragraph
            if deflist_open:
                text = ": " + text[2:]
                list_col = 2  # later indented blocks belong to the definition
            # RST: a prose line starting with `#` is text. CommonMark: `# x` is an
            # ATX heading, even mid-paragraph. Escape the marker to keep the text.
            text = re.sub(r"^(\s*)(#{1,6})(\s|$)", r"\1\\\2\3", text)
            # Likewise `>` opens a block quote, even mid-paragraph. A `>>>` line
            # is an RST doctest block, which this converter doesn't model.
            text = re.sub(r"^(\s*)>(?!>>)", r"\1\\>", text)
            if re.search(r"(^|[^:]|\s)::$", text) and not DIRECTIVE_RE.match(text):
                literal_next = True
                if text.strip() == "::":
                    text = None
                elif re.search(r"\s::$", text):
                    text = text[:-3].rstrip()
                else:
                    text = text[:-1]
            if text is not None:
                if deflist_open:
                    out.append(PROSE + text)  # a new paragraph for inline conversion
                    para_start = line
                    i += 1
                    continue
                out.append(PROSE + text)  # inline-converted per paragraph below
            if para_start is None:
                para_start = line
            i += 1
        return self.flush_prose(out), depth

    def flush_prose(self, out):
        """Inline-convert each run of prose lines as one string, so a role or
        link that wraps across a line break (`:ref:`text\\nmore <x>``) converts."""
        res, run = [], []
        for l in out + [None]:
            if l is not None and l.startswith(PROSE):
                run.append(l[len(PROSE) :])
                continue
            if run:
                res += self.inline("\n".join(run)).split("\n")
                run = []
            if l is not None:
                res.append(l)
        return res

    def directive_kind(self, name):
        if name == "meta" or name in CODE_DIRECTIVES or name == "toctree":
            return name
        if name in BACKTICK_DIRECTIVES:
            return "backtick"
        if name in COLON_DIRECTIVES:
            return "colon"
        return "eval-rst"

    def directive(self, name, arg, body):
        opts, k = [], 0
        # The body is dedented to its shallowest line, so an option line indented
        # deeper than the body (`    :options: +MOCK` over a 3-space body) keeps
        # leading spaces. docutils still reads it as an option.
        while k < len(body) and OPTION_RE.match(body[k].strip()):
            opts.append(body[k].strip())
            k += 1
        rest = strip_blank_edges(body[k:])
        kind = self.directive_kind(name)
        if kind != "eval-rst":
            opts = [myst_option(o) for o in opts]
        if (
            name in RAW_BODY_DIRECTIVES - {"math", "raw"}
            and not arg
            and k == 0
            and body
            and body[0].strip()
        ):
            self.flag(
                f"`.. {name}::` has no blank line before its body, so docutils "
                f"reads the first block as the directive's argument and renders "
                f"none of it; /en/master drops it, the .md renders it: "
                f"{body[0].strip()[:50]}"
            )

        if kind == "meta":
            for o in opts:
                om = OPTION_RE.match(o)
                self.meta[om.group(1)] = (om.group(2) or "").strip()
            return [], 0
        if kind in CODE_DIRECTIVES:
            if opts:
                return (
                    ["```{code-block} " + arg]
                    + opts
                    + [""]
                    + self.fence_body(rest)
                    + ["```"],
                    0,
                )
            return ["```" + arg] + self.fence_body(rest) + ["```"], 0
        if kind == "toctree":
            entries = [re.sub(r"\.(rst|md)$", "", e.strip()) for e in rest if e.strip()]
            return (
                ["```{toctree}"]
                + opts
                + ([""] if opts and entries else [])
                + entries
                + ["```"],
                0,
            )
        if kind == "backtick":
            head = "```{%s}%s" % (name, (" " + arg) if arg else "")
            content = rest
            if name not in RAW_BODY_DIRECTIVES and content:
                content = self.convert(content)[0]
            if name == "list-table":
                content = self.cell_line_blocks(content)
            return (
                [head]
                + opts
                + ([""] if opts and content else [])
                + self.fence_body(content)
                + ["```"],
                0,
            )
        if kind == "colon":
            if arg and name in ADMONITIONS:
                # A body line directly under `.. tip:: text`, with no blank line,
                # continues the argument's paragraph; only a blank starts a new one.
                cont = k == 0 and bool(body) and bool(body[0].strip())
                rest = [arg] + ([""] if rest and not cont else []) + rest
                arg = ""
            inner, d = self.convert(rest)
            colons = ":" * max(3, d + 1)
            head = f"{colons}{{{name}}}" + (f" {self.inline(arg)}" if arg else "")
            return [head] + opts + inner + [colons], len(colons)
        what = "autodoc" if AUTODOC_RE.match(name) else "unknown directive"
        self.flag(f"{what} '{name}' wrapped in eval-rst")
        return (
            ["```{eval-rst}", f".. {name}:: {arg}".rstrip()]
            + ["   " + o for o in opts]
            + ([""] if rest else [])
            + self.fence_body(["   " + l if l else "" for l in rest])
            + ["```"],
            0,
        )

    def grid_table(self, block):
        """An RST grid table whose rows are each one `|` line between `+` borders,
        as a list-table. Returns None for any other shape (multi-line cells, spans,
        a missing header separator), which a human converts."""
        border = block[0]
        if not re.fullmatch(r"\+(-+\+)+", border):
            return None
        widths = [len(s) for s in border.strip("+").split("+")]
        cols = len(widths)
        rows, header = [], None
        for k, l in enumerate(block):
            if l.startswith("|"):
                if k + 1 >= len(block) or not block[k + 1].startswith("+"):
                    return None  # a cell spans several lines
                cells = [c.strip() for c in l.strip("|").split("|")]
                if len(cells) != cols:
                    return None  # a span, or a `|` inside a cell
                rows.append(cells)
            elif re.fullmatch(r"\+(=+\+)+", l):
                header = len(rows)
            elif l != border:
                return None
        if not rows:
            return None

        def esc(c):
            return re.sub(r"^(#{1,6})(\s|$)", r"\\\1\2", self.inline(c))

        # No :widths:. A grid table renders no <colgroup>, and :widths: adds one.
        out = ["```{list-table}"] + ([f":header-rows: {header}"] if header else [])
        out += [""]
        for r in rows:
            out.append("* - " + esc(r[0]))
            out += ["  - " + esc(c) for c in r[1:]]
        return out + ["```"]

    def cell_line_blocks(self, lines):
        """A list-table cell written as an RST line block (`- | a` then `  | b`)
        would render literal `|` text in MyST. Join its lines with hard breaks,
        like a top-level line block."""
        out, i = [], 0
        while i < len(lines):
            m = re.match(r"^(\s*(?:\* )?- )\| (.*)$", lines[i])
            if not m:
                # A cell is parsed as its own block, so a cell opening with `# `
                # (`# of trials`) becomes a heading. RST renders it as text.
                out.append(
                    re.sub(r"^(\s*(?:\* )?- )(#{1,6})(\s|$)", r"\1\\\2\3", lines[i])
                )
                i += 1
                continue
            col = " " * len(m.group(1))
            run = [m.group(1) + m.group(2)]
            i += 1
            while i < len(lines) and lines[i].startswith(col + "| "):
                run.append(col + lines[i][len(col) + 2 :])
                i += 1
            out += [l + "\\" for l in run[:-1]] + run[-1:]
            self.flag(
                "line block in list-table cell -> Markdown hard breaks: "
                "renders <p>+<br>, an explainable render diff"
            )
        return out

    def document(self, text):
        # docutils expands tabs to 8-column stops before parsing (tab_width).
        lines = [l.expandtabs(8) for l in text.splitlines()]
        kept = []
        for idx, l in enumerate(lines):
            if l.strip() == ":orphan:" and all(
                not x.strip() or x.startswith(".. ") or x.startswith("   ")
                for x in lines[:idx]
            ):
                self.orphan = True
                continue
            kept.append(l)
        self.targets = {
            m.group(1).strip("`").lower()
            for l in kept
            if (m := re.match(r"^\.\. _(`[^`]+`|[^:`]+): *\S+\s*$", l))
        }
        body = strip_blank_edges(self.convert(kept)[0])
        if self.prompt_fences:
            self.flag(
                f"{self.prompt_fences} fence(s) hold `>>>` prompts. Rewrite a "
                "real one as {testcode} plus {testoutput}, and an illustrative "
                "one as plain python and text blocks (Hard rule 4). Left as is, "
                "--doctest-glob='*.md' runs it, which nothing did for the .rst"
            )
        front = []
        if self.meta or self.orphan:
            front = ["---"]
            if self.orphan:
                front.append("orphan: true")
            if self.meta:
                front += ["myst:", "  html_meta:"]
                for key, val in self.meta.items():
                    val = val.replace("\\", "\\\\").replace('"', '\\"')
                    front.append(f'    {key}: "{val}"')
            front += ["---", ""]
        return "\n".join(front + body) + "\n"


# ------------------------------------------------------------------ helpers
def myst_option(opt):
    """MyST parses directive options with a YAML-style parser, and docutils
    takes the value verbatim. In myst-parser 5.1.0, a value that starts with
    `#` or holds ` #` loses the comment, a leading quote is stripped, and a
    leading `|` or `>` or an unterminated quote is a parse error. Double-quote
    exactly those values; every other value parses back unchanged."""
    m = OPTION_RE.match(opt)
    val = m.group(2) or ""
    if not (re.match(r"""[#"'|>]""", val) or re.search(r"\s#", val)):
        return opt
    return f':{m.group(1)}: "' + val.replace("\\", "\\\\").replace('"', '\\"') + '"'


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


def block_end(lines, j):
    while j < len(lines) and (not lines[j].strip() or indent_of(lines[j]) > 0):
        j += 1
    return j


def split_trailing_blanks(blk):
    blk = list(blk)
    trail = 0
    while blk and not blk[-1].strip():
        blk.pop()
        trail += 1
    return blk, trail


def dedent(lines):
    ind = min((indent_of(l) for l in lines if l.strip()), default=0)
    return [l[ind:].rstrip() if l.strip() else "" for l in lines]


def strip_blank_edges(lines):
    lines = list(lines)
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


# ---------------------------------------------------------------- selftest
CASES = [
    (
        "meta+orphan",
        ':orphan:\n\n.. meta::\n   :description: A "q" page.\n\nTop\n===\n',
        '---\norphan: true\nmyst:\n  html_meta:\n    description: "A \\"q\\" page."\n---\n\n# Top\n',
    ),
    ("heading order", "A\n---\n\nB\n===\n\nC\n---\n", "# A\n\n## B\n\n# C\n"),
    ("label", ".. _x-y:\n\nTop\n===\n", "(x-y)=\n\n# Top\n"),
    (
        "inline",
        "Use ``a`` and :ref:`t <l>` and `L <https://x.io>`__ at https://y.io.\n",
        "Use `a` and {ref}`t <l>` and [L](https://x.io) at <https://y.io>.\n",
    ),
    (
        "nested tabs",
        ".. tab-set::\n\n    .. tab-item:: Py\n\n        .. code-block:: python\n\n            x = 1\n",
        "::::{tab-set}\n:::{tab-item} Py\n```python\nx = 1\n```\n:::\n::::\n",
    ),
    ("one-line note", ".. note:: Hi.\n", ":::{note}\nHi.\n:::\n"),
    (
        "note arg continues",
        ".. tip:: One line\n    continues.\n",
        ":::{tip}\nOne line\ncontinues.\n:::\n",
    ),
    ("note arg then para", ".. note:: A.\n\n    B.\n", ":::{note}\nA.\n\nB.\n:::\n"),
    (
        "toctree",
        ".. toctree::\n    :maxdepth: 1\n\n    a/b.rst\n    c\n",
        "```{toctree}\n:maxdepth: 1\n\na/b\nc\n```\n",
    ),
    (
        "literal ::",
        "Run this::\n\n    ls -l\n\nDone.\n",
        "Run this:\n\n```\nls -l\n```\n\nDone.\n",
    ),
    ("spaced ::", "Run this. ::\n\n    ls\n", "Run this.\n\n```\nls\n```\n"),
    ("quote", "Intro:\n\n  - a\n  - b\n", "Intro:\n\n> - a\n> - b\n"),
    ("list continuation", "- item\n\n  more\n", "- item\n\n  more\n"),
    (
        "fence in list item",
        "1. Step:\n\n   .. code-block:: bash\n\n      ls\n",
        "1. Step:\n\n   ```bash\n   ls\n   ```\n",
    ),
    (
        "inline pseudo-list",
        "Two kinds:\n1. one\n2. two\n",
        "Two kinds:\n1\\. one\n2. two\n",
    ),
    ("inline dash", "See\n- this\n", "See\n\\- this\n"),
    ("real list", "Kinds:\n\n1. one\n2. two\n", "Kinds:\n\n1. one\n2. two\n"),
    ("auto-numbered list", "Kinds:\n\n#. one\n#. two\n", "Kinds:\n\n1. one\n1. two\n"),
    (
        "enumerator paragraph",
        "Intro.\n\n1. Do this\nand that.\n\n2. Then\nmore.\n",
        "Intro.\n\n1\\. Do this\nand that.\n\n2\\. Then\nmore.\n",
    ),
    (
        "enumerator with indented body",
        "1. Do this\n   and that.\n",
        "1. Do this\n   and that.\n",
    ),
    (
        "list-table cell line block",
        ".. list-table:: T\n   :header-rows: 1\n\n   * - A\n     - B\n   * - x\n     - | one\n       | two\n",
        "```{list-table} T\n:header-rows: 1\n\n* - A\n  - B\n* - x\n  - one\\\n    two\n```\n",
    ),
    (
        "prose hash",
        "# Not a heading.\n\nText\n# also text\n",
        "\\# Not a heading.\n\nText\n\\# also text\n",
    ),
    ("line block", "| a\n| b\n", "a\\\nb\n"),
    ("comment", ".. TODO: fix\n   later\n", "<!-- TODO: fix\n   later -->\n"),
    (
        "named target",
        "Text.\n\n.. _`Apache Arrow`: https://arrow.apache.org/\n",
        "Text.\n",
    ),
    (
        "wrapped role",
        "See :ref:`the\nguide <g>` now.\n",
        "See {ref}`the\nguide <g>` now.\n",
    ),
    ("wrapped literal", "Use ``a\nb`` here.\n", "Use `a\nb` here.\n"),
    (
        "link url on next line",
        "See the `Java client\n<https://x.io>`_. Then\nmore.\n",
        "See the [Java client](https://x.io). Then\nmore.\n",
    ),
    (
        "literalinclude",
        ".. literalinclude:: a.py\n    :language: python\n",
        "```{literalinclude} a.py\n:language: python\n```\n",
    ),
    (
        "doctest ends blank",
        ".. doctest::\n\n    >>> 1 + 1\n    2\n\nNext.\n",
        "```{doctest}\n>>> 1 + 1\n2\n\n```\n\nNext.\n",
    ),
    (
        "prompt in code-block",
        ".. code-block:: python\n\n    >>> x\n    1\n",
        "```python\n>>> x\n1\n\n```\n",
    ),
    (
        "prompt in literal",
        "Try::\n\n    >>> x\n    1\n",
        "Try:\n\n```\n>>> x\n1\n\n```\n",
    ),
    (
        "no prompt, no blank",
        ".. testcode::\n\n    print(1)\n",
        "```{testcode}\nprint(1)\n```\n",
    ),
    (
        "inline dash in list item",
        "- **Launch**:\n  - `a`: one.\n  - `b`: two.\n- Next\n",
        "- **Launch**:\n  \\- `a`: one.\n  \\- `b`: two.\n- Next\n",
    ),
    ("deflist under prose, no escape", "Term text\n  - one\n", "Term text\n: - one\n"),
    (
        "escape in default role",
        "Use `f -x=\\<id\\>` and ``X``\\s here.\n",
        "Use `f -x=<id>` and `X`s here.\n",
    ),
    ("backticks in bold", "- **`low`**: both.\n", "- **\\`low\\`**: both.\n"),
    (
        "deflist under prose",
        "**Term**\n  Definition.\n\n  - a\n\nAfter.\n",
        "**Term**\n: Definition.\n\n  - a\n\nAfter.\n",
    ),
    ("escaped space", "library (\\ ``x``\\ ) and\n", "library (`x`) and\n"),
    (
        "grid table",
        "+----+-----+\n| # a| b   |\n+====+=====+\n| 1  | `x` |\n+----+-----+\n",
        "```{list-table}\n:header-rows: 1\n\n* - \\# a\n  - b\n* - 1\n  - `x`\n```\n",
    ),
    (
        "grid table multi-line cell",
        "+---+---+\n| a | b |\n| c | d |\n+---+---+\n",
        "+---+---+\n| a | b |\n| c | d |\n+---+---+\n",
    ),
    (
        "tag-like text",
        "Replace <REGION> and <AWS_ID> in `<x>` at `a <https://x.io>`_.\n",
        "Replace \\<REGION> and <AWS_ID> in `<x>` at [a](https://x.io).\n",
    ),
    (
        "comment in list item",
        "- a\n\n  .. TODO: b\n\n- c\n",
        "- a\n\n  % TODO: b\n\n- c\n",
    ),
    (
        "list-table hash cell",
        ".. list-table::\n\n   * - # of trials\n     - # Nodes\n",
        "```{list-table}\n* - \\# of trials\n  - \\# Nodes\n```\n",
    ),
    (
        "list-table empty cell",
        ".. list-table::\n\n   * -\n     - A\n     - B\n",
        "```{list-table}\n* -\n  - A\n  - B\n```\n",
    ),
    ("comment with arrow", "..\n   A --> B\n\n   end\n", "%\n% A --> B\n%\n% end\n"),
    (
        "bare url in parentheses",
        "The killer (https://x.io/a.html) by type.\n",
        "The killer (<https://x.io/a.html>) by type.\n",
    ),
    (
        "bare url in bold stays text",
        "The default URL is **http://localhost:8265**.\n",
        "The default URL is **http://localhost:8265**.\n",
    ),
    (
        "option deeper than the body",
        ".. testoutput::\n    :options: +MOCK\n\n   127.0.0.1:8265\n",
        "```{testoutput}\n:options: +MOCK\n\n127.0.0.1:8265\n```\n",
    ),
    (
        "uri-only link",
        "See `<https://x.io/a#>`_ and `<https://y.io>`__.\n",
        "See <https://x.io/a#> and <https://y.io>.\n",
    ),
    (
        "replacements escaped",
        "(a) x, (c) y, Intel(R), C++-style, etc..\n",
        "(a) x, \\(c) y, Intel\\(R), C++\\-style, etc.\\.\n",
    ),
    (
        "replacements in a link label",
        "`Acme (tm) <https://x.io>`_\n",
        "[Acme \\(tm)](https://x.io)\n",
    ),
    # Negative: code, three-dot ellipses, dashes, and URLs keep their text.
    (
        "replacements left alone",
        "``(c) +- ..`` and `a..b` wait... a -- b <https://x.io/..>\n",
        "`(c) +- ..` and `a..b` wait... a -- b <https://x.io/..>\n",
    ),
    ("range dots escaped", "A percentage (0..100).\n", "A percentage (0.\\.100).\n"),
    ("grid table border kept", "Text.\n\n+---+---+\n", "Text.\n\n+---+---+\n"),
    (
        "leftover directive in a list item kept",
        "- .. image:: a.png\n",
        "- .. image:: a.png\n",
    ),
    (
        "tabs expand to 8 columns",
        ".. code-block:: shell\n\n\t$ a\n\t# \tb\n",
        "```shell\n$ a\n#       b\n```\n",
    ),
    (
        "prose gt escaped",
        "- p3 > p2, g4\n  > g3, and\n\n> top\n",
        "- p3 > p2, g4\n  \\> g3, and\n\n\\> top\n",
    ),
    (
        "option quoted for MyST",
        ".. literalinclude:: a.yaml\n   :start-after: # Head\n   :end-before: x # y\n",
        '```{literalinclude} a.yaml\n:start-after: "# Head"\n:end-before: "x # y"\n```\n',
    ),
    # Negative: values MyST parses back unchanged stay bare, and eval-rst keeps RST.
    (
        "option left bare",
        '.. literalinclude:: a.py\n   :start-after: if __name__ == "__main__"\n   :emphasize-lines: 1#2\n',
        '```{literalinclude} a.py\n:start-after: if __name__ == "__main__"\n:emphasize-lines: 1#2\n```\n',
    ),
    (
        "eval-rst option stays RST",
        ".. click:: m:f\n   :prog: # x\n",
        "```{eval-rst}\n.. click:: m:f\n   :prog: # x\n```\n",
    ),
    (
        "deeper continuation is a quote",
        "- Tip:\n\n    .. code-block:: yaml\n\n        a: 1\n\n    more\n",
        "- Tip:\n\n  > ```yaml\n  > a: 1\n  > ```\n  >\n  > more\n",
    ),
    (
        "deeper continuation under 1.",
        "1. Step:\n\n    text\n",
        "1. Step:\n\n   > text\n",
    ),
]


# (name, source, substring that must appear in some FLAG)
FLAG_CASES = [
    (
        "indented simple table",
        "Intro:\n\n  - Kind\n      ===  ===\n      A    B\n      ===  ===\n",
        "RST table",
    ),
    ("deflist in list item", "- Term\n    - def\n", "definition list inside the item"),
    ("prompt fence", ".. doctest::\n\n    >>> 1\n    1\n", "hold `>>>` prompts"),
    (
        "deflist after a list",
        "- a\n\n**Term**\n  Definition.\n",
        "definition list converted",
    ),
    (
        "shallow line ends list",
        "1. Step:\n\n   .. testcode::\n\n     x = 1\n\n  .. testoutput::\n\n    1\n\n2. Next\n",
        "ends the list in RST",
    ),
    ("uri-only relative", "See `<other.html>`_.\n", "relative link needs"),
    (
        "named reference shows the reference",
        "Long text before it all. See `Apache Arrow`_.\n",
        "`Apache Arrow`_",
    ),
    (
        "testcode body with no blank line",
        ".. testcode::\n    x = 1\n\n    y = 2\n",
        "no blank line before its body",
    ),
    (
        "bare-word named reference",
        "Modin_, previously.\n\n.. _Modin: https://x.io\n",
        "named hyperlink reference (inline the target): Modin_",
    ),
    ("unverified replacement", "Wait.... what\n", "replacements extension rewrites"),
    (
        "doctest in a deeper continuation",
        "1. Step:\n\n    .. testcode::\n\n        x = 1\n",
        "inside a > block quote",
    ),
]


# (name, source, substring that must NOT appear in any FLAG)
NOFLAG_CASES = [
    (
        "uri-only is not a named reference",
        "See `<https://x.io>`_ for details.\n",
        "named hyperlink reference",
    ),
    (
        "testcode with an option",
        ".. testcode::\n    :hide:\n\n    x = 1\n",
        "no blank line before its body",
    ),
    (
        "note body with no blank line",
        ".. note::\n    Text.\n",
        "no blank line before its body",
    ),
    (
        "option deeper than the body",
        ".. testoutput::\n    :options: +MOCK\n\n   1\n",
        "no blank line before its body",
    ),
    (
        "trailing underscore with no target",
        "Set foo_ and bar_baz_ here.\n\n.. _Modin: https://x.io\n",
        "named hyperlink reference",
    ),
    ("three dots", "Wait... what\n", "replacements extension rewrites"),
]


def selftest():
    bad = 0
    for name, src, want in FLAG_CASES:
        c = Converter()
        c.document(src)
        if not any(want in f for f in c.flags):
            bad += 1
            print(f"FAIL flag {name}: want {want!r} in {c.flags!r}")
    for name, src, unwanted in NOFLAG_CASES:
        c = Converter()
        c.document(src)
        if any(unwanted in f for f in c.flags):
            bad += 1
            print(f"FAIL noflag {name}: {unwanted!r} in {c.flags!r}")
    for name, src, want in CASES:
        got = Converter().document(src)
        if got != want:
            bad += 1
            print(f"FAIL {name}\n--- want\n{want!r}\n--- got\n{got!r}")
    total = len(CASES) + len(FLAG_CASES) + len(NOFLAG_CASES)
    print(f"{total - bad}/{total} cases pass")
    return bad


def main(argv):
    if argv[1:] == ["--selftest"]:
        sys.exit(1 if selftest() else 0)
    if len(argv) != 2:
        sys.exit(__doc__)
    c = Converter()
    sys.stdout.write(c.document(open(argv[1]).read()))
    for f in c.flags:
        print(f"FLAG {argv[1]}: {f}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
