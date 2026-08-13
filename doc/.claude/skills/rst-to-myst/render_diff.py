"""Diff a Read the Docs PR preview against /en/master, page by page.

An RST-to-MyST conversion is meant to be format-only, so the rendered HTML
should be equivalent. A green build proves every reference resolved; it does not
prove the page still renders the same thing. Several ways to change the render
leave the build green and emit no warning at all:

  - `.. title::` does not set the document title from inside an {eval-rst}
    block, so a page whose title came from that directive silently loses it.
  - An RST `.. image::` with no `:alt:` takes its alt text from the URI and
    emits a bare <img>; Markdown `![]()` emits alt="" wrapped in a <p>.
  - A directive that builds RST into a ViewList and hands it to `nested_parse`
    degrades to literal visible text under MyST, losing every domain object,
    anchor, and cross-reference it would have created.

This catches all of them, because it compares output rather than source.

Usage:
    python3 render_diff.py <preview_base_url> <page.html> [<page.html> ...]

    python3 render_diff.py https://anyscale-ray--12345.com.readthedocs.build/en/12345/ \\
        index.html ray-core/key-concepts.html

Exit status is the number of pages with an unexplained diff. Read the diffs
rather than trusting the count: byte-identical is not always the right bar, and
a legitimate markup change (a caption-less {figure}, say) shows up here too.

Requires beautifulsoup4, which the docs toolchain already installs.
"""

import argparse
import difflib
import re
import sys
import urllib.request

from bs4 import BeautifulSoup

MASTER = "https://docs.ray.io/en/master/"

# Differences every MyST page shows against an RST page, carrying no rendered
# consequence. Filtered out so a real regression is not buried in nine copies of
# the same benign line.
#
#   tex2jax_ignore / mathjax_ignore: myst-parser stamps these on the root
#     <section> of every MyST document. Present on every already-Markdown page.
#   class="code": `default_role = "code"` makes a single-backtick RST literal
#     render as <code class="code docutils literal notranslate">; a Markdown
#     single-backtick drops the `code` class. That class carries no styling in
#     either Ray's CSS or pydata-sphinx-theme, so plain backticks are correct
#     and this diff is expected.
BENIGN = [
    (re.compile(r'\s*class="tex2jax_ignore mathjax_ignore"'), ""),
    (re.compile(r'class="code (docutils literal notranslate)"'), r'class="\1"'),
]

# Pages whose body is randomized at build time and so differ between any two
# builds of the same commit. `custom_directives.py` picks the example-gallery
# icons with random.randint / random.choice.
NONDETERMINISTIC = {"ray-overview/examples.html"}


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "render-diff"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read().decode("utf-8", "replace")


def normalize(html: str, base: str, keep_benign: bool):
    """Reduce a page to its comparable body plus its title.

    Strips what legitimately differs between two builds: the host in absolute
    links, the release string in version-pinned URLs, and the search-highlight
    query parameters Sphinx appends. Collapses whitespace so a re-indented
    directive body does not read as a content change, then puts one tag per
    line so difflib reports a tight hunk.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else "(no <title>)"

    body = soup.find("article") or soup.find("body")
    if body is None:
        return title, []

    text = str(body).replace(base, "/").replace(MASTER, "/")
    text = re.sub(r"/en/(master|latest|[\w.-]+)/", "/en/VERSION/", text)
    text = re.sub(r"\bRay \d+\.\d+\.\d+[\w.]*", "Ray VERSION", text)
    text = re.sub(r"\?highlight=[^\"'&]*", "", text)
    if not keep_benign:
        for pattern, replacement in BENIGN:
            text = pattern.sub(replacement, text)
    text = re.sub(r"\s+", " ", text)
    return title, re.sub(r">\s*<", ">\n<", text).splitlines()


def compare(page: str, preview_base: str, keep_benign: bool, context: int) -> bool:
    """Print one page's diff. Returns True when the page differs."""
    master_title, master_lines = normalize(fetch(MASTER + page), MASTER, keep_benign)
    preview_title, preview_lines = normalize(
        fetch(preview_base + page), preview_base, keep_benign
    )

    title_note = ""
    if master_title != preview_title:
        title_note = f"  TITLE master={master_title!r} preview={preview_title!r}"

    diff = list(
        difflib.unified_diff(
            master_lines, preview_lines, "master", "preview", n=context, lineterm=""
        )
    )
    if not diff and not title_note:
        print(f"IDENTICAL  {page}  (title: {master_title!r})")
        return False

    note = "  [known nondeterministic]" if page in NONDETERMINISTIC else ""
    added = sum(1 for d in diff if d.startswith("+") and not d.startswith("+++"))
    removed = sum(1 for d in diff if d.startswith("-") and not d.startswith("---"))
    print(f"\nDIFFERS    {page}  (+{added} -{removed}){title_note}{note}")
    for line in diff:
        print("   ", line)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("preview_base", help="RtD PR preview base URL")
    parser.add_argument("pages", nargs="+", help="page paths, e.g. ray-core/index.html")
    parser.add_argument(
        "--keep-benign",
        action="store_true",
        help="do not filter the known-benign MyST markup differences",
    )
    parser.add_argument("-n", "--context", type=int, default=1)
    args = parser.parse_args()

    preview_base = args.preview_base.rstrip("/") + "/"
    differing = sum(
        compare(page, preview_base, args.keep_benign, args.context)
        for page in args.pages
    )
    print(f"\n{len(args.pages) - differing}/{len(args.pages)} pages render identically")
    return differing


if __name__ == "__main__":
    sys.exit(main())
