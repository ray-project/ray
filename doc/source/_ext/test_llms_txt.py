"""Hermetic test for the in-repo ``llms_txt`` Sphinx extension.

Builds a tiny Sphinx project in a temp dir (in-process, no network, no Ray
import) that exercises:

* a toctree-structured index (``## Section`` per root toctree entry),
* all three description sources (MyST ``html_meta``, RST ``.. meta::``, and the
  first-paragraph fallback),
* ``llms_txt_exclude`` globs,
* the ``## Optional`` section, and
* the per-directory ``llms-full.txt`` shards + the root manifest.

Run directly (``python doc/source/_ext/test_llms_txt.py``) or under pytest. It
needs ``sphinx`` + ``myst-parser`` but not the full Ray docs toolchain.
"""

import io
import tempfile
from pathlib import Path

_EXT_DIR = str(Path(__file__).resolve().parent)

CONF = f"""\
import sys
sys.path.insert(0, {_EXT_DIR!r})
extensions = ["myst_parser", "llms_txt"]
project = "TestProj"
html_theme = "basic"
html_baseurl = "https://example.com/docs/"
llms_txt_title = "TestProj"
llms_txt_summary = "A test project."
llms_txt_exclude = ["secB/api/*"]
llms_txt_optional_sections = ["Reference"]
llms_txt_full_max_shard_tokens = 50
"""

FILES = {
    "conf.py": CONF,
    "index.rst": (
        "Test Root\n=========\n\n"
        ".. toctree::\n   :hidden:\n\n"
        "   Section A <secA/index>\n"
        "   Section B <secB/index>\n"
        "   Guides <guides/index>\n"
        "   Reference <secC/index>\n"
    ),
    # --- Section A: landing (.. meta::) + 3 children, one per description source
    "secA/index.rst": (
        ".. meta::\n   :description: Everything in section A.\n\n"
        "Section A Landing\n=================\n\n"
        ".. toctree::\n\n   page1\n   page2\n   page3\n"
    ),
    "secA/page1.md": (
        "---\nmyst:\n  html_meta:\n"
        '    description: "Page one via html_meta front-matter."\n---\n\n'
        "# Page One\n\nBody of page one.\n"
    ),
    "secA/page2.rst": (
        ".. meta::\n   :description: Page two via rst meta directive.\n\n"
        "Page Two\n========\n\nBody of page two.\n"
    ),
    "secA/page3.rst": (
        "Page Three\n==========\n\n"
        "This is the first real paragraph of page three and becomes its "
        "description via the fallback.\n\n"
        ".. toctree::\n\n   page3-sub\n"
    ),
    # A grandchild (two levels under the Section A landing) — must appear in a
    # complete index, which lists the full subtree, not just direct children.
    "secA/page3-sub.rst": (
        "Page Three Sub\n==============\n\n"
        "A grandchild page reached two levels under Section A.\n"
    ),
    # --- Section B: landing (html_meta) + a plain child + an EXCLUDED child
    "secB/index.md": (
        "---\nmyst:\n  html_meta:\n"
        '    description: "Section B landing description."\n---\n\n'
        "# Section B Landing\n\n"
        "```{toctree}\nplain\napi/excluded\n```\n"
    ),
    "secB/plain.rst": (
        "Plain Page\n==========\n\n"
        "A plain page with only a paragraph and no description metadata.\n"
    ),
    "secB/api/excluded.rst": (
        "Excluded API Page\n=================\n\n"
        "This page is excluded and must not appear anywhere.\n"
    ),
    # --- Section C: marked Optional in conf
    "secC/index.rst": (
        ".. meta::\n   :description: Reference material.\n\n"
        "Reference Landing\n=================\n\n"
        ".. toctree::\n\n   refpage\n"
    ),
    "secC/refpage.rst": (
        "Ref Page\n========\n\nA reference page paragraph for the corpus.\n"
    ),
    # --- Guides: a section with subdirectories, large enough (vs the tiny
    #     llms_txt_full_max_shard_tokens budget) to trigger sub-sharding.
    "guides/index.rst": (
        ".. meta::\n   :description: Guides section overview.\n\n"
        "Guides Landing\n==============\n\n"
        ".. toctree::\n\n   topic-a/intro\n   topic-a/deep\n   topic-b/intro\n"
    ),
    "guides/topic-a/intro.rst": (
        "Topic A Intro\n=============\n\n"
        "Body text for the topic A intro page, long enough to add real bytes.\n"
    ),
    "guides/topic-a/deep.rst": (
        "Topic A Deep Dive\n=================\n\n"
        "Body text for the topic A deep dive page with additional length here.\n"
    ),
    "guides/topic-b/intro.rst": (
        "Topic B Intro\n=============\n\n"
        "Body text for the topic B intro page within the guides section here.\n"
    ),
    # In no toctree but under Section A's directory (mimics a gallery page Ray
    # links from a grid card, not a toctree). Must fold into Section A, not the
    # catch-all.
    "secA/gallery-orphan.rst": (
        ":orphan:\n\n"
        "Section A Gallery Orphan\n========================\n\n"
        "A gallery-linked page under secA that no toctree reaches.\n"
    ),
    # Build-fetched into _collections/<lib>/... — keyed on <lib> (secB) so it
    # folds into Section B rather than a synthetic collections bucket.
    "_collections/secB/fetched-example.rst": (
        ":orphan:\n\n"
        "Fetched Collection Example\n==========================\n\n"
        "A build-fetched example page keyed to section B.\n"
    ),
    # In-scope, in no toctree, AND in a directory that maps to no section — the
    # only kind of page left in the trailing "## Other pages" catch-all.
    "orphan.rst": (
        ":orphan:\n\n"
        "Orphan Page\n===========\n\n"
        "An in-scope page not referenced by any toctree.\n"
    ),
}


def _build(tmp: Path, extra_conf: str = "") -> Path:
    from sphinx.application import Sphinx

    srcdir = tmp / "src"
    outdir = tmp / "out"
    doctreedir = tmp / "doctrees"
    for relpath, content in FILES.items():
        if relpath == "conf.py":
            content = content + extra_conf
        path = srcdir / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    status, warning = io.StringIO(), io.StringIO()
    app = Sphinx(
        str(srcdir),
        str(srcdir),
        str(outdir),
        str(doctreedir),
        "html",
        status=status,
        warning=warning,
    )
    app.build()
    return outdir


def _check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def test_llms_txt():
    with tempfile.TemporaryDirectory() as tmp:
        out = _build(Path(tmp))

        index = (out / "llms.txt").read_text(encoding="utf-8")

        # Header + summary
        _check(index.startswith("# TestProj"), "missing H1 title")
        _check("> A test project." in index, "missing summary blockquote")

        # Section headers, from the root toctree labels
        _check("## Section A" in index, "missing Section A heading")
        _check("## Section B" in index, "missing Section B heading")
        # Reference is optional -> under '## Optional', not its own heading
        _check("## Optional" in index, "missing Optional heading")
        _check("## Reference" not in index, "Reference should be under Optional")

        # Section ordering: A, then B, then Optional (Reference) last
        _check(
            index.index("## Section A")
            < index.index("## Section B")
            < index.index("## Optional"),
            "sections out of order",
        )

        # All three description sources resolved correctly
        _check(
            "Page one via html_meta front-matter." in index,
            "html_meta description not resolved",
        )
        _check(
            "Page two via rst meta directive." in index,
            ".. meta:: description not resolved",
        )
        _check(
            "first real paragraph of page three" in index,
            "first-paragraph fallback not resolved",
        )
        _check("Everything in section A." in index, "landing description missing")

        # Exclusion
        _check("Excluded API Page" not in index, "excluded page leaked into index")
        _check("api/excluded" not in index, "excluded docname leaked into index")

        # Absolute URLs via html_baseurl
        _check(
            "https://example.com/docs/secA/page1.html" in index,
            "page URL not absolute / wrong",
        )

        # Complete index: a grandchild (two levels under the section landing) is
        # listed, not just direct children.
        _check(
            "Page Three Sub" in index,
            "grandchild page missing — index isn't listing the full subtree",
        )
        # Pointer to the full-text corpus.
        _check(
            "[llms-full.txt](https://example.com/docs/llms-full.txt)" in index,
            "index missing pointer to llms-full.txt",
        )
        # Directory folding: a page in no toctree but under a section's
        # directory lists under that section, not the catch-all.
        _check(
            index.index("## Section A")
            < index.index("Section A Gallery Orphan")
            < index.index("## Section B"),
            "gallery orphan not folded into Section A",
        )
        # _collections/<lib> pages fold on <lib>, into that library's section.
        _check(
            index.index("## Section B")
            < index.index("Fetched Collection Example")
            < index.index("## Guides"),
            "_collections page not folded into Section B",
        )
        # Catch-all: only a page whose directory maps to no section lands here.
        _check("## Other pages" in index, "missing '## Other pages' catch-all")
        _check("Orphan Page" in index, "orphan page missing from catch-all")
        _check(
            index.index("Orphan Page") > index.index("## Other pages"),
            "true orphan should be under the catch-all",
        )
        _check(
            max(
                index.index("Section A Gallery Orphan"),
                index.index("Fetched Collection Example"),
            )
            < index.index("## Other pages"),
            "a folded page leaked into the catch-all",
        )

        # --- llms-full root manifest + per-dir shards ---
        manifest = (out / "llms-full.txt").read_text(encoding="utf-8")
        _check(manifest.startswith("# TestProj: full documentation"), "bad manifest H1")
        for rel in ("secA/llms-full.txt", "secB/llms-full.txt", "secC/llms-full.txt"):
            _check(rel in manifest, f"manifest missing link to {rel}")
            _check((out / rel).exists(), f"missing shard {rel}")
        # Manifest entries carry page count + the section description.
        _check(
            "pages): Everything in section A." in manifest,
            "manifest entry missing page count + description",
        )

        secA_full = (out / "secA" / "llms-full.txt").read_text(encoding="utf-8")
        _check("Body of page one." in secA_full, "shard missing verbatim source")
        _check(
            "Source: https://example.com/docs/secA/page1.html" in secA_full,
            "shard missing Source header",
        )
        # Shard opens with a description + Contents TOC listing its pages.
        _check("> Everything in section A." in secA_full, "shard missing description")
        _check("## Contents" in secA_full, "shard missing Contents TOC")
        _check(
            "- [Page One](https://example.com/docs/secA/page1.html)" in secA_full,
            "shard TOC missing page link",
        )
        # Landing page leads both the TOC and the body.
        _check(
            secA_full.index("Section A Landing") < secA_full.index("Page One"),
            "shard not led by landing page",
        )

        secB_full = (out / "secB" / "llms-full.txt").read_text(encoding="utf-8")
        _check(
            "excluded and must not appear" not in secB_full,
            "excluded page leaked into shard",
        )
        # The excluded page's title would only appear via its content or a TOC
        # entry; the bare docname `api/excluded` legitimately occurs in the
        # landing page's verbatim toctree source, so don't assert on that.
        _check(
            "Excluded API Page" not in secB_full,
            "excluded page leaked into shard TOC",
        )
        # _collections/<lib> pages group into <lib>'s full-text shard too — same
        # section membership as the index — not a separate _collections shard.
        _check(
            "A build-fetched example page keyed to section B." in secB_full,
            "_collections page not folded into the secB full-text shard",
        )
        _check(
            not (out / "_collections" / "llms-full.txt").exists()
            and not (out / "_collections" / "secB" / "llms-full.txt").exists(),
            "a stray _collections shard was written",
        )
        _check(
            "_collections/" not in manifest,
            "manifest still references a _collections shard",
        )

        # --- sub-sharding: an oversize section splits into per-subdir shards ---
        # "Guides" exceeds the tiny llms_txt_full_max_shard_tokens budget and has
        # subdirectories, so it splits; "Section A" is over budget too but has no
        # subdirectories, so it stays a single shard (asserted above).
        _check(
            (out / "guides" / "topic-a" / "llms-full.txt").exists(),
            "sub-shard guides/topic-a/llms-full.txt not written",
        )
        _check(
            (out / "guides" / "topic-b" / "llms-full.txt").exists(),
            "sub-shard guides/topic-b/llms-full.txt not written",
        )
        _check(
            (out / "guides" / "llms-full.txt").exists(),
            "parent remainder shard guides/llms-full.txt not written",
        )
        # Parent keeps only its directly-held page, not the subdir pages.
        guides_parent = (out / "guides" / "llms-full.txt").read_text(encoding="utf-8")
        _check("Guides Landing" in guides_parent, "parent shard missing landing page")
        _check(
            "Topic A Deep Dive" not in guides_parent,
            "subdir page leaked into parent remainder shard",
        )
        # The topic-a sub-shard carries both of its pages.
        topic_a = (out / "guides" / "topic-a" / "llms-full.txt").read_text(
            encoding="utf-8"
        )
        _check(
            "Topic A Intro" in topic_a and "Topic A Deep Dive" in topic_a,
            "topic-a sub-shard missing its pages",
        )
        # Manifest nests the sub-shards under their parent (indented link).
        _check(
            "guides/topic-a/llms-full.txt" in manifest and "  - [" in manifest,
            "manifest missing nested sub-shard entry",
        )

    return True


def test_notebook_exclusion():
    """Notebooks are dropped by source suffix (so build-time-fetched notebooks a
    conf-load scan would miss are also caught), independent of llms_txt_exclude.

    Unit-level so the test needs no myst-nb / notebook execution: the extension
    decides via ``env.doc2path(docname)``, which we stub.
    """
    import sys

    sys.path.insert(0, _EXT_DIR)
    import llms_txt

    class FakeEnv:
        def doc2path(self, docname):
            suffix = ".ipynb" if docname.endswith("-nb") else ".rst"
            return f"/src/{docname}{suffix}"

    env = FakeEnv()
    _check(llms_txt._is_notebook(env, "guide-nb"), "notebook not detected by suffix")
    _check(not llms_txt._is_notebook(env, "guide"), "non-notebook misdetected")
    # _excluded drops notebooks even when no glob matches them...
    _check(llms_txt._excluded(env, "guide-nb", []), "notebook not excluded")
    # ...and still honors glob excludes for non-notebook pages.
    _check(llms_txt._excluded(env, "api/foo", ["api/*"]), "glob exclude regressed")
    _check(not llms_txt._excluded(env, "guide", ["api/*"]), "false exclusion")
    return True


def test_build_gate():
    """No output is generated when `llms_txt_build` is False — the switch RtD PR
    previews use to skip the agent corpus (DOC-1048)."""
    with tempfile.TemporaryDirectory() as tmp:
        out = _build(Path(tmp), extra_conf="\nllms_txt_build = False\n")
        _check(not (out / "llms.txt").exists(), "llms.txt written despite gate off")
        _check(
            not (out / "llms-full.txt").exists(),
            "manifest written despite gate off",
        )
        _check(
            not (out / "secA" / "llms-full.txt").exists(),
            "shard written despite gate off",
        )
    return True


def test_clean_coercion():
    """`_clean` coerces non-string front-matter values instead of crashing the
    build (a YAML `description:` can parse to a number, bool, or list)."""
    import sys

    sys.path.insert(0, _EXT_DIR)
    import llms_txt

    _check(llms_txt._clean("  a   b ") == "a b", "string not collapsed")
    _check(llms_txt._clean(123) == "123", "int not coerced")
    _check(llms_txt._clean(["a", "b"]) == "a b", "list not joined")
    _check(llms_txt._clean(None) == "", "None not handled")
    return True


def test_base_url_override():
    """llms_txt_base_url overrides html_baseurl for all generated links, so links
    can track the version being built (e.g. RtD's READTHEDOCS_CANONICAL_URL)."""
    with tempfile.TemporaryDirectory() as tmp:
        out = _build(
            Path(tmp),
            extra_conf='\nllms_txt_base_url = "https://example.com/en/v2/"\n',
        )
        index = (out / "llms.txt").read_text(encoding="utf-8")
        _check(
            "https://example.com/en/v2/secA/page1.html" in index,
            "llms_txt_base_url not used for page links",
        )
        _check(
            "https://example.com/docs/" not in index,
            "html_baseurl leaked despite llms_txt_base_url override",
        )
        manifest = (out / "llms-full.txt").read_text(encoding="utf-8")
        _check(
            "https://example.com/en/v2/secA/llms-full.txt" in manifest,
            "llms_txt_base_url not used for shard links",
        )
    return True


if __name__ == "__main__":
    test_llms_txt()
    test_notebook_exclusion()
    test_build_gate()
    test_clean_coercion()
    test_base_url_override()
    print(
        "PASS: llms_txt extension produced a correct index, Optional section, "
        "llms-full shards, excludes notebooks by source type, and honors the "
        "llms_txt_build gate."
    )
