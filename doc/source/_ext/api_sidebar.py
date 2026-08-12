"""Section-scoped sidebars (Pattern B): one shared nav per section, loaded client-side.

Ray's global sidebar server-renders the whole toctree into *every* page. That is both
heavy and undifferentiated: a reader deep in the API reference or in the KubeRay guides
gets the entire site's navigation on every page. This extension gives a *section* its
own sidebar, rendered once per build and hydrated client-side:

  1. Capture the section's toctree once at ``env-updated``, *before*
     ``sphinx_remove_toctrees`` (priority 500) prunes pages out of it, and render it to
     a single fragment written to ``_static/<fragment>`` at ``build-finished`` (the HTML
     writer isn't ready until then).
  2. On the section's pages, swap in a small container that loads that fragment via
     ``_static/section-nav-loader.js`` and highlights the current page client-side.
  3. Server-render the section's *top-level* pages into that container as a fallback,
     so the nav works without JavaScript and is present at parse time for assistive
     tech and crawlers. The loader upgrades it to the full tree, and leaves it alone
     if the fetch fails.

So each page in a section embeds only a handful of nav links; the full tree is one
browser-cached file. Pages in a section also keep their navbar tab highlighted, via
``navbar_active_file`` (see ``render_header_nodes`` in ``custom_directives.py``).

Pages keep their *original* locations. A section is defined by a root docname (whose
toctree supplies the nav) plus the source-directory prefixes whose pages belong to it,
so a section can aggregate content from anywhere without a URL change.

"Is this page in section X?" is answered by membership in ``prefixes`` -- a stateless
check, so it works under parallel writing (no reliance on cross-process state).

Two sections are configured today; see ``SECTIONS``:

* **APIs** (``apis/index``). The symbol-level API nav has ~3k stub pages; rendering
  those into every page would bloat each one (~250 KB of sidebar) and OOM the build.
  The stubs are kept OUT of the global toctree by ``remove_from_toctrees`` in conf.py,
  so the global ``main-sidebar`` stays small, and the APIs tab is the only place they
  are navigable. The reference pages keep their original locations (``data/api/``,
  ``train/api/``, ``rllib/package_ref/``, ...); they are pulled into the tab purely by
  ``apis/index``'s toctree.
* **KubeRay** (``cluster/kubernetes/index``). Roughly 80 pages that also remain in the
  global sidebar under Ray Clusters. Here the win is scope, not size: KubeRay readers
  get a sidebar containing only KubeRay.

To add a section, append an entry to ``SECTIONS`` and add a matching ``navbar.yml``
tab pointing at its ``root``. No template or loader change is needed.

Note: ``api_sidebar`` is a historical module name from when the APIs tab was the only
section. The mechanism is general. Renaming it means touching ``.buildkite`` tag rules,
``doc/BUILD.bazel``, and ``ci/pipeline/test_doc_api_rules_sync.py``, so it is left alone
here to keep this change off the shared CI-rules surface.
"""
import os
import posixpath
import re

import bs4
from sphinx.environment.adapters.toctree import global_toctree_for_doc
from sphinx.util import logging as sphinx_logging

# Reuse the in-repo copy rather than importing from ``pydata_sphinx_theme.toctree``:
# that symbol isn't part of the theme's public API, so importing it directly makes
# the docs build fragile across theme upgrades. See the docstring on the vendored copy.
from custom_directives import add_collapse_checkboxes

logger = sphinx_logging.getLogger(__name__)

SIDEBAR_TEMPLATE = "section-sidebar.html"

# Source directories whose pages make up the APIs tab. These are aggregated by the
# toctree in apis/index.md; pages under them get the shared API sidebar. Kept in sync
# with that toctree, and with the ``doc_api`` rule in .buildkite/test.rules.txt --
# ci/pipeline/test_doc_api_rules_sync.py parses this assignment out of the AST, so it
# must stay a module-level literal tuple.
API_PATH_PREFIXES = (
    "apis/",  # the APIs landing page (apis/index)
    "data/api/",
    "train/api/",
    "tune/api/",
    "serve/api/",
    "ray-core/api/",
    "rllib/package_ref/",
)

# The KubeRay guides are a single contiguous subtree, so one prefix covers the tab.
KUBERAY_PATH_PREFIXES = ("cluster/kubernetes/",)

# name:     internal id; also the _state key.
# root:     docname whose toctree supplies the section nav.
# fragment: filename written under _static/.
# prefixes: docname prefixes whose pages get this sidebar.
# label:    human-readable section name, used in the sidebar's aria-label.
# maxdepth: toctree depth, counted from the *site* root. A section nested N levels
#           deep needs N extra levels to expose the same depth below its own root.
SECTIONS = (
    {
        "name": "apis",
        "root": "apis/index",
        "fragment": "api-nav.html",
        "prefixes": API_PATH_PREFIXES,
        "label": "APIs",
        "maxdepth": 6,
    },
    {
        "name": "kuberay",
        "root": "cluster/kubernetes/index",
        "fragment": "kuberay-nav.html",
        "prefixes": KUBERAY_PATH_PREFIXES,
        "label": "KubeRay",
        # cluster/kubernetes/index sits two levels down (root -> Ray Clusters ->
        # Kubernetes), so 8 leaves the same six usable levels the APIs tab gets.
        "maxdepth": 8,
    },
)

# Captured in the main process at env-updated and consumed in the main process at
# build-finished (same process), so it is safe under parallel read/write.
_state = {}

# Per-process memo for the server-rendered fallback nav: root docname -> tuple of
# (docname, title) for the section's immediate toctree children. Derived from
# ``app.env``, which every parallel-write worker has, rather than from ``_state``,
# which only the main process fills.
_children_cache = {}


def _section_children(env, root_docname):
    """The section root's immediate toctree children, as (docname, title) pairs.

    Backs the no-JS fallback nav in ``section-sidebar.html``. Titles come from
    ``env.titles`` (each target page's own title), not from any custom label the
    section root's toctree gives the entry, so a relabelled entry reads slightly
    differently here than in the hydrated tree. That only shows without JavaScript
    and before hydration, and the link targets are identical either way."""
    if root_docname in _children_cache:
        return _children_cache[root_docname]
    items = []
    for child in getattr(env, "toctree_includes", {}).get(root_docname, []):
        title = env.titles.get(child)
        items.append((child, title.astext() if title is not None else child))
    if not items and root_docname in getattr(env, "all_docs", {}):
        # Only a problem when the section root is actually part of this build. Under a
        # scoped build (DOC_LIB / build_one_lib) an out-of-scope section is absent by
        # design, and warning there would fail the build under `fail_on_warning`.
        logger.warning(
            "[api_sidebar] no toctree children found for %s; the no-JS fallback nav "
            "for this section will be empty",
            root_docname,
        )
    _children_cache[root_docname] = tuple(items)
    return _children_cache[root_docname]


def _capture_section_toctrees(app, env):
    """env-updated @ priority < 500: resolve each section's toctree while pruned pages
    are still present (sphinx_remove_toctrees prunes them at priority 500)."""
    for section in SECTIONS:
        root = section["root"]
        try:
            node = global_toctree_for_doc(
                env,
                root,
                app.builder,
                collapse=False,
                maxdepth=section["maxdepth"],
                includehidden=True,
                titles_only=True,
            )
        except Exception as exc:
            logger.warning(
                "[api_sidebar] could not capture the %s toctree: %s",
                section["name"],
                exc,
            )
            continue
        if node is None:
            logger.warning(
                "[api_sidebar] %s toctree resolved empty (is %s present?)",
                section["name"],
                root,
            )
            continue
        _state[section["name"]] = node


def _root_relative(html, root_docname):
    """Rewrite the fragment's hrefs to be relative to the doc root.

    global_toctree_for_doc resolves links relative to ``root_docname``, which lives in
    ``dirname(root_docname)``. Rebasing onto that directory makes each href
    root-relative; the loader then resolves them against each page's URL root."""
    base = posixpath.dirname(root_docname)

    def repl(m):
        href = m.group(1)
        if re.match(r"^(https?:|/|#|mailto:)", href):
            return m.group(0)
        return 'href="%s"' % posixpath.normpath(posixpath.join(base, href))

    return re.sub(r'href="([^"]*)"', repl, html)


def _isolate_section(soup):
    """Keep only the section's own subtree from the whole-site nav.

    global_toctree_for_doc returns the entire site nav (all top-level sections). Because
    the toc was resolved *for the section root*, that root and each of its ancestors are
    marked ``current``; the deepest such list item is the section root itself. Its child
    list is the section nav. Leaving the section is the top nav's job, so nothing above
    it belongs in this sidebar."""
    current = [li for li in soup.find_all("li") if "current" in (li.get("class") or [])]
    if not current:
        return None
    deepest = max(current, key=lambda li: len(li.find_parents("li")))
    return deepest.find("ul", recursive=False)


def _render_and_write(app, exc):
    """build-finished (main process, writer ready): render each captured toc, keep only
    the section's own subtree, and write it as that section's shared fragment."""
    if exc is not None:
        return
    for section in SECTIONS:
        node = _state.get(section["name"])
        if node is None:
            continue
        try:
            html = app.builder.render_partial(node)["fragment"]
        except Exception as e:
            logger.warning(
                "[api_sidebar] could not render the %s nav fragment: %s",
                section["name"],
                e,
            )
            continue
        soup = bs4.BeautifulSoup(html, "html.parser")
        subtree = _isolate_section(soup)
        if subtree is None:
            logger.warning(
                "[api_sidebar] could not isolate the %s section in the toc; "
                "fragment not written",
                section["name"],
            )
            continue
        subtree["class"] = "nav bd-sidenav"
        # render_partial emits plain nested <ul>; reuse pydata's helper to add the
        # collapsible <details>/<summary> structure (closed by default) so the loader
        # can expand the current path and the theme's CSS styles the chevrons natively.
        frag_soup = bs4.BeautifulSoup(str(subtree), "html.parser")
        add_collapse_checkboxes(frag_soup)
        html = _root_relative(str(frag_soup), section["root"])
        static_dir = os.path.join(app.outdir, "_static")
        os.makedirs(static_dir, exist_ok=True)
        with open(
            os.path.join(static_dir, section["fragment"]), "w", encoding="utf-8"
        ) as fh:
            fh.write(html)
        logger.info(
            "[api_sidebar] wrote shared %s nav fragment (%d KB, section subtree only) "
            "to _static/%s",
            section["name"],
            len(html) // 1024,
            section["fragment"],
        )


def _on_html_page_context(app, pagename, templatename, context, doctree):
    for section in SECTIONS:
        if pagename.startswith(section["prefixes"]):
            context["sidebars"] = [SIDEBAR_TEMPLATE]
            context["section_nav_fragment"] = section["fragment"]
            context["section_nav_label"] = section["label"]
            context["section_nav_fallback"] = _section_children(
                app.env, section["root"]
            )
            # Keeps this section's navbar tab highlighted on every page in it; read
            # by render_header_nodes in custom_directives.py.
            context["navbar_active_file"] = section["root"]
            return


def setup(app):
    # priority < 500 so we capture the toctrees BEFORE sphinx_remove_toctrees prunes.
    app.connect("env-updated", _capture_section_toctrees, priority=1)
    app.connect("build-finished", _render_and_write)
    app.connect("html-page-context", _on_html_page_context, priority=900)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}
