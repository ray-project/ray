"""APIs-tab sidebar (Pattern B): a single shared API navigation, loaded client-side.

Ray's global sidebar server-renders the whole toctree into *every* page. The symbol-
level API nav has ~3k stub pages; rendering those into every page would bloat each one
(~250 KB of sidebar) and OOM the build. Instead:

  1. Capture the full ``apis/`` toctree once at ``env-updated``, *before*
     ``sphinx_remove_toctrees`` (priority 500) prunes the stub pages, and render it to
     a single fragment written to ``_static/api-nav.html`` at ``build-finished`` (the
     HTML writer isn't ready until then).
  2. The per-symbol stubs are kept OUT of the global toctree by ``remove_from_toctrees``
     in conf.py (Ray already does this), so the global ``main-sidebar`` stays small.
  3. On API pages, swap in a tiny container that loads the shared fragment via
     ``_static/api-nav-loader.js`` and highlights the current page client-side.

The API reference pages keep their *original* locations (``data/api/``, ``train/api/``,
``rllib/package_ref/``, ...); they are pulled into the APIs tab purely by
``apis/index``'s toctree, with no URL change. "Is this an API page?" is therefore
answered by membership in those known source directories (``API_PATH_PREFIXES``) -- a
stateless check, so it works under parallel writing (no reliance on cross-process
state).

So each API page embeds ~no navigation HTML; the nav is one browser-cached file.
"""
import os
import re

import bs4
from pydata_sphinx_theme.toctree import add_collapse_checkboxes
from sphinx.environment.adapters.toctree import global_toctree_for_doc
from sphinx.util import logging as sphinx_logging

logger = sphinx_logging.getLogger(__name__)

APIS_PREFIX = "apis"
NAV_DOCNAME = APIS_PREFIX + "/index"
NAV_FILENAME = "api-nav.html"

# Source directories whose pages make up the APIs tab. These are aggregated by the
# toctree in apis/index.md; pages under them get the shared API sidebar. Kept in sync
# with that toctree. A page's docname starting with one of these (or being the APIs
# landing itself) marks it as API content. Stateless on purpose -- html-page-context
# can fire in worker processes under `-j`, where main-process state isn't shared.
API_PATH_PREFIXES = (
    "apis/",  # the APIs landing page (apis/index)
    "data/api/",
    "train/api/",
    "tune/api/",
    "serve/api/",
    "ray-core/api/",
    "rllib/package_ref/",
)

# Captured in the main process at env-updated and consumed in the main process at
# build-finished (same process), so it is safe under parallel read/write.
_state = {}


def _capture_apis_toctree(app, env):
    """env-updated @ priority < 500: resolve the full apis/ toctree while the stub
    pages are still present (sphinx_remove_toctrees prunes them at priority 500)."""
    try:
        node = global_toctree_for_doc(
            env, NAV_DOCNAME, app.builder,
            collapse=False, maxdepth=6, includehidden=True, titles_only=True,
        )
    except Exception as exc:
        logger.warning("[api_sidebar] could not capture apis toctree: %s", exc)
        return
    if node is None:
        logger.warning("[api_sidebar] apis toctree resolved empty (is %s present?)", NAV_DOCNAME)
        return
    _state["node"] = node


def _root_relative(html):
    """global_toctree_for_doc resolves links relative to apis/index, which lives in the
    apis/ directory (depth 1). The API docs themselves live at their original locations
    (data/api/, train/api/, ...), so each link is ``../<path>``. Strip the single
    leading ``../`` to make hrefs root-relative; the loader then resolves them against
    each page's URL root."""
    def repl(m):
        href = m.group(1)
        if re.match(r"^(https?:|/|#|mailto:)", href):
            return m.group(0)
        if href.startswith("../"):
            href = href[3:]
        return 'href="%s"' % href
    return re.sub(r'href="([^"]*)"', repl, html)


def _render_and_write(app, exc):
    """build-finished (main process, writer ready): render the captured global toc,
    keep ONLY the APIs section, and write it as the shared fragment.

    global_toctree_for_doc returns the whole-site nav (all top-level sections), so we
    isolate the API subtree here: because the toc was resolved for apis/index, the
    "APIs" top-level entry is the one marked ``current`` -- we keep just its child
    list (the libraries and their symbols). Leaving the API section is the top nav's
    job, so nothing else belongs in this sidebar."""
    if exc is not None or _state.get("node") is None:
        return
    try:
        html = app.builder.render_partial(_state["node"])["fragment"]
    except Exception as e:
        logger.warning("[api_sidebar] could not render apis nav fragment: %s", e)
        return
    soup = bs4.BeautifulSoup(html, "html.parser")
    apis_li = next(
        (li for li in soup.select("li.toctree-l1") if "current" in (li.get("class") or [])),
        None,
    )
    libs = apis_li.find("ul") if apis_li else None
    if libs is None:
        logger.warning("[api_sidebar] could not isolate the APIs section in the toc; "
                       "fragment not written")
        return
    libs["class"] = "nav bd-sidenav"
    # render_partial emits plain nested <ul>; reuse pydata's helper to add the
    # collapsible <details>/<summary> structure (closed by default) so the loader can
    # expand the current path and the theme's CSS styles the chevrons natively.
    frag_soup = bs4.BeautifulSoup(str(libs), "html.parser")
    add_collapse_checkboxes(frag_soup)
    html = _root_relative(str(frag_soup))
    static_dir = os.path.join(app.outdir, "_static")
    os.makedirs(static_dir, exist_ok=True)
    with open(os.path.join(static_dir, NAV_FILENAME), "w", encoding="utf-8") as fh:
        fh.write(html)
    logger.info("[api_sidebar] wrote shared apis nav fragment (%d KB, API subtree only) "
                "to _static/%s", len(html) // 1024, NAV_FILENAME)


def _on_html_page_context(app, pagename, templatename, context, doctree):
    if pagename.startswith(API_PATH_PREFIXES):
        context["sidebars"] = ["api-sidebar.html"]


def setup(app):
    # priority < 500 so we capture the toctree BEFORE sphinx_remove_toctrees prunes it.
    app.connect("env-updated", _capture_apis_toctree, priority=1)
    app.connect("build-finished", _render_and_write)
    app.connect("html-page-context", _on_html_page_context, priority=900)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}
