"""Keep MyST ``{eval-rst}`` blocks from pickling the build environment into doctrees.

MyST parses an ``{eval-rst}`` block into a throwaway docutils document that shares
the page's ``settings`` object, then moves the parsed nodes into the page with
``extend()`` (``myst_parser.mdit_to_docutils.base.render_restructuredtext``).
docutils re-points ``node.document`` only for the direct children it appends, so
every deeper node keeps a reference to the throwaway document. That document's
``settings.env`` is the live ``BuildEnvironment``.

Sphinx clears ``settings.env`` on the page's own doctree before pickling it, but
not on that throwaway document, so pickling the page pickles a full copy of the
environment with it, including viewcode's cache of Python module sources. Each
Markdown page with an ``{eval-rst}`` block wrote a 28 to 40 MB doctree instead of
tens of KB, and the parallel HTML writer loads those doctrees back into memory.
That was enough to get the Read the Docs build killed for running out of memory.

The fix: at ``doctree-read``, the last event before Sphinx pickles the doctree,
point every node that still references another document at the page's own
doctree. The nodes are part of that doctree by then, so this is the reference
docutils would have set had it re-pointed the whole subtree.
"""

from docutils import nodes
from sphinx.util import logging as sphinx_logging

logger = sphinx_logging.getLogger(__name__)


def adopt_foreign_nodes(doctree: nodes.document) -> int:
    """Point every node in ``doctree`` that references another document at ``doctree``.

    Returns the number of nodes re-pointed.
    """
    adopted = 0
    for node in doctree.findall():
        document = getattr(node, "document", None)
        if document is not None and document is not doctree:
            node.document = doctree
            adopted += 1
    return adopted


def _on_doctree_read(app, doctree):
    adopted = adopt_foreign_nodes(doctree)
    if adopted:
        logger.debug(
            "[myst_eval_rst_doctree] %s: re-pointed %d nodes from a nested document",
            app.env.docname,
            adopted,
        )


def setup(app):
    # Run after every other doctree-read handler (default priority 500), so nodes
    # any of them insert are covered too.
    app.connect("doctree-read", _on_doctree_read, priority=900)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}
