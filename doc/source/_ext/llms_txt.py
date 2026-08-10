"""Generate ``llms.txt`` and per-section ``llms-full.txt`` files for AI agents.

This in-repo Sphinx extension replaces the third-party ``sphinx-llms-txt`` with
output structured by the site's navigation (the `llms.txt spec
<https://llmstxt.org/>`_ and most coding-assistant tooling favor sectioned
output). It emits three kinds of file into the HTML build output:

``llms.txt`` (root)
    The index. ``# title`` + ``> summary`` blockquote, a pointer to
    ``llms-full.txt``, then one ``## Section`` per top-level ``toctree`` entry of
    the root document. Each section lists its landing page and **every** in-scope
    page beneath it (the full ``toctree`` subtree, deduped) as
    ``- [title](url): description`` lines, so an agent can find any page from the
    index itself without drilling into the corpus. A page the nav toctrees never
    reach (e.g. an example linked only from a gallery grid) is folded into the
    section that owns its top-level directory, so it still lists under its
    library. Sections named in ``llms_txt_optional_sections`` move to a trailing
    ``## Optional`` section; only pages whose directory maps to no section at all
    land in a final ``## Other pages`` section, so the index stays complete.

``<section>/llms-full.txt``
    The verbatim source of every in-scope page under a section, each prefixed
    with a ``# title`` / ``Source: url`` header and separated by ``---``, behind
    a ``## Contents`` TOC. Sharded per directory because the whole corpus is far
    larger than any context window; a section that still exceeds
    ``llms_txt_full_max_shard_tokens`` is split further into per-subdirectory
    sub-shards (e.g. ``cluster/kubernetes/llms-full.txt``) so each loadable unit
    stays within an agent's effective context budget.

``llms-full.txt`` (root)
    A manifest that links to every shard — sub-shards nested under their parent
    section, each with a description and page count — so an agent can land at the
    root and route to exactly the unit it needs without downloading anything.

A page's description resolves in three steps: the ``description`` key in
front-matter/docinfo (``env.metadata``), then a ``<meta name="description">``
node (MyST ``html_meta`` or an RST ``.. meta::`` directive), then the page's
first real paragraph. ``llms_txt_exclude`` (fnmatch globs over docnames) drops
low-signal pages such as auto-generated API reference; Jupyter notebooks are
dropped automatically (by source suffix), so they need no exclude entry.

Config values (set generic defaults here; Ray specifics live in ``conf.py``):

``llms_txt_title``
    H1 title for the manifests. Defaults to the Sphinx ``project``.
``llms_txt_summary``
    Blockquote summary placed under the H1.
``llms_txt_exclude``
    List of fnmatch globs (matched against docnames) to omit from all output.
``llms_txt_optional_sections``
    Section labels to render under ``## Optional`` instead of inline.
``llms_txt_full``
    Whether to emit the per-section ``llms-full.txt`` shards and root manifest
    (default ``True``).
``llms_txt_full_max_shard_tokens``
    Approximate token budget above which a section shard is split into
    per-subdirectory sub-shards (default ``200000``; ~4 chars/token).
``llms_txt_build``
    Master switch (default ``True``). Set to ``False`` to skip all generation —
    e.g. on RtD PR previews, where the agent corpus isn't review-critical and
    the full-source read is wasted work.
``llms_txt_base_url``
    Absolute base URL for generated links. Defaults to ``html_baseurl``; set it
    to the current build's canonical URL (e.g. ``READTHEDOCS_CANONICAL_URL``) so
    links track the version being built rather than a pinned SEO canonical.

All work happens in ``build-finished`` so it is parallel-safe, and the module
sticks to APIs that survive the Sphinx 8 -> 9 jump (``findall`` not
``traverse``, ``root_doc`` not ``master_doc``, ``docutils.nodes.meta``).
"""

from __future__ import annotations

import fnmatch
from pathlib import Path

from docutils import nodes
from sphinx import addnodes
from sphinx.util import logging

logger = logging.getLogger(__name__)

# Fallback first-paragraph descriptions are truncated to roughly one sentence.
_FALLBACK_MAX_CHARS = 250
# Paragraphs shorter than this are skipped as fallbacks (badges, one-word lines).
_FALLBACK_MIN_CHARS = 30
# Rough bytes->tokens divisor for sizing full-text shards against a budget.
_CHARS_PER_TOKEN = 4


def _meta_node_types() -> tuple:
    """Meta-node classes to scan, across docutils/Sphinx versions.

    ``docutils.nodes.meta`` is the modern home (docutils >= 0.18, and the only
    one in Sphinx 9); older Sphinx also exposed ``sphinx.addnodes.meta``.
    """
    types = []
    meta = getattr(nodes, "meta", None)
    if meta is not None:
        types.append(meta)
    legacy = getattr(addnodes, "meta", None)
    if legacy is not None and legacy not in types:
        types.append(legacy)
    return tuple(types)


def _get_doctree(env, docname, cache):
    """Return the (unresolved) doctree for ``docname``, cached; None on error."""
    if docname not in cache:
        try:
            cache[docname] = env.get_doctree(docname)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[llms_txt] could not read doctree for %s: %s", docname, exc)
            cache[docname] = None
    return cache[docname]


def _clean(text) -> str:
    """Collapse whitespace/newlines into a single line, coercing non-strings.

    A front-matter/docinfo ``description`` can be parsed as a non-string — a
    YAML scalar (number, bool) or a list — so coerce rather than let a stray
    value raise ``AttributeError`` and crash the whole build.
    """
    if text is None:
        return ""
    if isinstance(text, (list, tuple)):
        text = " ".join(str(item) for item in text)
    elif not isinstance(text, str):
        text = str(text)
    return " ".join(text.split())


def _doc_title(env, docname: str) -> str:
    """Human title for a docname, falling back to the docname itself."""
    title = getattr(env, "titles", {}).get(docname)
    return title.astext() if title is not None else docname


def _toctree_children(env, docname, cache):
    """Yield ``(title, child_docname)`` for the direct toctree entries of a doc.

    ``title`` is the explicit toctree label (``Title <doc>``) when present, else
    the child page's own title. External links, ``self`` entries, and unknown
    docnames are skipped. Order and de-duplication follow document order.
    """
    doctree = _get_doctree(env, docname, cache)
    if doctree is None:
        return []
    seen = set()
    children = []
    for toctree in doctree.findall(addnodes.toctree):
        for title, ref in toctree["entries"]:
            if not ref or ref == "self" or "://" in ref:
                continue
            if ref not in env.all_docs or ref in seen:
                continue
            seen.add(ref)
            children.append((title or _doc_title(env, ref), ref))
    return children


def _first_paragraph(doctree) -> str:
    """First substantive paragraph of a doctree, truncated; '' if none."""
    if doctree is None:
        return ""
    for para in doctree.findall(nodes.paragraph):
        text = _clean(para.astext())
        if len(text) >= _FALLBACK_MIN_CHARS:
            if len(text) > _FALLBACK_MAX_CHARS:
                text = text[:_FALLBACK_MAX_CHARS].rsplit(" ", 1)[0] + "…"
            return text
    return ""


def _curated_description(env, docname, meta_types, cache):
    """Return the page's authored description (metadata or meta node), or None.

    This is the curated `<meta name="description">` — front-matter/docinfo
    ``description`` or an html_meta/``.. meta::`` node — excluding the
    first-paragraph fallback.
    """
    metadata = getattr(env, "metadata", {}).get(docname, {})
    if metadata.get("description"):
        return _clean(metadata["description"])

    doctree = _get_doctree(env, docname, cache)
    if doctree is not None and meta_types:
        for node in doctree.findall(lambda n: isinstance(n, meta_types)):
            if (
                node.get("name") == "description"
                or node.get("property") == "description"
            ):
                content = node.get("content")
                if content:
                    return _clean(content)
    return None


def _description(env, docname, meta_types, cache) -> str:
    """Resolve a page description: curated (metadata/meta node) -> first paragraph."""
    curated = _curated_description(env, docname, meta_types, cache)
    if curated:
        return curated
    return _first_paragraph(_get_doctree(env, docname, cache))


def _is_excluded(docname: str, patterns) -> bool:
    # fnmatchcase (not fnmatch) so matching is case-sensitive on every platform;
    # fnmatch case-normalizes per-OS, but Sphinx docnames are case-sensitive.
    return any(fnmatch.fnmatchcase(docname, pat) for pat in patterns)


def _is_notebook(env, docname) -> bool:
    """True if the page's source is a Jupyter notebook.

    Tested by source suffix at build time, so it catches notebooks fetched into
    the build (e.g. by sphinx-collections) as well as checked-in ones — a
    conf-load-time file scan can't see build-time-generated files. Raw notebook
    JSON (cells, outputs, embedded base64 images) is high-bytes, low-signal for
    an agent corpus, so notebooks are dropped from all output.
    """
    try:
        return str(env.doc2path(docname)).endswith(".ipynb")
    except Exception:  # pragma: no cover - defensive
        return False


def _excluded(env, docname, patterns) -> bool:
    """Whether to drop a page from llms output: it matches an exclude glob or
    its source is a notebook."""
    return _is_excluded(docname, patterns) or _is_notebook(env, docname)


def _top_dir(docname: str) -> str:
    return docname.split("/", 1)[0] if "/" in docname else docname


def _shard_relpath(docname: str) -> str:
    """Logical path used for section grouping and shard-tree placement.

    Pages fetched into ``_collections/<library>/...`` at build time (external
    example repos, linked from galleries rather than toctrees) are keyed under
    ``<library>`` — the ``_collections/`` prefix stripped — so a Serve tutorial
    living at ``_collections/serve/tutorials/…`` groups with Ray Serve in BOTH
    the index and the full-text shards, not a synthetic ``_collections`` bucket.
    Only grouping/placement uses this; the real docname still reads source and
    builds URLs.
    """
    if docname.startswith("_collections/"):
        return docname[len("_collections/") :]
    return docname


def _match_dir(docname: str) -> str:
    """Top-level directory a page is grouped under (see ``_shard_relpath``)."""
    return _top_dir(_shard_relpath(docname))


def _base_url(config) -> str:
    """Absolute base URL for generated links (trailing slash stripped).

    Prefers ``llms_txt_base_url`` — set it to the *current build's* canonical URL
    (e.g. Read the Docs' ``READTHEDOCS_CANONICAL_URL``, which carries the correct
    host and version, including PR-preview hosts) so links track the version
    being built. Falls back to the SEO-pinned ``html_baseurl``.
    """
    base = (
        getattr(config, "llms_txt_base_url", None)
        or getattr(config, "html_baseurl", "")
        or ""
    )
    return base.rstrip("/")


def _page_url(app, docname: str) -> str:
    """Absolute URL for a page."""
    base = _base_url(app.config)
    uri = app.builder.get_target_uri(docname)
    return f"{base}/{uri}" if base else uri


def _asset_url(app, relpath: str) -> str:
    """Absolute URL for a build-output asset (e.g. a generated llms-full.txt)."""
    base = _base_url(app.config)
    return f"{base}/{relpath}" if base else relpath


def _link(label: str, url: str, description: str) -> str:
    """Render a single ``llms.txt`` list item."""
    if description:
        return f"- [{label}]({url}): {description}"
    return f"- [{label}]({url})"


def _collect_pages(env, landing, cache, exclude, seen):
    """Recursively collect ``(title, docname)`` for every in-scope toctree
    descendant of ``landing``, in document order, deduped via ``seen`` (each page
    is listed once, under the first section that reaches it)."""
    pages = []
    for title, child in _toctree_children(env, landing, cache):
        if child in seen or _excluded(env, child, exclude):
            continue
        seen.add(child)
        pages.append((title, child))
        pages.extend(_collect_pages(env, child, cache, exclude, seen))
    return pages


def _build_sections(app, env, exclude, cache):
    """Build ``([(label, landing, pages), ...], unreached)``.

    ``pages`` is every in-scope page under a section — the full toctree subtree,
    deduped so each page appears under only the first section that reaches it —
    so the index can list any page without an agent having to drill into the
    corpus.

    Pages the nav toctrees never reach are then folded into the section that
    owns their top-level directory. Ray drives its example galleries with grid
    cards and query-param links (from ``examples.yml``), not ``toctree``, so a
    Serve tutorial or Train example is in-scope yet unreached; directory folding
    lists it under its library instead of a catch-all. ``unreached`` is only
    what is left — pages whose directory maps to no section — swept into a
    trailing ``## Other pages`` group so the index still stays complete.
    """
    root_doc = getattr(env.config, "root_doc", None) or getattr(
        env.config, "master_doc", "index"
    )
    seen = {root_doc}
    sections = []
    for label, landing in _toctree_children(env, root_doc, cache):
        if landing in seen or _excluded(env, landing, exclude):
            continue
        seen.add(landing)
        pages = _collect_pages(env, landing, cache, exclude, seen)
        sections.append((label, landing, pages))

    # Fold each un-navigated page into the section owning its directory (first
    # section to land in that directory wins, matching the full-text grouping).
    dir_to_idx: dict[str, int] = {}
    for i, (_label, landing, _pages) in enumerate(sections):
        dir_to_idx.setdefault(_top_dir(landing), i)
    unreached = []
    for docname in sorted(env.all_docs):
        if docname in seen or _excluded(env, docname, exclude):
            continue
        idx = dir_to_idx.get(_match_dir(docname))
        if idx is None:
            unreached.append(docname)
        else:
            # Appended after the toctree-walked pages, in docname order (the
            # loop iterates sorted docnames).
            sections[idx][2].append((_doc_title(env, docname), docname))
    return sections, unreached


def _render_index(
    app,
    env,
    sections,
    unreached,
    title,
    summary,
    optional,
    meta_types,
    cache,
    full_enabled,
):
    """Render the root ``llms.txt`` index as a string.

    Lists every in-scope page under its nav section (so an agent can find any
    page in the index itself, without drilling into the corpus), points at the
    full-text ``llms-full.txt``, and sweeps any un-navigated pages into a
    trailing ``## Other pages`` section.
    """
    lines = [f"# {title}", ""]
    if summary:
        lines += [f"> {summary}", ""]
    if full_enabled:
        lines += [
            "Full page text, grouped by section, is in "
            f"[llms-full.txt]({_asset_url(app, 'llms-full.txt')}).",
            "",
        ]

    def entry(label, docname):
        return _link(
            label,
            _page_url(app, docname),
            _description(env, docname, meta_types, cache),
        )

    def render(label, landing, pages, heading=None):
        out = [f"## {heading or label}", ""]
        out.append(entry(_doc_title(env, landing), landing))
        out += [entry(page_title, page) for page_title, page in pages]
        out.append("")
        return out

    main = [s for s in sections if s[0] not in optional]
    optional_secs = [s for s in sections if s[0] in optional]
    for label, landing, pages in main:
        lines += render(label, landing, pages)
    if optional_secs:
        # Flatten optional sections' links under the single Optional heading.
        lines += ["## Optional", ""]
        for label, landing, pages in optional_secs:
            lines.append(entry(label, landing))
            lines += [entry(page_title, page) for page_title, page in pages]
        lines.append("")
    if unreached:
        lines += ["## Other pages", ""]
        lines += [entry(_doc_title(env, page), page) for page in unreached]
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _src_size(env, docname) -> int:
    """Byte size of a page's source file (0 if unreadable)."""
    try:
        return Path(env.doc2path(docname)).stat().st_size
    except OSError:  # pragma: no cover - defensive
        return 0


def _humanize(segment: str) -> str:
    """Turn a path segment into a display label: ``running-applications`` ->
    ``Running Applications``."""
    return segment.replace("-", " ").replace("_", " ").strip().title() or segment


def _lead_page(env, prefix, docnames, dir_landing, meta_types, cache):
    """Return ``(lead_docname, confident)`` for a shard group.

    The lead is the page that best represents the group: the section's nav
    landing, then a ``<prefix>/index`` page, then the first page carrying an
    authored description. ``confident`` is ``False`` when none of those exist and
    we fall back to an arbitrary first page — so callers can avoid labelling a
    shard with an unrepresentative page title/description.
    """
    landing = dir_landing.get(prefix)
    if landing in docnames:
        return landing, True
    if f"{prefix}/index" in docnames:
        return f"{prefix}/index", True
    curated = next(
        (d for d in docnames if _curated_description(env, d, meta_types, cache)), None
    )
    if curated:
        return curated, True
    return docnames[0], False


def _shard_identity(env, prefix, docnames, dir_landing, dir_label, meta_types, cache):
    """Return ``(lead, label, description)`` for a shard group.

    A top-level nav section uses its toctree label; an identifiable sub-group
    (landing/index/curated lead) uses the lead page's title and description; a
    sub-group with no representative page falls back to a humanized directory
    name and an empty description rather than an arbitrary page's text.
    """
    lead, confident = _lead_page(env, prefix, docnames, dir_landing, meta_types, cache)
    if dir_label.get(prefix):
        return lead, dir_label[prefix], _description(env, lead, meta_types, cache)
    if confident:
        return lead, _doc_title(env, lead), _description(env, lead, meta_types, cache)
    return lead, _humanize(prefix.rsplit("/", 1)[-1]), ""


def _write_shard(app, env, outdir, relpath, label, description, lead, docnames):
    """Write one ``llms-full.txt`` shard (lead page first); return its page count.

    Body = ``# label: full text`` header, optional ``> description``, a
    ``## Contents`` TOC, then the verbatim source of every page.
    """
    ordered = [lead] + [d for d in docnames if d != lead]
    body = [f"# {label}: full text", ""]
    if description:
        body += [f"> {description}", ""]
    body += ["## Contents", ""]
    body += [f"- [{_doc_title(env, d)}]({_page_url(app, d)})" for d in ordered]
    body += ["", "---", ""]
    for d in ordered:
        try:
            content = Path(env.doc2path(d)).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:  # pragma: no cover - defensive
            # UnicodeDecodeError is a ValueError, not an OSError, so decoding a
            # non-UTF-8 source would otherwise escape this handler and abort the
            # build with a traceback from inside `build-finished`.
            logger.warning("[llms_txt] could not read source for %s: %s", d, exc)
            continue
        body += [
            f"# {_doc_title(env, d)}",
            f"Source: {_page_url(app, d)}",
            "",
            content.strip(),
            "",
            "---",
            "",
        ]
    path = outdir / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(body).rstrip() + "\n", encoding="utf-8")
    return len(ordered)


def _emit_shards(
    app,
    env,
    prefix,
    docnames,
    depth,
    budget,
    dir_landing,
    dir_label,
    meta_types,
    cache,
    outdir,
    records,
):
    """Write a shard for ``prefix``, splitting into sub-shards when it exceeds
    ``budget`` bytes and has subdirectories to split along (recursively).

    Appends ``(relpath_or_None, label, description, page_count, depth)`` records
    in display order. A split parent keeps the pages held directly at its level;
    if it has none, it contributes a header-only record (no shard file).
    """
    total = sum(_src_size(env, d) for d in docnames)

    # Partition into subdirectory groups vs pages held directly at this level.
    # Split on the logical path (``_collections/`` alias stripped) so fetched
    # pages nest into their library's shard tree; the real docname is kept for
    # reading source and building URLs.
    subgroups: dict[str, list[str]] = {}
    direct = []
    for d in docnames:
        rest = _shard_relpath(d)[len(prefix) + 1 :]
        if "/" in rest:
            subgroups.setdefault(rest.split("/", 1)[0], []).append(d)
        else:
            direct.append(d)

    # Single shard when small enough, or when there's nothing to split along.
    if total <= budget or not subgroups:
        if total > budget:
            logger.info(
                "[llms_txt] shard %s ~%dk tokens but has no subdirectories to "
                "split along; emitting whole",
                prefix,
                total // _CHARS_PER_TOKEN // 1000,
            )
        lead, label, desc = _shard_identity(
            env, prefix, docnames, dir_landing, dir_label, meta_types, cache
        )
        relpath = f"{prefix}/llms-full.txt"
        count = _write_shard(app, env, outdir, relpath, label, desc, lead, docnames)
        records.append((relpath, label, desc, count, depth))
        return

    # Split: the parent keeps its directly-held pages; recurse into subdirs.
    if direct:
        lead, label, desc = _shard_identity(
            env, prefix, direct, dir_landing, dir_label, meta_types, cache
        )
        relpath = f"{prefix}/llms-full.txt"
        count = _write_shard(app, env, outdir, relpath, label, desc, lead, direct)
        records.append((relpath, label, desc, count, depth))
    else:
        _lead, label, desc = _shard_identity(
            env, prefix, docnames, dir_landing, dir_label, meta_types, cache
        )
        records.append((None, label, desc, 0, depth))

    for sub in sorted(subgroups):
        _emit_shards(
            app,
            env,
            f"{prefix}/{sub}",
            subgroups[sub],
            depth + 1,
            budget,
            dir_landing,
            dir_label,
            meta_types,
            cache,
            outdir,
            records,
        )


def _write_full_files(
    app, env, exclude, title, summary, sections, meta_types, cache, max_tokens
):
    """Write per-section ``llms-full.txt`` shards and the root manifest.

    Each shard opens with a ``## Contents`` TOC (surfacing deep pages absent from
    the root ``llms.txt`` index) followed by verbatim page source, landing page
    first. Sections whose source exceeds ``max_tokens`` (estimated) are split
    into per-subdirectory sub-shards so every loadable unit stays within an
    agent's context budget. The root manifest lists every shard — nested under
    its parent section — with a description and page count, so an agent can route
    without downloading anything.
    """
    outdir = Path(app.outdir)
    root_doc = getattr(env.config, "root_doc", None) or getattr(
        env.config, "master_doc", "index"
    )
    budget = max(1, max_tokens) * _CHARS_PER_TOKEN

    # Group every in-scope page by its section directory (build-fetched
    # ``_collections/<lib>/…`` pages key under ``<lib>``; see ``_shard_relpath``)
    # so the full-text shards match the index's section membership.
    groups: dict[str, list[str]] = {}
    for docname in env.all_docs:
        if docname == root_doc or _excluded(env, docname, exclude):
            continue
        groups.setdefault(_match_dir(docname), []).append(docname)

    # Map each directory to its nav-section label + landing page (first wins).
    dir_label, dir_landing = {}, {}
    for label, landing, _children in sections:
        directory = _top_dir(landing)
        dir_label.setdefault(directory, label)
        dir_landing.setdefault(directory, landing)

    records = []
    for directory in sorted(groups):
        _emit_shards(
            app,
            env,
            directory,
            sorted(groups[directory]),
            0,
            budget,
            dir_landing,
            dir_label,
            meta_types,
            cache,
            outdir,
            records,
        )

    manifest = [f"# {title}: full documentation", ""]
    if summary:
        manifest += [f"> {summary}", ""]
    manifest += [
        "Full page text grouped by section, one file each (large sections are "
        "split into sub-shards, nested below). Each entry gives the section, its "
        "page count, and what it covers — fetch only what you need:",
        "",
    ]
    shard_files = 0
    for relpath, label, description, count, depth in records:
        indent = "  " * depth
        if relpath:
            shard_files += 1
            entry = f"{indent}- [{label}]({_asset_url(app, relpath)}) ({count} pages)"
        else:
            entry = f"{indent}- **{label}**"
        if description:
            entry += f": {description}"
        manifest.append(entry)

    manifest.append("")
    (outdir / "llms-full.txt").write_text(
        "\n".join(manifest).rstrip() + "\n", encoding="utf-8"
    )
    logger.info("[llms_txt] wrote llms-full.txt + %d section shards", shard_files)


def on_build_finished(app, exception):
    """Emit the manifests after a successful HTML build."""
    if exception is not None:
        return
    if app.builder.name not in ("html", "dirhtml"):
        return

    config = app.config
    if not getattr(config, "llms_txt_build", True):
        logger.info(
            "[llms_txt] skipped (llms_txt_build is False; e.g. a PR preview build)"
        )
        return

    if not _base_url(config):
        logger.warning(
            "[llms_txt] no base URL (set llms_txt_base_url or html_baseurl); "
            "generated links will be relative, but the llms.txt spec expects "
            "absolute URLs."
        )

    env = app.env
    exclude = list(getattr(config, "llms_txt_exclude", None) or [])
    optional = set(getattr(config, "llms_txt_optional_sections", None) or [])
    title = getattr(config, "llms_txt_title", None) or getattr(
        config, "project", "Documentation"
    )
    summary = getattr(config, "llms_txt_summary", "") or ""
    meta_types = _meta_node_types()
    cache: dict = {}

    full_enabled = getattr(config, "llms_txt_full", True)
    sections, unreached = _build_sections(app, env, exclude, cache)
    index = _render_index(
        app,
        env,
        sections,
        unreached,
        title,
        summary,
        optional,
        meta_types,
        cache,
        full_enabled,
    )
    (Path(app.outdir) / "llms.txt").write_text(index, encoding="utf-8")
    logger.info("[llms_txt] wrote llms.txt (%d sections)", len(sections))

    if full_enabled:
        max_tokens = getattr(config, "llms_txt_full_max_shard_tokens", None) or 200000
        _write_full_files(
            app, env, exclude, title, summary, sections, meta_types, cache, max_tokens
        )


def setup(app):
    app.add_config_value("llms_txt_title", None, "html")
    app.add_config_value("llms_txt_summary", "", "html")
    app.add_config_value("llms_txt_exclude", [], "html")
    app.add_config_value("llms_txt_optional_sections", [], "html")
    app.add_config_value("llms_txt_full", True, "html")
    app.add_config_value("llms_txt_full_max_shard_tokens", 200000, "html")
    app.add_config_value("llms_txt_build", True, "html")
    app.add_config_value("llms_txt_base_url", None, "html")

    app.connect("build-finished", on_build_finished)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
