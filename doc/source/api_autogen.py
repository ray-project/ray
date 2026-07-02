"""Standalone autosummary stub generation for the public API reference.

The API-doc consistency check (``ci/ray_ci/doc``) reads autosummary stub
``.rst`` files that are generated from the directives in :data:`AUTOGEN_FILES`
-- for example the per-class method tables that the hand-written API pages
``.. include::``. Historically those stubs were produced only as a side effect
of a full ``make -C doc/ html`` (a hidden full-build dependency of every
Python-touching premerge PR). This module generates just the stubs, so the
check no longer needs the whole Sphinx render.

It also closes the silent-failure gap: :func:`generate_api_stubs` raises when
generation produces no files, instead of the old ``try/except`` in ``conf.py``
that downgraded any failure to a warning -- a broken autogen step now fails the
build loudly.

``conf.py`` imports :data:`AUTOGEN_FILES`, :data:`AUTOSUMMARY_FILENAME_MAP`, and
:func:`generate_api_stubs` from here, and importing this module registers the
custom Jinja filters the autosummary templates use, so the render and the
standalone path stay in lockstep. Run as a script, it generates the stubs and
exits nonzero on failure.
"""

import os
import sys
from importlib import import_module

from jinja2.filters import FILTERS
from sphinx.ext.autosummary import generate
from sphinx.util.inspect import safe_getattr

DEFAULT_API_GROUP = "Others"

# Source files whose autosummary directives drive stub generation. The
# generated stubs are ``.. include::``'d by the hand-written API pages and read
# by the API-doc consistency check, so this list is the single source of truth
# shared with conf.py.
AUTOGEN_FILES = [
    "data/api/_autogen.rst",
]

# Names whose generated stub filename differs from the object name.
AUTOSUMMARY_FILENAME_MAP = {
    "ray.serve.deployment": "ray.serve.deployment_decorator",
    "ray.serve.Deployment": "ray.serve.Deployment",
}


def filter_out_undoc_class_members(member_name, class_name, module_name):
    module = import_module(module_name)
    cls = getattr(module, class_name)
    if getattr(cls, member_name).__doc__:
        return f"~{class_name}.{member_name}"
    else:
        return ""


def has_public_constructor(class_name, module_name):
    cls = getattr(import_module(module_name), class_name)
    return _is_public_api(cls)


def get_api_groups(method_names, class_name, module_name):
    api_groups = set()
    cls = getattr(import_module(module_name), class_name)
    for method_name in method_names:
        method = getattr(cls, method_name)
        if _is_public_api(method):
            api_groups.add(
                safe_getattr(method, "_annotated_api_group", DEFAULT_API_GROUP)
            )

    return sorted(api_groups)


def select_api_group(method_names, class_name, module_name, api_group):
    cls = getattr(import_module(module_name), class_name)
    return [
        method_name
        for method_name in method_names
        if _is_public_api(getattr(cls, method_name))
        and _is_api_group(getattr(cls, method_name), api_group)
    ]


def _is_public_api(obj):
    api_type = safe_getattr(obj, "_annotated_type", None)
    if not api_type:
        return False
    return api_type.value == "PublicAPI"


def _is_api_group(obj, group):
    return safe_getattr(obj, "_annotated_api_group", DEFAULT_API_GROUP) == group


# Register the custom Jinja filters the autosummary templates (e.g.
# _templates/autosummary/class_v2.rst) use. Importing this module -- from
# conf.py or the standalone entry point below -- makes them available.
FILTERS["filter_out_undoc_class_members"] = filter_out_undoc_class_members
FILTERS["get_api_groups"] = get_api_groups
FILTERS["select_api_group"] = select_api_group
FILTERS["has_public_constructor"] = has_public_constructor


def _build_standalone_app(srcdir):
    """Build the minimal Sphinx stand-in that sphinx-autogen uses.

    When generation is invoked outside a full build (no real Sphinx app), this
    mirrors ``sphinx.ext.autosummary.generate.main``: a DummyApplication with
    the documenters set up, the project's ``_templates`` directory on the
    template path (so ``:template:`` references resolve), and the filename map.
    """
    import sphinx.locale
    from sphinx.ext.autosummary.generate import DummyApplication, setup_documenters
    from sphinx.util import logging as sphinx_logging

    sphinx.locale.init_console()
    app = DummyApplication(sphinx.locale.get_translator())
    sphinx_logging.setup(app, sys.stdout, sys.stderr)
    setup_documenters(app)
    app.config.templates_path.append(os.path.join(srcdir, "_templates"))
    app.config.autosummary_filename_map = AUTOSUMMARY_FILENAME_MAP
    return app


def generate_api_stubs(srcdir, app=None):
    """Generate the autosummary stub files for :data:`AUTOGEN_FILES`.

    Args:
        srcdir: The Sphinx source directory (``doc/source``) that
            :data:`AUTOGEN_FILES` are relative to.
        app: A live Sphinx application when called from the render's
            ``builder-inited`` hook; ``None`` when run standalone, in which case
            a minimal stand-in is built.

    Returns:
        The list of generated stub file paths.

    Raises:
        RuntimeError: If generation produced no files. This is the loud failure
            that replaces the previous silent ``try/except`` -- a broken
            autosummary source or template should fail the build, not pass as an
            empty fixture the consistency check then reads as "nothing to do".
    """
    sources = [os.path.join(srcdir, file) for file in AUTOGEN_FILES]
    if app is None:
        app = _build_standalone_app(srcdir)

    written = generate.generate_autosummary_docs(sources, app=app)

    if not written:
        raise RuntimeError(
            "API stub generation produced no files for "
            f"{AUTOGEN_FILES}; the autosummary sources or templates are likely "
            "broken. The API-doc consistency check depends on these stubs."
        )
    return written


def main(argv=None):
    # The module lives in the Sphinx source directory, so its own directory is
    # the srcdir the AUTOGEN_FILES are relative to.
    srcdir = os.path.dirname(os.path.abspath(__file__))
    try:
        written = generate_api_stubs(srcdir)
    except Exception as e:
        print(f"[api_autogen] ERROR: {e}", file=sys.stderr)
        return 1
    print(f"[api_autogen] generated {len(written)} API stub file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
