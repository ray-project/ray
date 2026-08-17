import json
import logging
import os
import pathlib
import re
import sys
from datetime import datetime
from dataclasses import is_dataclass
from typing import Any, Dict

import sphinx
from docutils import nodes
from sphinx.util.matching import compile_matchers

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath("."))
from custom_directives import (  # noqa
    DownloadAndPreprocessEcosystemDocs,
    update_context,
    LinkcheckSummarizer,
    parse_navbar_config,
    setup_context,
    pregenerate_example_rsts,
    generate_versions_json,
    collect_example_orphans,
)

# Importing api_autogen registers the custom autosummary Jinja filters and
# exposes the shared stub-generation entry point (see doc/source/api_autogen.py).
from api_autogen import (  # noqa: E402
    AUTOGEN_FILES,
    AUTOSUMMARY_FILENAME_MAP,
    generate_api_stubs,
)

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
assert not os.path.exists(
    "../../python/ray/_raylet.so"
), "_raylet.so should not be imported for the purpose for doc build, please rename the file to _raylet.so.bak and try again."
sys.path.insert(0, os.path.abspath("../../python/"))

# -- General configuration ------------------------------------------------

# This setting controls how single backticks are handled by sphinx. Developers
# are used to using single backticks for code, but RST syntax requires that code
# code to be denoted with _double_ backticks.
# Here we make sphinx treat single backticks as code also, because everyone is
# used to using single backticks as is done with markdown; without this setting,
# lots of documentation ends up getting committed with single backticks anyway,
# so we might as well make it work as developers intend for it to.
default_role = "code"

sys.path.append(os.path.abspath("./_ext"))

extensions = [
    "callouts",  # custom extension from _ext folder
    "queryparamrefs",
    "api_sidebar",  # APIs tab: shared client-side API nav (see _ext/api_sidebar.py)
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_click.ext",
    "sphinx-jsonschema",
    "sphinxemoji.sphinxemoji",
    "sphinx_copybutton",
    "sphinx_sitemap",
    "myst_nb",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.autosummary",
    "sphinxcontrib.autodoc_pydantic",
    "sphinxcontrib.redoc",
    "sphinx_remove_toctrees",
    "sphinx_design",
    "sphinx.ext.intersphinx",
    "sphinx_docsearch",
    "sphinx_collections",
    "llms_txt",  # in-repo extension from _ext folder (replaces sphinx-llms-txt)
    "sphinxext.opengraph",
]

# -- llms.txt: agent-friendly index + per-section full corpus -----------
# Emitted by the in-repo `llms_txt` extension (doc/source/_ext/llms_txt.py),
# which replaces the third-party `sphinx-llms-txt`.

# H1 title for llms.txt / llms-full.txt.
llms_txt_title = "Ray"

# Nav sections to move under llms.txt's trailing `## Optional` heading (content
# agents may skip to save context). Empty for v1.
llms_txt_optional_sections = []

# Skip the agent manifests on Read the Docs PR previews (they aren't
# review-critical and the full-source read is wasted work), generating them only
# on published builds (master, release tags) and local builds. `external` is
# RtD's version type for PR builds — the same signal `.readthedocs.yaml` keys
# the incremental-vs-full build off of. (DOC-1048)
llms_txt_build = os.getenv("READTHEDOCS_VERSION_TYPE") != "external"

# Blockquote summary for llms.txt, kept as editable prose in a sibling file so
# it can be updated without touching conf.py. `.txt` isn't a Sphinx source
# suffix, so the file isn't built as a page. Whitespace is collapsed to a single
# line for the `> summary` blockquote, so the file can be wrapped for readability.
llms_txt_summary = " ".join(
    (pathlib.Path(__file__).parent / "llms_txt_summary.txt")
    .read_text(encoding="utf-8")
    .split()
)

# Filter low-signal pages from llms-full.txt. Auto-generated API reference
# pages (one per public class/method) are excluded because they would
# dominate the corpus with autodoc boilerplate. Mirrors the directories in
# `remove_from_toctrees` below. Agents needing specific API details can
# fetch per-page markdown twins via Read the Docs' Markdown for Agents
# content negotiation. Tuning of this list is tracked separately.
llms_txt_exclude = [
    "search",
    "genindex",
    "404",
    "_TableOfContents",
    # Include-only fragments and template/example scaffolding — not standalone
    # nav pages; keep them out of the per-section llms-full shards.
    "_includes/*",
    "_templates/*",
    "templates/*",
    "cluster/running-applications/job-submission/doc/*",
    "ray-observability/reference/doc/*",
    "ray-core/api/doc/*",
    "ray-core/compiled-graph/doc/*",
    "data/api/doc/*",
    "train/api/doc/*",
    "tune/api/doc/*",
    "serve/api/doc/*",
    "rllib/package_ref/*",
    # Deprecated pages: surfacing a superseded API/guide to an agent is worse
    # than omitting it — the agent may follow the old API. (DOC-908)
    "train/api/deprecated",
    "train/deprecated-user-guides/*",
    # Retired Ray AIR namespace: orphaned, no longer in the site nav.
    "ray-air/deployment",
    # Include-only fragments spliced into other pages (no standalone title).
    "train/common/*",
    "ray-contribute/involvement",
    # Helper/utility code files and raw scripts rendered as pages — code, not
    # docs; the real guide pages that literalinclude them are kept.
    "train/user-guides/_collate_utils",
    "tune/examples/pbt_visualization/pbt_visualization_utils",
    "cluster/vms/user-guides/community/slurm-*",
    # Thin literalinclude example stubs (a heading + a code block, no prose).
    "tune/examples/includes/*",
]

# Jupyter notebooks are dropped from llms.txt / llms-full.txt by the llms_txt
# extension itself, which skips any page whose source is a `.ipynb` (by file
# suffix, at build time — so it also catches notebooks fetched into the build by
# sphinx-collections). No docname enumeration needed here. Notebooks remain
# fully rendered in the HTML build; only the agent corpus drops them.

# Thin API-reference hub pages: a title plus an autosummary table, under 40 words
# of real prose. The per-symbol pages they link to are already excluded above via
# the `*/api/doc/*` patterns, so an agent gains nothing from the shell — and a
# description on one would only restate its title. `data/api/_autogen` has no
# title at all, and the two `*_regression_example` pages are literalinclude
# stubs with zero prose. Every remaining in-scope page carries a curated
# description; these are the pages where exclusion beats describing.
llms_txt_exclude += [
    "data/api/_autogen",
    "data/api/aggregate",
    "data/api/api",
    "data/api/checkpoint",
    "data/api/data_context",
    "data/api/data_iterator",
    "data/api/dataset",
    "data/api/datatype",
    "data/api/execution_options",
    "data/api/grouped_data",
    "data/api/llm",
    "data/api/loading_data",
    "data/api/preprocessor",
    "data/api/saving_data",
    "ray-core/api/cli",
    "ray-core/api/core",
    "ray-core/api/exceptions",
    "ray-core/api/index",
    "ray-core/api/runtime-env",
    "ray-core/api/scheduling",
    "ray-core/api/utility",
    "ray-core/compiled-graph/compiled-graph-api",
    "train/examples/pytorch/torch_regression_example",
    "train/examples/tf/tensorflow_regression_example",
    "tune/api/api",
    "tune/api/execution",
    "tune/api/integration",
    "tune/api/internals",
    "tune/api/result_grid",
    "tune/api/search_space",
    "tune/api/syncing",
]

# -- sphinx-collections: pull external template files at build time -----------
# The fetch machinery, template registry, collections config, and _collections/
# Sphinx wiring live in template_collections.py so template-publishing changes
# stay scoped away from Sphinx config. See that module.
from template_collections import (
    collections,
    collections_clean,
    collections_final_clean,
)
import template_collections

# The collections config contains a function reference (for the "function" driver)
# which Sphinx cannot pickle for caching. This is harmless — suppress the warning
# so it doesn't cause a build failure under -W (warnings-as-errors).
suppress_warnings = [
    "config.cache",
    # sphinxcontrib-redoc (unmaintained, 1.6.0) redundantly copies its bundled
    # redoc.js asset; Sphinx 8's new copy_overwrite check flags the second copy over
    # the existing (identical) file. Benign and not fixable upstream.
    "misc.copy_overwrite",
]
# Disable autodoc_pydantic features that can produce empty raw directives
# (e.g. when schema JSON fails for models with non-serializable fields)
autodoc_pydantic_model_show_json = False

# Configuration for algolia
# Note: This API key grants read access to our indexes and is intended to be public.
# See https://www.algolia.com/doc/guides/security/api-keys/ for more information.
docsearch_app_id = "LBHF0PABBL"
docsearch_api_key = "6c42f30d9669d8e42f6fc92f44028596"
docsearch_index_name = "docs-ray"

# Remove the per-symbol autogenerated API reference pages (one page per
# class/method) from the rendered toctree via sphinx-remove-toctrees, so the
# navigation sidebar isn't swamped by thousands of API stubs. The pages are
# still generated and linked from the autosummary tables; this only drops them
# from the nav tree. These API-ref directories mirror the API-ref entries in
# `llms_txt_exclude` above, which excludes the same pages from the agent corpus.
remove_from_toctrees = [
    "cluster/running-applications/job-submission/doc/*",
    "ray-observability/reference/doc/*",
    "ray-core/api/doc/*",
    "data/api/doc/*",
    "train/api/doc/*",
    "tune/api/doc/*",
    "serve/api/doc/*",
    "rllib/package_ref/algorithm/*",
    "rllib/package_ref/policy/*",
    "rllib/package_ref/models/*",
    "rllib/package_ref/catalogs/*",
    "rllib/package_ref/rl_modules/*",
    "rllib/package_ref/learner/*",
    "rllib/package_ref/evaluation/*",
    "rllib/package_ref/replay-buffers/*",
    "rllib/package_ref/utils/*",
]

myst_enable_extensions = [
    "dollarmath",
    "amsmath",
    "deflist",
    "html_admonition",
    "html_image",
    "colon_fence",
    "smartquotes",
    "replacements",
    "substitution",
]

myst_heading_anchors = 4

# Reusable prose fragments for deprecation notices.
#
# A deprecation timeline that engineering and product have publicly committed to is a
# real future event, so these fragments say "will". Keeping the wording here rather than
# in the pages does three things: every notice reads the same, the phrasing changes in
# one place, and the sentence sits outside the paths Vale lints, so the blanket
# `Google.Will` rule can keep guarding ordinary prose.
#
# Pages supply the values through front matter. See doc/source/_includes/README.md.
myst_substitutions = {
    "deprecation_planned": (
        "Ray will deprecate {{ deprecated_feature }} in Ray {{ deprecated_in }}."
        '{{ " Use " + deprecation_replacement + " instead."'
        ' if deprecation_replacement is defined else "" }}'
    ),
    "deprecation_notice": (
        "Ray deprecated {{ deprecated_feature }} in Ray {{ deprecated_in }} and will "
        'remove it in {{ ("Ray " + removed_in) if removed_in is defined'
        ' else "a future release" }}.'
        '{{ " Use " + deprecation_replacement + " instead."'
        ' if deprecation_replacement is defined else "" }}'
    ),
}

# Add these for attachment handling
nb_render_key_pairs = {
    "html": [
        ("img", ["src", "alt"]),
    ]
}

nb_output_folder = "_build/jupyter_execute"

# Make broken internal references into build time errors.
# See https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-nitpicky
# for more information. :py:class: references are ignored due to false positives
# arising from type annotations. See https://github.com/ray-project/ray/pull/46103
# for additional context.
nitpicky = True
nitpick_ignore_regex = [
    ("py:obj", "ray.actor.T"),
    ("py:obj", "ray.data.aggregate.AccumulatorType"),
    ("py:obj", "ray.data.aggregate.SupportsRichComparisonType"),
    ("py:obj", "ray.data.aggregate.AggOutputType"),
    ("py:class", ".*"),
    # Workaround for https://github.com/sphinx-doc/sphinx/issues/10974
    ("py:obj", "ray\\.data\\.datasource\\.datasink\\.WriteReturnType"),
    # UnknownPreprocessorError is an internal exception not exported in public API
    ("py:exc", "UnknownPreprocessorError"),
    ("py:exc", "ray\\.data\\.preprocessors\\.version_support\\.UnknownPreprocessorError"),
    # TypeVar for gRPCInputStream generic type
    ("py:obj", "ray\\.serve\\.grpc_util\\.T"),
    # autodoc_pydantic generates invalid py:obj refs for pydantic v2 validators
    # (e.g. "all fields", "_validate_*" references in validator docstrings)
    ("py:obj", r"ray\.serve\.config\.\w+\.all fields"),
    ("py:obj", r"ray\.serve\.config\.GangSchedulingConfig\._validate_runtime_failure_policy"),
    ("py:obj", r"ray\.serve\.schema\.\w+\.all fields"),
    # autodoc_pydantic also emits invalid field refs for these dashboard job models.
    ("py:obj", r"ray\.dashboard\.modules\.job\.pydantic_models\.(DriverInfo|JobDetails)\.\w+"),
]

# Cache notebook outputs in _build/.jupyter_cache
# To prevent notebook execution, set this to "off". To force re-execution, set this to
# "force". To cache previous runs, set this to "cache".
nb_execution_mode = os.getenv("RUN_NOTEBOOKS", "off")

# Add a render priority for doctest
nb_mime_priority_overrides = [
    ("html", "application/vnd.jupyter.widget-view+json", 10),
    ("html", "application/javascript", 20),
    ("html", "text/html", 30),
    ("html", "image/svg+xml", 40),
    ("html", "image/png", 50),
    ("html", "image/jpeg", 60),
    ("html", "text/markdown", 70),
    ("html", "text/latex", 80),
    ("html", "text/plain", 90),
]

html_extra_path = ["robots.txt"]

html_baseurl = "https://docs.ray.io/en/latest/"

# Base URL for links in the generated llms.txt / llms-full.txt. Unlike the SEO
# `html_baseurl` (deliberately pinned to /en/latest/ as the canonical), these
# should point at the version actually being built, so a version's manifest and
# index links resolve within that same version — and PR previews are
# self-navigable rather than pointing at prod /en/latest/. RtD's
# `READTHEDOCS_CANONICAL_URL` carries the correct host + current version slug;
# fall back to html_baseurl for local builds. (DOC-1130)
llms_txt_base_url = os.getenv("READTHEDOCS_CANONICAL_URL") or html_baseurl

# `html_baseurl` already encodes `/en/latest/`, so override sphinx-sitemap's
# default `{lang}{version}{link}` scheme to just `{link}`. Otherwise the
# extension prepends `en/` again, producing URLs like `en/latesten/<page>`.
sitemap_url_scheme = "{link}"

# sphinxext-opengraph: emit Open Graph metadata per page. Pin `ogp_site_url`
# to `html_baseurl` so the `og:url` tag tracks the same canonical URL as
# Sphinx's `<link rel="canonical">`. If `ogp_site_url` were left unset, the
# extension would fall back to Read the Docs' `READTHEDOCS_CANONICAL_URL`
# env var (set by RtD's Addons framework from the project's "Canonical
# version" admin setting), which can diverge from `html_baseurl`. Per-page
# `:og:description:` and `:og:image:` can still be set in individual files.
ogp_site_url = html_baseurl

# This pattern matches:
# - Python Repl prompts (">>> ") and it's continuation ("... ")
# - Bash prompts ("$ ")
# - IPython prompts ("In []: ", "In [999]: ") and it's continuations
#   ("  ...: ", "     : ")
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True

# Ignore divs with class="no-copybutton"
copybutton_selector = "div:not(.no-copybutton) > div.highlight > pre"

# By default, tabs can be closed by selecting an open tab. We disable this
# functionality with the `sphinx_tabs_disable_tab_closing` option.
sphinx_tabs_disable_tab_closing = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Ray"
copyright = str(datetime.now().year) + ", The Ray Team"
author = "The Ray Team"

# The version info for the project you're documenting acts as replacement for
# |version| and |release|, and is also used in various other places throughout the
# built documents. Retrieve the version using `find_version` rather than importing
# directly (from ray import __version__) because initializing ray will prevent
# mocking of certain external dependencies.
from setup import find_version  # noqa

release = find_version("ray", "_version.py")

language = "en"

# autogen files are only used to auto-generate public API documentation.
# They are not included in the toctree to avoid warnings such as documents not included
# in any toctree.
autogen_files = AUTOGEN_FILES

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# Also helps resolve warnings about documents not included in any toctree.
exclude_patterns = [
    # Committed intersphinx inventory snapshots + refresh tooling, not docs.
    "_intersphinx/**",
    "templates/*",
    "cluster/running-applications/doc/ray.*",
    "data/api/ray.data.*.rst",
    # Hide README.md used for display on the console (anyscale templates)
    "serve/tutorials/**/content/**README.md",
    "data/examples/**/content/**README.md",
    "ray-overview/examples/**/content/**README.md",
    "ray-core/examples/**/content/**README.md",
    "train/examples/**/content/**README.md",
    "tune/examples/**/content/**README.md",
    # Other misc files (overviews, console-only examples, etc)
    "serve/tutorials/video-analysis/*.ipynb",
    # Legacy/backward compatibility
    "ray-overview/examples/**/README.md",
    "train/examples/**/README.md",
] + template_collections.exclude_patterns() + autogen_files

# If "DOC_LIB" is found, only build that top-level navigation item.
build_one_lib = os.getenv("DOC_LIB")

all_toc_libs = [
    f.path.strip("./") for f in os.scandir(".") if f.is_dir() and "ray-" in f.path
]
all_toc_libs += [
    "cluster",
    "tune",
    "data",
    "train",
    "rllib",
    "serve",
    "llm",
    "workflows",
]
if build_one_lib and build_one_lib in all_toc_libs:
    all_toc_libs.remove(build_one_lib)
    exclude_patterns += all_toc_libs


# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

# Do not check anchors for links because it produces many false positives
# and is slow (it needs to download the linked website).
linkcheck_anchors = False

if os.environ.get("LINKCHECK_ALL"):
    # Only check external links, i.e. the ones starting with http:// or https://.
    linkcheck_ignore = [
        r"^((?!http).)*$",  # exclude links not starting with http
        "http://ala2017.it.nuigalway.ie/papers/ALA2017_Gupta.pdf",  # broken
        "https://mvnrepository.com/artifact/*",  # working but somehow not with linkcheck
        # This should be fixed -- is temporal the successor of cadence? Do the examples need to be updated?
        "https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-cadence.md",
        "https://www.oracle.com/java/technologies/javase-jdk15-downloads.html",  # forbidden for client
        "https://speakerdeck.com/*",  # forbidden for bots
        r"https://huggingface.co/*",  # seems to be flaky
        r"https://www.meetup.com/*",  # seems to be flaky
        r"https://www.pettingzoo.ml/*",  # seems to be flaky
        r"http://localhost[:/].*",  # Ignore localhost links
        r"^http:/$",  # Ignore incomplete links
        # 403 Client Error: Forbidden for url.
        # They ratelimit bots.
        "https://www.datanami.com/2018/02/01/rays-new-library-targets-high-speed-reinforcement-learning/",
        # 403 Client Error: Forbidden for url.
        # They ratelimit bots.
        "https://www.researchgate.net/publication/222573328_Stochastic_Gradient_Boosting",
        "https://www.datanami.com/2019/11/05/why-every-python-developer-will-love-ray/",
        "https://dev.mysql.com/doc/connector-python/en/",
        # Returning 522s intermittently.
        "https://lczero.org/",
        # Returns 406 but remains accessible
        "https://www.uber.com/blog/elastic-xgboost-ray/",
        # Aggressive anti-bot checks
        "https://archive.vn/*",
        "https://archive.is/*",
        # 429: Rate limited
        "https://medium.com/*",
        "https://towardsdatascience.com/*",
    ]
else:
    # Only check links that point to the ray-project org on github, since those
    # links are under our control and therefore much more likely to be real
    # issues that we need to fix if they are broken.
    linkcheck_ignore = [
        r"^(?!https://(raw\.githubusercontent|github)\.com/ray-project/).*$"
    ]


# -- Options for HTML output ----------------------------------------------
def render_svg_logo(path):
    with open(pathlib.Path(__file__).parent / path, "r") as f:
        content = f.read()

    return content


# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "pydata_sphinx_theme"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "use_edit_page_button": True,
    "announcement": """Try Ray with $100 credit — <a target="_blank" href="https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=banner">Start now</a><button type="button" id="close-banner" aria-label="Close banner">&times;</button>""",
    "logo": {
        "svg": render_svg_logo("_static/img/ray_logo.svg"),
    },
    "navbar_start": ["navbar-ray-logo"],
    "navbar_end": [
        "theme-switcher",
        "version-switcher",
        "navbar-icon-links",
        "navbar-anyscale",
    ],
    "navbar_center": ["navbar-links"],
    "navbar_align": "left",
    "secondary_sidebar_items": [
        "page-toc",
        "edit-on-github",
    ],
    "content_footer_items": [
        "csat",
    ],
    "navigation_depth": 4,
    "pygments_light_style": "stata-dark",
    "pygments_dark_style": "stata-dark",
    "switcher": {
        "json_url": "https://docs.ray.io/en/master/_static/versions.json",
        "version_match": os.getenv("READTHEDOCS_VERSION", "master"),
    },
}

html_context = {
    "github_user": "ray-project",
    "github_repo": "ray",
    "github_version": "master",
    "doc_path": "doc/source/",
}

# Pick the sidebar template by build environment: Read the Docs builds
# (READTHEDOCS=True) use `main-sidebar-readthedocs`, all other builds use
# `main-sidebar`. The `ray-overview/examples` gallery page renders with no
# sidebar (empty list).
html_sidebars = {
    "**": [
        (
            "main-sidebar-readthedocs"
            if os.getenv("READTHEDOCS") == "True"
            else "main-sidebar"
        )
    ],
    "ray-overview/examples": [],
    # Custom 404 page (DOC-945): drop the section-navigation sidebar so the
    # standalone error page renders clean and centered, like the examples page.
    "404": [],
}

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
html_title = f"Ray {release}"

autodoc_typehints_format = "short"

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = "_static/favicon.ico"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# Output file base name for HTML help builder.
htmlhelp_basename = "Raydoc"

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    # 'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    # 'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    # 'preamble': '',
    # Latex figure (float) alignment
    # 'figure_align': 'htbp',
}

latex_documents = [
    (master_doc, "Ray.tex", "Ray Documentation", author, "manual"),
]

# -- Options for manual page output ---------------------------------------

man_pages = [(master_doc, "ray", "Ray Documentation", [author], 1)]

# -- Options for Texinfo output -------------------------------------------
texinfo_documents = [
    (
        master_doc,
        "Ray",
        "Ray Documentation",
        author,
        "Ray",
        "Ray provides a simple, universal API for building distributed applications.",
        "Miscellaneous",
    ),
]

# Python methods should be presented in source code order
autodoc_member_order = "bysource"

# Show type hints in both the signature and the Parameters description list
# (see custom.css). "documented" scopes the description-side types to params
# that already have a docstring entry, which keeps their prose descriptions and
# preserves short type names. Plain "both" with the default "all" target drops
# the descriptions and renders verbose typing spellings.
autodoc_typehints = "both"
autodoc_typehints_description_target = "documented"


def add_custom_assets(
    app: sphinx.application.Sphinx,
    pagename: str,
    templatename: str,
    context: Dict[str, Any],
    doctree: nodes.Node,
):
    """Add custom per-page assets.

    See documentation on Sphinx Core Events for more information:
    https://www.sphinx-doc.org/en/master/extdev/appapi.html#sphinx-core-events
    """
    if pagename == "index":
        app.add_css_file("css/index.css")
        app.add_js_file("js/index.js")
        return "index.html"  # Use the special index.html template for this page

    if pagename == "404":
        # Custom 404 page (DOC-945). Read the Docs serves this page's HTML for
        # any missing URL under the docs domain while the browser keeps the
        # originally requested (wrong) path, so the 404 template pins a <base>
        # to the canonical version root to keep every relative URL working.
        # Prefer Read the Docs' per-build canonical URL (which is correct on PR
        # previews and per-version builds); fall back to html_baseurl for local
        # builds where the env var is unset. Scoped to this page only, so it
        # cannot re-introduce the sitewide sidebar-href regression from #63343.
        base_url = os.environ.get("READTHEDOCS_CANONICAL_URL") or app.config.html_baseurl or "/"
        if not base_url.endswith("/"):
            base_url += "/"
        context["notfound_base_url"] = base_url
        app.add_css_file("css/404.css")
        return "404.html"  # Use the special 404.html template for this page

    if pagename == "ray-overview/examples":
        app.add_css_file("css/examples.css")
        app.add_js_file("js/examples.js")
        return "ray-overview/examples.html"

    if pagename in [
        "data/examples",
        "train/examples",
        "serve/examples",
    ]:
        return "examples.html"

    if pagename == "train/train":
        app.add_css_file("css/ray-train.css")
    elif pagename == "ray-overview/ray-libraries":
        app.add_css_file("css/ray-libraries.css")
    elif pagename == "ray-overview/use-cases":
        app.add_css_file("css/use_cases.css")


def _autogen_apis(app: sphinx.application.Sphinx):
    """
    Auto-generate public API documentation.

    Delegates to the shared generate_api_stubs (see doc/source/api_autogen.py),
    which raises if generation produces nothing. The failure is intentionally
    not swallowed: the API-doc consistency check reads these stubs, so a broken
    autogen step must fail the build instead of silently emitting an empty
    fixture.
    """
    generate_api_stubs(app.srcdir, app=app)


def process_signature(app, what, name, obj, options, signature, return_annotation):
    # Sphinx is unable to render dataclass with factory/`field`
    # https://github.com/sphinx-doc/sphinx/issues/10893
    if what == "class" and is_dataclass(obj):
        return signature.replace("<factory>", "..."), return_annotation


def setup(app):
    # Only generate versions JSON during RTD build
    if os.getenv("READTHEDOCS") == "True":
        generate_versions_json()

    pregenerate_example_rsts(app)

    # NOTE: 'MOCK' is a custom option we introduced to illustrate mock outputs. Since
    # `doctest` doesn't support this flag by default, `sphinx.ext.doctest` raises
    # warnings when we build the documentation.
    import doctest

    doctest.register_optionflag("MOCK")
    app.connect("html-page-context", update_context)

    app.add_config_value("navbar_content_path", "navbar.yml", "env")
    app.connect("config-inited", parse_navbar_config)
    app.connect("html-page-context", setup_context)
    app.connect("html-page-context", add_custom_assets)

    # https://github.com/ines/termynal
    app.add_js_file("js/termynal.js", defer="defer")
    app.add_css_file("css/termynal.css")

    app.add_js_file("js/custom.js", defer="defer")
    app.add_css_file("css/custom.css", priority=800)

    app.add_js_file("js/csat.js", defer="defer")
    app.add_css_file("css/csat.css")

    app.add_js_file("js/dismissable-banner.js", defer="defer")
    app.add_css_file("css/dismissable-banner.css")

    base_path = pathlib.Path(__file__).parent
    github_docs = DownloadAndPreprocessEcosystemDocs(base_path)
    # Download docs from ecosystem library repos
    app.connect("builder-inited", github_docs.write_new_docs)
    # Restore original file content after build
    app.connect("build-finished", github_docs.write_original_docs)

    # Hook into the logger used by linkcheck to display a summary at the end.
    linkcheck_summarizer = LinkcheckSummarizer()
    app.connect("builder-inited", linkcheck_summarizer.add_handler_to_linkcheck)
    app.connect("build-finished", linkcheck_summarizer.summarize)

    # Hook into the auto generation of public apis
    app.connect("builder-inited", _autogen_apis)

    app.connect("autodoc-process-signature", process_signature)

    class DuplicateObjectFilter(logging.Filter):
        def filter(self, record):
            # Intentionally allow duplicate object description of ray.actor.ActorMethod.bind:
            # once in Ray Core API and once in Compiled Graph API
            if "duplicate object description of ray.actor.ActorMethod.bind" in record.getMessage():
                return False  # Don't log this specific warning
            return True  # Log all other warnings

    logging.getLogger("sphinx").addFilter(DuplicateObjectFilter())

    template_collections.register(app)

    # Register hook to mark orphan documents
    example_orphan_documents = collect_example_orphans(app.confdir, app.srcdir)
    def mark_orphans(app, docname, _source):
        if docname in example_orphan_documents:
            app.env.metadata.setdefault(docname, {})
            app.env.metadata[docname]["orphan"] = True

    app.connect('source-read', mark_orphans)


    app.add_config_value("ipython3_lexer_patterns", [], "env")
    app.add_config_value("ipython3_lexer_exclude_patterns", [], "env")
    app.connect("config-inited", _compile_pattern_matchers)
    app.connect("source-read", apply_ipython3_lexer)


redoc = [
    {
        "name": "Ray Jobs API",
        "page": "cluster/running-applications/job-submission/api",
        "spec": "cluster/running-applications/job-submission/openapi.yml",
        "embed": True,
    },
]

redoc_uri = "https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"

autosummary_filename_map = AUTOSUMMARY_FILENAME_MAP

# Mock out external dependencies here.

# Prefer not to mock libraries that are actually installed in the docs build
# environment (doc/requirements-doc.lock.txt). Mocking an installed library
# shadows the real module: an eager import in a documented class body then hits
# the mock and aborts the whole package import as a misleading error. numpy and
# pyarrow are installed, so they are not mocked. tensorflow is also installed (a
# direct requirements-doc entry), but importing it for real breaks the autodoc
# import of ray.rllib.algorithms.algorithm at build time, so it stays mocked.
# The mock list is shared with api_autogen.py and the API/doc consistency check
# (ci/ray_ci/doc) via api_mock_imports.py, so the standalone stub generator and
# the check see the same API surface this render produces. THIRD_PARTY_MOCK
# covers uninstalled third-party libraries; BUILD_ONLY_MOCK covers Ray's
# compiled/generated modules, which are absent only in a source-checkout build.
from api_mock_imports import BUILD_ONLY_MOCK_MODULES, THIRD_PARTY_MOCK_MODULES

autodoc_mock_imports = THIRD_PARTY_MOCK_MODULES + BUILD_ONLY_MOCK_MODULES

for mock_target in autodoc_mock_imports:
    if mock_target in sys.modules:
        logger.info(
            f"Potentially problematic mock target ({mock_target}) found; "
            "autodoc_mock_imports cannot mock modules that have already "
            "been loaded into sys.modules when the sphinx build starts."
        )


# Other sphinx docs can be linked to if the appropriate URL to the docs
# is specified in the `intersphinx_mapping` - for example, types annotations
# that are defined in dependencies can link to their respective documentation.
#
# `_intersphinx_targets` is the source of truth: name -> (base_url, inventory).
# `base_url` is where generated cross-reference links point. `inventory` is the
# upstream objects.inv used to *resolve* those references at build time; None
# means the Sphinx default of <base_url>objects.inv. A few projects pin an
# explicit inventory URL because their hosted objects.inv is unreliable; the
# ray-project/*/releases/.../object-mirror-* URLs are stable mirrors we control.
#
# To avoid fetching two dozen inventories over the network on every build (slow,
# and occasionally flaky via the GitHub release-asset redirects), we commit a
# snapshot of each under doc/source/_intersphinx/ and prefer it. The
# `intersphinx_mapping` built below lists the local snapshot first and the
# upstream location second; Sphinx uses the first that loads, so a present
# snapshot means no network fetch, and a missing one degrades to the old remote
# behavior (an info message, not a build-breaking warning). Refresh snapshots
# with `python doc/source/_intersphinx/refresh.py` (see that directory's README).
#
# Maintenance note: the build log emits "intersphinx inventory has moved: A -> B"
# when A returns a redirect (only when fetching remotely, i.e. on refresh or
# fallback). Only chase it when B is another documentation URL (the project
# relocated). Do NOT copy B when it points at a signed, expiring
# release-assets.githubusercontent.com URL - that's just GitHub's normal redirect
# for a releases/download/ asset, and the github.com/.../releases/download/ URL
# is the stable one to keep.
_intersphinx_targets = {
    "aiohttp": ("https://docs.aiohttp.org/en/stable/", None),
    "composer": ("https://docs.mosaicml.com/en/latest/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
    "datasets": ("https://huggingface.co/docs/datasets/main/en/", None),
    "distributed": ("https://distributed.dask.org/en/stable/", None),
    "grpc": ("https://grpc.github.io/grpc/python/", None),
    "gymnasium": ("https://gymnasium.farama.org/", None),
    "horovod": ("https://horovod.readthedocs.io/en/stable/", None),
    "lightgbm": ("https://lightgbm.readthedocs.io/en/latest/", None),
    "mars": ("https://mars-project.readthedocs.io/en/latest/", None),
    "modin": ("https://modin.readthedocs.io/en/stable/", None),
    "nevergrad": ("https://facebookresearch.github.io/nevergrad/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": (
        "https://pandas.pydata.org/pandas-docs/stable/",
        "https://github.com/ray-project/pandas/releases/download/object-mirror-0.1.0/objects.inv",
    ),
    "pyarrow": ("https://arrow.apache.org/docs", None),
    "pydantic": ("https://pydantic.dev/docs/validation/latest/", None),
    "pymongoarrow": ("https://mongo-arrow.readthedocs.io/en/latest/", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "python": ("https://docs.python.org/3", None),
    "pytorch_lightning": (
        "https://lightning.ai/docs/pytorch/stable/",
        # lightning.ai serves its docs as an SPA and returns HTML for
        # objects.inv (recent readthedocs versions redirect there and do the
        # same, breaking -W builds with "invalid inventory header"); 2.0.9 is
        # the newest readthedocs version still serving a real inventory.
        "https://pytorch-lightning.readthedocs.io/en/2.0.9/objects.inv",
    ),
    "scipy": (
        "https://docs.scipy.org/doc/scipy/",
        "https://github.com/ray-project/scipy/releases/download/object-mirror-0.1.0/objects.inv",
    ),
    "sklearn": ("https://scikit-learn.org/stable/", None),
    "tensorflow": (
        "https://www.tensorflow.org/api_docs/python",
        "https://raw.githubusercontent.com/GPflow/tensorflow-intersphinx/master/tf2_py_objects.inv",
    ),
    "torch": (
        "https://docs.pytorch.org/docs/stable/",
        "https://docs.pytorch.org/docs/2.7/objects.inv",
    ),
    "torchvision": ("https://docs.pytorch.org/vision/stable/", None),
    "transformers": ("https://huggingface.co/docs/transformers/main/en/", None),
}

# Prefer the committed local snapshot, falling back to the upstream inventory
# (or the <base_url>objects.inv default when None) if a snapshot is missing.
intersphinx_mapping = {
    name: (base_url, (f"_intersphinx/{name}.inv", inventory))
    for name, (base_url, inventory) in _intersphinx_targets.items()
}

intersphinx_timeout = 15

# Ray must not be imported in conf.py because third party modules initialized by
# `import ray` will no be mocked out correctly. Perform a check here to ensure
# ray is not imported by future maintainers.
assert (
    "ray" not in sys.modules
), "If ray is already imported, we will not render documentation correctly!"

os.environ["RAY_DOC_BUILD"] = "1"

ipython3_lexer_patterns = [
    *template_collections.IPYTHON3_LEXER_PATTERNS,
    "ray-overview/examples/**/content/**.ipynb",
    "serve/tutorials/**/content/**.ipynb",
    "data/examples/**/content/**.ipynb",
    "tune/examples/**/content/**.ipynb",
]
ipython3_lexer_exclude_patterns = []


def _compile_pattern_matchers(app, config):
    app.ipython3_lexer_patterns = compile_matchers(
        config.ipython3_lexer_patterns or []
    )
    app.ipython3_lexer_exclude_patterns = compile_matchers(
        config.ipython3_lexer_exclude_patterns or []
    )


def apply_ipython3_lexer(app, docname, source):
    """Force the ipython3 pygments lexer on notebooks matching
    ``ipython3_lexer_patterns`` (minus ``ipython3_lexer_exclude_patterns``).

    Sphinx + myst-nb otherwise default to the python3 lexer, which fails on
    ``!shell`` and ``%magic`` cells and is fatal under Readthedocs ``-W``.
    """
    # Sphinx 8 returns a _StrPath from doc2path; coerce to str so the re-based
    # matchers (compile_matchers) and .endswith below accept it.
    doc_source = str(app.env.doc2path(docname, base=False))
    if not doc_source.endswith(".ipynb"):
        return
    if any(m(doc_source) for m in app.ipython3_lexer_exclude_patterns):
        return
    if not any(m(doc_source) for m in app.ipython3_lexer_patterns):
        return

    notebook = json.loads(source[0])
    lang_info = notebook.setdefault("metadata", {}).setdefault("language_info", {})
    if lang_info.get("pygments_lexer") != "ipython3":
        lang_info["pygments_lexer"] = "ipython3"
        source[0] = json.dumps(notebook, ensure_ascii=False)
