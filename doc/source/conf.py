# -*- coding: utf-8 -*-
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.abspath("."))
from custom_directives import *
from datetime import datetime


# Mocking modules allows Sphinx to work without installing Ray.
mock_modules()

assert (
    "ray" not in sys.modules
), "If ray is already imported, we will not render documentation correctly!"

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath("../../python/"))

import ray

# -- General configuration ------------------------------------------------

# The name of a reST role (builtin or Sphinx extension) to use as the default role, that
# is, for text marked up `like this`. This can be set to 'py:obj' to make `filter` a
# cross-reference to the Python function “filter”. The default is None, which doesn’t
# reassign the default role.

default_role = "py:obj"

extensions = [
    "sphinx_panels",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_click.ext",
    "sphinx-jsonschema",
    "sphinxemoji.sphinxemoji",
    "sphinx_copybutton",
    "sphinxcontrib.yt",
    "versionwarning.extension",
    "sphinx_sitemap",
    "myst_nb",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.autosummary",
    "sphinx_external_toc",
    "sphinx_thebe",
    "sphinxcontrib.autodoc_pydantic",
    "sphinxcontrib.redoc",
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
]

# Thebe configuration for launching notebook cells within the docs.
thebe_config = {
    "selector": "div.highlight",
    "repository_url": "https://github.com/ray-project/ray",
    "repository_branch": "master",
}

# Cache notebook outputs in _build/.jupyter_cache
# To prevent notebook execution, set this to "off". To force re-execution, set this to "force".
# To cache previous runs, set this to "cache".
jupyter_execute_notebooks = os.getenv("RUN_NOTEBOOKS", "off")

external_toc_exclude_missing = False
external_toc_path = "_toc.yml"

html_extra_path = ["robots.txt"]

# Omit prompt when using copy button
copybutton_prompt_text = r"\$ "
copybutton_prompt_is_regexp = True


# There's a flaky autodoc import for "TensorFlowVariables" that fails depending on the doc structure / order
# of imports.
# autodoc_mock_imports = ["ray.experimental.tf_utils"]

# This is used to suppress warnings about explicit "toctree" directives.
suppress_warnings = ["etoc.toctree"]

versionwarning_admonition_type = "note"
versionwarning_banner_title = "Join the Ray Discuss Forums!"

FORUM_LINK = "https://discuss.ray.io"
versionwarning_messages = {
    # Re-enable this after Ray Summit.
    # "latest": (
    #     "This document is for the latest pip release. "
    #     'Visit the <a href="/en/master/">master branch documentation here</a>.'
    # ),
    # "master": (
    #     "<b>Got questions?</b> Join "
    #     f'<a href="{FORUM_LINK}">the Ray Community forum</a> '
    #     "for Q&A on all things Ray, as well as to share and learn use cases "
    #     "and best practices with the Ray community."
    # ),
}

versionwarning_body_selector = "#main-content"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Ray"
copyright = str(datetime.now().year) + ", The Ray Team"
author = "The Ray Team"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
from ray import __version__ as version

# The full version, including alpha/beta/rc tags.
release = version

language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build"]

# If "DOC_LIB" is found, only build that top-level navigation item.
build_one_lib = os.getenv("DOC_LIB")

all_toc_libs = [f.path for f in os.scandir(".") if f.is_dir() and "ray-" in f.path]
all_toc_libs += [
    "cluster",
    "tune",
    "data",
    "train",
    "rllib",
    "serve",
    "workflows",
]
if build_one_lib and build_one_lib in all_toc_libs:
    all_toc_libs.remove(build_one_lib)
    exclude_patterns += all_toc_libs


# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "lovelace"


# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

# Do not check anchors for links because it produces many false positives
# and is slow (it needs to download the linked website).
linkcheck_anchors = False

# Only check external links, i.e. the ones starting with http:// or https://.
linkcheck_ignore = [
    r"^((?!http).)*$",  # exclude links not starting with http
    "http://ala2017.it.nuigalway.ie/papers/ALA2017_Gupta.pdf",  # broken
    "https://mvnrepository.com/artifact/*",  # working but somehow not with linkcheck
    # This should be fixed -- is temporal the successor of cadence? Do the examples need to be updated?
    "https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-cadence.md",
    # TODO(richardliaw): The following probably needs to be fixed in the tune_sklearn package
    "https://scikit-optimize.github.io/stable/modules/",
    "https://www.oracle.com/java/technologies/javase-jdk15-downloads.html",  # forbidden for client
    "https://speakerdeck.com/*",  # forbidden for bots
    r"https://huggingface.co/*",  # seems to be flaky
    r"https://www.meetup.com/*",  # seems to be flaky
    r"https://www.pettingzoo.ml/*",  # seems to be flaky
    r"http://localhost[:/].*",  # Ignore localhost links
    r"^http:/$",  # Ignore incomplete links
]

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_book_theme"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "repository_url": "https://github.com/ray-project/ray",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
    "path_to_docs": "doc/source",
    "home_page_in_toc": False,
    "show_navbar_depth": 0,
    "launch_buttons": {
        "notebook_interface": "jupyterlab",
        "binderhub_url": "https://mybinder.org",
        "colab_url": "https://colab.research.google.com",
    },
}

# Add any paths that contain custom themes here, relative to this directory.
# html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
html_title = f"Ray {release}"

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = "images/ray_logo.png"

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

# Better typehint formatting (see custom.css)
autodoc_typehints = "signature"


# Add a render priority for doctest
nb_render_priority = {
    "doctest": (),
    "html": (
        "application/vnd.jupyter.widget-view+json",
        "application/javascript",
        "text/html",
        "image/svg+xml",
        "image/png",
        "image/jpeg",
        "text/markdown",
        "text/latex",
        "text/plain",
    ),
}


def setup(app):
    app.connect("html-page-context", update_context)

    # Custom CSS
    app.add_css_file("css/custom.css", priority=800)
    app.add_css_file(
        "https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.css"
    )
    # https://github.com/ines/termynal
    app.add_css_file("css/termynal.css")

    # Custom JS
    app.add_js_file(
        "https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js",
        defer="defer",
    )
    app.add_js_file("js/docsearch.js", defer="defer")

    # https://github.com/medmunds/rate-the-docs for allowing users
    # to give thumbs up / down and feedback on existing docs pages.
    app.add_js_file("js/rate-the-docs.es.min.js")

    # https://github.com/ines/termynal
    app.add_js_file("js/termynal.js", defer="defer")
    app.add_js_file("js/custom.js", defer="defer")

    app.add_js_file("js/try-anyscale.js", defer="defer")

    base_path = Path(__file__).parent
    github_docs = DownloadAndPreprocessEcosystemDocs(base_path)
    # Download docs from ecosystem library repos
    app.connect("builder-inited", github_docs.write_new_docs)
    # Restore original file content after build
    app.connect("build-finished", github_docs.write_original_docs)

    # Hook into the logger used by linkcheck to display a summary at the end.
    linkcheck_summarizer = LinkcheckSummarizer()
    app.connect("builder-inited", linkcheck_summarizer.add_handler_to_linkcheck)
    app.connect("build-finished", linkcheck_summarizer.summarize)

    # Create galleries on the fly
    app.connect("builder-inited", build_gallery)


redoc = [
    {
        "name": "Ray Jobs API",
        "page": "cluster/running-applications/job-submission/api",
        "spec": "cluster/running-applications/job-submission/openapi.yml",
        "embed": True,
    },
]

redoc_uri = "https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"
