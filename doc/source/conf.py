from datetime import datetime
from pathlib import Path
from importlib import import_module
import os
import sys
from jinja2.filters import FILTERS

sys.path.insert(0, os.path.abspath("."))
from custom_directives import (
    DownloadAndPreprocessEcosystemDocs,
    update_context,
    LinkcheckSummarizer,
    build_gallery,
)

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath("../../python/"))

# -- General configuration ------------------------------------------------

# The name of a reST role (builtin or Sphinx extension) to use as the default role, that
# is, for text marked up `like this`. This can be set to 'py:obj' to make `filter` a
# cross-reference to the Python function “filter”. The default is None, which doesn’t
# reassign the default role.

default_role = "py:obj"

sys.path.append(os.path.abspath("./_ext"))

extensions = [
    "callouts",  # custom extension from _ext folder
    "queryparamrefs",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_click.ext",
    "sphinx-jsonschema",
    "sphinxemoji.sphinxemoji",
    "sphinx_copybutton",
    "versionwarning.extension",
    "sphinx_sitemap",
    "myst_nb",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.autosummary",
    "sphinx_external_toc",
    "sphinxcontrib.autodoc_pydantic",
    "sphinxcontrib.redoc",
    "sphinx_tabs.tabs",
    "sphinx_remove_toctrees",
    "sphinx_design",
    "sphinx.ext.intersphinx",
]

# Prune deep toc-trees on demand for smaller html and faster builds.
# This only effects the navigation bar, not the content.
if os.getenv("FAST", False):
    remove_from_toctrees = [
        "data/api/doc/*",
        "ray-air/api/doc/*",
        "ray-core/api/doc/*",
        "ray-observability/api/state/doc/*",
        "serve/api/doc/*",
        "train/api/doc/*",
        "tune/api/doc/*",
        "workflows/api/doc/*",
        "cluster/running-applications/job-submission/doc/*",
        "serve/production-guide/*",
        "serve/tutorials/deployment-graph-patterns/*",
        "rllib/package_ref/env/*",
        "rllib/package_ref/policy/*",
        "rllib/package_ref/evaluation/*",
        "rllib/package_ref/utils/*",
        "workflows/api/*",
        "cluster/kubernetes/user-guides/*",
        "cluster/kubernetes/examples/*",
        "cluster/vms/user-guides/*",
        "cluster/running-applications/job-submission/*",
        "ray-core/actors/*",
        "ray-core/objects/*",
        "ray-core/scheduling/*",
        "ray-core/tasks/*",
        "ray-core/patterns/*",
        "tune/examples/*",
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

# Cache notebook outputs in _build/.jupyter_cache
# To prevent notebook execution, set this to "off". To force re-execution, set this to "force".
# To cache previous runs, set this to "cache".
jupyter_execute_notebooks = os.getenv("RUN_NOTEBOOKS", "off")

external_toc_exclude_missing = False
external_toc_path = "_toc.yml"

html_extra_path = ["robots.txt"]

html_baseurl = "https://docs.ray.io/en/latest"

# This pattern matches:
# - Python Repl prompts (">>> ") and it's continuation ("... ")
# - Bash prompts ("$ ")
# - IPython prompts ("In []: ", "In [999]: ") and it's continuations
#   ("  ...: ", "     : ")
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True

# By default, tabs can be closed by selecting an open tab. We disable this
# functionality with the `sphinx_tabs_disable_tab_closing` option.
sphinx_tabs_disable_tab_closing = True

# Special mocking of packaging.version.Version is required when using sphinx;
# we can't just add this to autodoc_mock_imports, as packaging is imported by
# sphinx even before it can be mocked. Instead, we patch it here.
import packaging

Version = packaging.version.Version


class MockVersion(Version):
    def __init__(self, version: str):
        if isinstance(version, (str, bytes)):
            super().__init__(version)
        else:
            super().__init__("0")


packaging.version.Version = MockVersion

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
# built documents. Retrieve the version using `find_version` rather than importing
# directly (from ray import __version__) because initializing ray will prevent
# mocking of certain external dependencies.
from setup import find_version

release = find_version("ray", "__init__.py")

language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# Also helps resolve warnings about documents not included in any toctree.
exclude_patterns = [
    "templates/*",
    "cluster/running-applications/doc/ray.*",
]

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
    # Returns 429 errors in Linkcheck due to too many requests
    "https://archive.is/2022.12.16-171259/https://www.businessinsider.com/openai-chatgpt-trained-on-anyscale-ray-generative-lifelike-ai-models-2022-12",
    # Returns 406 but remains accessible
    "https://www.uber.com/blog/elastic-xgboost-ray/",
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
    "home_page_in_toc": True,
    "show_navbar_depth": 1,
    "announcement": "<div class='topnav'></div>",
}

# Add any paths that contain custom themes here, relative to this directory.
# html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
html_title = f"Ray {release}"

autodoc_typehints_format = "short"

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

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


def filter_out_undoc_class_members(member_name, class_name, module_name):
    module = import_module(module_name)
    cls = getattr(module, class_name)
    if getattr(cls, member_name).__doc__:
        return f"~{class_name}.{member_name}"
    else:
        return ""


FILTERS["filter_out_undoc_class_members"] = filter_out_undoc_class_members

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
    # NOTE: 'MOCK' is a custom option we introduced to illustrate mock outputs. Since
    # `doctest` doesn't support this flag by default, `sphinx.ext.doctest` raises
    # warnings when we build the documentation.
    import doctest

    doctest.register_optionflag("MOCK")

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
    app.add_js_file("js/csat.js", defer="defer")

    # https://github.com/ines/termynal
    app.add_js_file("js/termynal.js", defer="defer")
    app.add_js_file("js/custom.js", defer="defer")
    app.add_js_file("js/assistant.js", defer="defer")

    app.add_js_file("js/top-navigation.js", defer="defer")

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

    # tag filtering system
    app.add_js_file("js/tags.js")


redoc = [
    {
        "name": "Ray Jobs API",
        "page": "cluster/running-applications/job-submission/api",
        "spec": "cluster/running-applications/job-submission/openapi.yml",
        "embed": True,
    },
]

redoc_uri = "https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"

autosummary_filename_map = {
    "ray.serve.deployment": "ray.serve.deployment_decorator",
    "ray.serve.Deployment": "ray.serve.Deployment",
}

# Mock out external dependencies here.
autodoc_mock_imports = [
    "aiohttp",
    "composer",
    "dask",
    "datasets",
    "fastapi",
    "fsspec",
    "grpc",
    "gymnasium",
    "horovod",
    "huggingface",
    "joblib",
    "lightgbm",
    "lightgbm_ray",
    "nevergrad",
    "numpy",
    "pandas",
    "pyarrow",
    "pytorch_lightning",
    "scipy",
    "setproctitle",
    "skimage",
    "sklearn",
    "skopt",
    "starlette",
    "tensorflow",
    "torch",
    "torchvision",
    "transformers",
    "tree",
    "uvicorn",
    "wandb",
    "watchfiles",
    "xgboost",
    "xgboost_ray",
    # Internal compiled modules
    "ray._raylet",
    "ray.core.generated",
    "ray.serve.generated",
]


for mock_target in autodoc_mock_imports:
    assert mock_target not in sys.modules, (
        f"Problematic mock target ({mock_target}) found; "
        "autodoc_mock_imports cannot mock modules that have already"
        "been loaded into sys.modules when the sphinx build starts."
    )

from sphinx.ext import autodoc


class MockedClassDocumenter(autodoc.ClassDocumenter):
    def add_line(self, line: str, source: str, *lineno: int) -> None:
        if line == "   Bases: :py:class:`object`":
            return
        super().add_line(line, source, *lineno)


autodoc.ClassDocumenter = MockedClassDocumenter

# Other sphinx docs can be linked to if the appropriate URL to the docs
# is specified in the `intersphinx_mapping` - for example, types annotations
# that are defined in dependencies can link to their respective documentation.
intersphinx_mapping = {
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
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
    "pymongoarrow": ("https://mongo-arrow.readthedocs.io/en/latest/", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "python": ("https://docs.python.org/3", None),
    "pytorch_lightning": ("https://lightning.ai/docs/pytorch/stable/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "sklearn": ("https://scikit-learn.org/stable/", None),
    "skopt": ("https://scikit-optimize.github.io/stable/", None),
    "tensorflow": (
        "https://www.tensorflow.org/api_docs/python",
        "https://raw.githubusercontent.com/GPflow/tensorflow-intersphinx/master/tf2_py_objects.inv",
    ),
    "torch": ("https://pytorch.org/docs/stable/", None),
    "torchvision": ("https://pytorch.org/vision/stable/", None),
    "transformers": ("https://huggingface.co/docs/transformers/main/en/", None),
}

# Ray must not be imported in conf.py because third party modules initialized by
# `import ray` will no be mocked out correctly. Perform a check here to ensure
# ray is not imported by future maintainers.
assert (
    "ray" not in sys.modules
), "If ray is already imported, we will not render documentation correctly!"
