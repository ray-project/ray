import logging
import os
import pathlib
import sys
from datetime import datetime
from dataclasses import is_dataclass
from importlib import import_module
from typing import Any, Dict

import sphinx
from docutils import nodes
from jinja2.filters import FILTERS
from sphinx.ext import autodoc
from sphinx.ext.autosummary import generate
from sphinx.util.inspect import safe_getattr

DEFAULT_API_GROUP = "Others"

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
]

# Configuration for algolia
# Note: This API key grants read access to our indexes and is intended to be public.
# See https://www.algolia.com/doc/guides/security/api-keys/ for more information.
docsearch_app_id = "LBHF0PABBL"
docsearch_api_key = "6c42f30d9669d8e42f6fc92f44028596"
docsearch_index_name = "docs-ray"

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
]

myst_heading_anchors = 3

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

html_baseurl = "https://docs.ray.io/en/latest"

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

# Special mocking of packaging.version.Version is required when using sphinx;
# we can't just add this to autodoc_mock_imports, as packaging is imported by
# sphinx even before it can be mocked. Instead, we patch it here.
import packaging.version as packaging_version  # noqa

Version = packaging_version.Version


class MockVersion(Version):
    def __init__(self, version: str):
        if isinstance(version, (str, bytes)):
            super().__init__(version)
        else:
            super().__init__("0")


packaging_version.Version = MockVersion

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
autogen_files = [
    "data/api/_autogen.rst",
]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# Also helps resolve warnings about documents not included in any toctree.
exclude_patterns = [
    "templates/*",
    "cluster/running-applications/doc/ray.*",
    "data/api/ray.data.*.rst",
    "ray-overview/examples/**/README.md",  # Exclude .md files in examples subfolders
    "train/examples/**/README.md",
    "serve/tutorials/deployment-serve-llm/README.*",
    "serve/tutorials/deployment-serve-llm/*/notebook.ipynb",
    "ray-overview/examples/llamafactory-llm-fine-tune/README.ipynb",
    "ray-overview/examples/llamafactory-llm-fine-tune/**/*.ipynb",
] + autogen_files

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
    "announcement": """Join us at Ray Summit 2025 — <a target="_blank" href="https://www.anyscale.com/ray-summit/2025?utm_source=ray_docs&utm_medium=docs&utm_campaign=banner">Register early and save.</a><button type="button" id="close-banner" aria-label="Close banner">&times;</button>""",
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
    "pygment_light_style": "stata-dark",
    "pygment_dark_style": "stata-dark",
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

html_sidebars = {
    "**": [
        (
            "main-sidebar-readthedocs"
            if os.getenv("READTHEDOCS") == "True"
            else "main-sidebar"
        )
    ],
    "ray-overview/examples": [],
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

# Better typehint formatting (see custom.css)
autodoc_typehints = "signature"


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


FILTERS["filter_out_undoc_class_members"] = filter_out_undoc_class_members
FILTERS["get_api_groups"] = get_api_groups
FILTERS["select_api_group"] = select_api_group
FILTERS["has_public_constructor"] = has_public_constructor


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
    """
    generate.generate_autosummary_docs(
        [os.path.join(app.srcdir, file) for file in autogen_files],
        app=app,
    )


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

    app.add_js_file("js/assistant.js", defer="defer")
    app.add_css_file("css/assistant.css")

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
            if (
                "duplicate object description of ray.actor.ActorMethod.bind"
                in record.getMessage()
            ):
                return False  # Don't log this specific warning
            return True  # Log all other warnings

    logging.getLogger("sphinx").addFilter(DuplicateObjectFilter())


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
    "async_timeout",
    "backoff",
    "cachetools",
    "composer",
    "cupy",
    "dask",
    "datasets",
    "fastapi",
    "filelock",
    "fsspec",
    "google",
    "grpc",
    "gymnasium",
    "horovod",
    "huggingface",
    "httpx",
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
    "starlette",
    "tensorflow",
    "torch",
    "torchvision",
    "transformers",
    "tree",
    "typer",
    "uvicorn",
    "wandb",
    "watchfiles",
    "openai",
    "xgboost",
    "xgboost_ray",
    "psutil",
    "colorama",
    "grpc",
    "vllm",
    # Internal compiled modules
    "ray._raylet",
    "ray.core.generated",
    "ray.serve.generated",
]

for mock_target in autodoc_mock_imports:
    if mock_target in sys.modules:
        logger.info(
            f"Potentially problematic mock target ({mock_target}) found; "
            "autodoc_mock_imports cannot mock modules that have already "
            "been loaded into sys.modules when the sphinx build starts."
        )


class MockedClassDocumenter(autodoc.ClassDocumenter):
    """Remove note about base class when a class is derived from object."""

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
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
    "pymongoarrow": ("https://mongo-arrow.readthedocs.io/en/latest/", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "python": ("https://docs.python.org/3", None),
    "pytorch_lightning": ("https://lightning.ai/docs/pytorch/stable/", None),
    "scipy": (
        "https://docs.scipy.org/doc/scipy/",
        "https://github.com/ray-project/scipy/releases/download/object-mirror-0.1.0/objects.inv",
    ),
    "sklearn": ("https://scikit-learn.org/stable/", None),
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

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

os.environ["RAY_DOC_BUILD"] = "1"
