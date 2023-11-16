import re
import sphinx
from typing import List, Dict, Union, Callable, Any
import copy
import yaml
import bs4
import logging
import logging.handlers
import pathlib
import urllib
import urllib.request
from queue import Queue
import requests
from preprocess_github_markdown import preprocess_github_markdown_file
from sphinx.util import logging as sphinx_logging
from sphinx.util.console import red  # type: ignore
from sphinx.util.nodes import make_refnode
from docutils import nodes
from functools import lru_cache
from pydata_sphinx_theme.toctree import add_collapse_checkboxes

logger = logging.getLogger(__name__)

__all__ = [
    "DownloadAndPreprocessEcosystemDocs",
    "update_context",
    "LinkcheckSummarizer",
    "setup_context",
]

# Taken from https://github.com/edx/edx-documentation
FEEDBACK_FORM_FMT = (
    "https://github.com/ray-project/ray/issues/new?"
    "title={title}&labels=docs&body={body}"
)


def feedback_form_url(project, page):
    """Create a URL for feedback on a particular page in a project."""
    return FEEDBACK_FORM_FMT.format(
        title=urllib.parse.quote("[docs] Issue on `{page}.rst`".format(page=page)),
        body=urllib.parse.quote(
            "# Documentation Problem/Question/Comment\n"
            "<!-- Describe your issue/question/comment below. -->\n"
            "<!-- If there are typos or errors in the docs, feel free "
            "to create a pull-request. -->\n"
            "\n\n\n\n"
            "(Created directly from the docs)\n"
        ),
    )


def update_context(app, pagename, templatename, context, doctree):
    """Update the page rendering context to include ``feedback_form_url``."""
    context["feedback_form_url"] = feedback_form_url(app.config.project, pagename)


# Add doc files from external repositories to be downloaded during build here
# (repo, ref, path to get, path to save on disk)
EXTERNAL_MARKDOWN_FILES = []


class DownloadAndPreprocessEcosystemDocs:
    """
    This class downloads markdown readme files for various
    ecosystem libraries, saves them in specified locations and preprocesses
    them before sphinx build starts.

    If you have ecosystem libraries that live in a separate repo from Ray,
    adding them here will allow for their docs to be present in Ray docs
    without the need for duplicate files. For more details, see ``doc/README.md``.
    """

    def __init__(self, base_path: str) -> None:
        self.base_path = pathlib.Path(base_path).absolute()
        assert self.base_path.is_dir()
        self.original_docs = {}

    @staticmethod
    def get_latest_release_tag(repo: str) -> str:
        """repo is just the repo name, eg. ray-project/ray"""
        response = requests.get(f"https://api.github.com/repos/{repo}/releases/latest")
        return response.json()["tag_name"]

    @staticmethod
    def get_file_from_github(
        repo: str, ref: str, path_to_get_from_repo: str, path_to_save_on_disk: str
    ) -> None:
        """If ``ref == "latest"``, use latest release"""
        if ref == "latest":
            ref = DownloadAndPreprocessEcosystemDocs.get_latest_release_tag(repo)
        urllib.request.urlretrieve(
            f"https://raw.githubusercontent.com/{repo}/{ref}/{path_to_get_from_repo}",
            path_to_save_on_disk,
        )

    def save_original_doc(self, path: str):
        with open(path, "r") as f:
            self.original_docs[path] = f.read()

    def write_new_docs(self, *args, **kwargs):
        for (
            repo,
            ref,
            path_to_get_from_repo,
            path_to_save_on_disk,
        ) in EXTERNAL_MARKDOWN_FILES:
            path_to_save_on_disk = self.base_path.joinpath(path_to_save_on_disk)
            self.save_original_doc(path_to_save_on_disk)
            self.get_file_from_github(
                repo, ref, path_to_get_from_repo, path_to_save_on_disk
            )
            preprocess_github_markdown_file(path_to_save_on_disk)

    def write_original_docs(self, *args, **kwargs):
        for path, content in self.original_docs.items():
            with open(path, "w") as f:
                f.write(content)

    def __call__(self):
        self.write_new_docs()


class _BrokenLinksQueue(Queue):
    """Queue that discards messages about non-broken links."""

    def __init__(self, maxsize: int = 0) -> None:
        self._last_line_no = None
        self.used = False
        super().__init__(maxsize)

    def put(self, item: logging.LogRecord, block=True, timeout=None):
        self.used = True
        message = item.getMessage()
        # line nos are separate records
        if ": line" in message:
            self._last_line_no = item
        # same formatting as in sphinx.builders.linkcheck
        # to avoid false positives if "broken" is in url
        if red("broken    ") in message or "broken link:" in message:
            if self._last_line_no:
                super().put(self._last_line_no, block=block, timeout=timeout)
                self._last_line_no = None
            return super().put(item, block=block, timeout=timeout)


class _QueueHandler(logging.handlers.QueueHandler):
    """QueueHandler without modifying the record."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


class LinkcheckSummarizer:
    """Hook into the logger used by linkcheck to display a summary at the end."""

    def __init__(self) -> None:
        self.logger = None
        self.queue_handler = None
        self.log_queue = _BrokenLinksQueue()

    def add_handler_to_linkcheck(self, *args, **kwargs):
        """Adds a handler to the linkcheck logger."""
        self.logger = sphinx_logging.getLogger("sphinx.builders.linkcheck")
        self.queue_handler = _QueueHandler(self.log_queue)
        if not self.logger.hasHandlers():
            # If there are no handlers, add the one that would
            # be used anyway.
            self.logger.logger.addHandler(logging.lastResort)
        self.logger.logger.addHandler(self.queue_handler)

    def summarize(self, *args, **kwargs):
        """Summarizes broken links."""
        if not self.log_queue.used:
            return

        self.logger.logger.removeHandler(self.queue_handler)

        self.logger.info("\nBROKEN LINKS SUMMARY:\n")
        has_broken_links = False
        while self.log_queue.qsize() > 0:
            has_broken_links = True
            record: logging.LogRecord = self.log_queue.get()
            self.logger.handle(record)

        if not has_broken_links:
            self.logger.info("No broken links found!")


def parse_navbar_config(app: sphinx.application.Sphinx, config: sphinx.config.Config):
    """Parse the navbar config file into a set of links to show in the navbar.

    Parameters
    ----------
    app : sphinx.application.Sphinx
        Application instance passed when the `config-inited` event is emitted
    config : sphinx.config.Config
        Initialized configuration to be modified
    """
    if "navbar_content_path" in config:
        filename = app.config["navbar_content_path"]
    else:
        filename = ""

    if filename:
        with open(pathlib.Path(__file__).parent / filename, "r") as f:
            config.navbar_content = yaml.safe_load(f)
    else:
        config.navbar_content = None


NavEntry = Dict[str, Union[str, List["NavEntry"]]]


def preload_sidebar_nav(get_toctree: Callable[[Any], str]) -> bs4.BeautifulSoup:
    """Return the navigation link structure in HTML.

    This function is modified from the pydata_sphinx_theme function
    `generate_toctree_html`. However, for ray we only produce one
    sidebar for all pages. We therefore can call this function just once (per worker),
    cache the result, and reuse it.

    The result of this function is equivalent to calling

        pydata_sphinx_theme.toctree.generate_toctree_html(
            "sidebar",
            startdepth=0,
            show_nav_level=0,
            collapse=False,
            includehidden=True,
            maxdepth=4,
            titles_only=True
        )

    Here we cache the result on this function itself because the `get_toctree`
    function is not the same instance for each `html-page-context` event, even
    if the toctree content is identical.

    Parameters
    ----------
    get_toctree : Callable[[Any], str]
        The function defined in context["toctree"] when html-page-context is triggered

    Returns
    -------
    bs4.BeautifulSoup
        Soup to display in the side navbar
    """
    if hasattr(preload_sidebar_nav, "cached_toctree"):
        return preload_sidebar_nav.cached_toctree

    toctree = get_toctree(
        collapse=False, includehidden=True, maxdepth=4, titles_only=True
    )

    soup = bs4.BeautifulSoup(toctree, "html.parser")

    # All the URIs are relative to the current document location, e.g.
    # "../../<whatever.html>". There's no need for this, and it messes up the build if
    # we reuse the same sidebar (with different relative URIs for various pages) on
    # different pages. Replace the leading "../" sequences in the hrefs
    for a in soup.select("a"):
        a["href"] = re.sub(r"^(\.\.\/)*", "/", a["href"])

    # pair "current" with "active" since that's what we use w/ bootstrap
    for li in soup("li", {"class": "current"}):
        li["class"].append("active")

    # Remove sidebar links to sub-headers on the page
    for li in soup.select("li"):
        # Remove
        if li.find("a"):
            href = li.find("a")["href"]
            if "#" in href and href != "#":
                li.decompose()

    # Add bootstrap classes for first `ul` items
    for ul in soup("ul", recursive=False):
        ul.attrs["class"] = ul.attrs.get("class", []) + ["nav", "bd-sidenav"]

    # Add collapse boxes for parts/captions.
    # Wraps the TOC part in an extra <ul> to behave like chapters with toggles
    # show_nav_level: 0 means make parts collapsible.
    partcaptions = soup.find_all("p", attrs={"class": "caption"})
    if len(partcaptions):
        new_soup = bs4.BeautifulSoup("<ul class='list-caption'></ul>", "html.parser")
        for caption in partcaptions:
            # Assume that the next <ul> element is the TOC list
            # for this part
            for sibling in caption.next_siblings:
                if sibling.name == "ul":
                    toclist = sibling
                    break
            li = soup.new_tag("li", attrs={"class": "toctree-l0"})
            li.extend([caption, toclist])
            new_soup.ul.append(li)
        soup = new_soup

    # Add icons and labels for collapsible nested sections
    add_collapse_checkboxes(soup)

    preload_sidebar_nav.cached_toctree = soup
    return soup


def setup_context(app, pagename, templatename, context, doctree):
    @lru_cache(maxsize=None)
    def render_header_nav_links() -> bs4.BeautifulSoup:
        """Render external header links into the top nav bar.
        The structure rendered here is defined in an external yaml file.

        Returns
        -------
        str
            Raw HTML to be rendered in the top nav bar
        """
        if not hasattr(app.config, "navbar_content"):
            raise ValueError(
                "A template is attempting to call render_header_nav_links(); a "
                "navbar configuration must be specified."
            )

        node = nodes.container(classes=["navbar-content"])
        node.append(render_header_nodes(app.config.navbar_content))
        header_soup = bs4.BeautifulSoup(
            app.builder.render_partial(node)["fragment"], "html.parser"
        )
        return add_nav_chevrons(header_soup)

    def render_header_nodes(
        obj: List[NavEntry], is_top_level: bool = True
    ) -> nodes.Node:
        """Generate a set of header nav links with docutils nodes.

        Parameters
        ----------
        is_top_level : bool
            True if the call to this function is rendering the top level nodes,
            False otherwise (non-top level nodes are displayed as submenus of the top
            level nodes)
        obj : List[NavEntry]
            List of yaml config entries to render as docutils nodes

        Returns
        -------
        nodes.Node
            Bullet list which will be turned into header nav HTML by the sphinx builder
        """
        bullet_list = nodes.bullet_list(
            bullet="-",
            classes=["navbar-toplevel" if is_top_level else "navbar-sublevel"],
        )

        for item in obj:

            if "file" in item:
                ref_node = make_refnode(
                    app.builder,
                    context["current_page_name"],
                    item["file"],
                    None,
                    nodes.inline(classes=["navbar-link-title"], text=item.get("title")),
                    item.get("title"),
                )
            elif "link" in item:
                ref_node = nodes.reference("", "", internal=False)
                ref_node["refuri"] = item.get("link")
                ref_node["reftitle"] = item.get("title")
                ref_node.append(
                    nodes.inline(classes=["navbar-link-title"], text=item.get("title"))
                )

            if "caption" in item:
                caption = nodes.Text(item.get("caption"))
                ref_node.append(caption)

            paragraph = nodes.paragraph()
            paragraph.append(ref_node)

            container = nodes.container(classes=["ref-container"])
            container.append(paragraph)

            list_item = nodes.list_item(
                classes=["active-link"] if item.get("file") == pagename else []
            )
            list_item.append(container)

            if "sections" in item:
                wrapper = nodes.container(classes=["navbar-dropdown"])
                wrapper.append(
                    render_header_nodes(item["sections"], is_top_level=False)
                )
                list_item.append(wrapper)

            bullet_list.append(list_item)

        return bullet_list

    context["cached_toctree"] = preload_sidebar_nav(context["toctree"])
    context["render_header_nav_links"] = render_header_nav_links


def add_nav_chevrons(input_soup: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
    """Add dropdown chevron icons to the header nav bar.

    Parameters
    ----------
    input_soup : bs4.BeautifulSoup
        Soup containing rendered HTML which will be inserted into the header nav bar

    Returns
    -------
    bs4.BeautifulSoup
        A new BeautifulSoup instance containing chevrons on the list items that
        are meant to be dropdowns.
    """
    soup = copy.copy(input_soup)

    for li in soup.find_all("li", recursive=True):
        divs = li.find_all("div", {"class": "navbar-dropdown"}, recursive=False)
        if divs:
            ref = li.find("div", {"class": "ref-container"})
            ref.append(soup.new_tag("i", attrs={"class": "fa-solid fa-chevron-down"}))

    return soup
