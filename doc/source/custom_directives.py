from collections.abc import Iterable
from enum import Enum
import re
import sphinx
from typing import List, Dict, Union, Callable, Any, Optional, Tuple
import copy
import yaml
import bs4
import logging
import logging.handlers
import pathlib
import random
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

from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter


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

EXAMPLE_GALLERY_CONFIGS = [
    "source/data/examples.yml",
    "source/serve/examples.yml",
    "source/train/examples.yml",
]


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


def preload_sidebar_nav(
    get_toctree: Callable[[Any], str],
    pathto: Callable[[str], str],
    root_doc: str,
    pagename: str,
) -> bs4.BeautifulSoup:
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
        # Need to retrieve a copy of the cached toctree HTML so as not to modify
        # the cached version when we set the "checked" state of the inputs
        soup = copy.copy(preload_sidebar_nav.cached_toctree)
    else:
        toctree = get_toctree(
            collapse=False, includehidden=True, maxdepth=4, titles_only=True
        )

        soup = bs4.BeautifulSoup(toctree, "html.parser")

        # Remove "current" class since this toctree HTML is being reused
        # from some other page
        for li in soup("li", {"class": "current"}):
            li["class"].remove("current")

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
            new_soup = bs4.BeautifulSoup(
                "<ul class='list-caption'></ul>", "html.parser"
            )
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

        # Cache a fresh copy of the toctree HTML
        preload_sidebar_nav.cached_toctree = copy.copy(soup)

    # All the URIs are relative to the current document location, e.g.
    # "../../<whatever.html>". There's no need for this, and it messes up the build if
    # we reuse the same sidebar (with different relative URIs for various pages) on
    # different pages. Replace the leading "../" sequences in the hrefs with the correct
    # number of "../" for the given rootdoc
    if pagename == root_doc:
        to_root_prefix = "./"
    else:
        to_root_prefix = re.sub(f"{root_doc}.html", "", pathto(root_doc))

    for a in soup.select("a"):
        absolute_href = re.sub(r"^(\.\.\/)*", "", a["href"])
        a["href"] = to_root_prefix + absolute_href

        if absolute_href == f"{pagename}.html":

            # Add a current-page class to the parent li element for styling
            parent_li = a.find_parent("li")
            parent_li["class"] = parent_li.get("class", []) + ["current-page"]

            # Open the dropdowns of every parent li for the active page
            for parent_li in a.find_parents("li", attrs={"class": "has-children"}):
                el = parent_li.find("input")
                if el:
                    el.attrs["checked"] = True

    return soup


class ExampleEnum(Enum):
    """Enum which allows easier enumeration of members for example metadata."""

    @classmethod
    def items(cls: type) -> Iterable[Tuple["ExampleEnum", str]]:
        """Return an iterable mapping between the enum type and the corresponding value.

        Returns
        -------
        Dict['ExampleEnum', str]
            Dictionary of enum type, enum value for the enum class
        """
        yield from {entry: entry.value for entry in cls}.items()

    @classmethod
    def values(cls: type) -> List[str]:
        """Return a list of the values of the enum.

        Returns
        -------
        List[str]
            List of values for the enum class
        """
        return [entry.value for entry in cls]

    @classmethod
    def _missing_(cls: type, value: str) -> "ExampleEnum":
        """Allow case-insensitive lookup of enum values.

        This shouldn't be called directly. Instead this is called when e.g.,
        "SkillLevel('beginner')" is called.

        Parameters
        ----------
        value : str
            Value for which the associated enum class instance is to be returned.
            Spaces are stripped from the beginning and end of the string, and
            matching is case-insensitive.

        Returns
        -------
        ExampleEnum
            Enum class instance for the requested value
        """
        value = value.lstrip().rstrip().lower()
        for member in cls:
            if member.value.lstrip().rstrip().lower() == value:
                return member
        return None

    @classmethod
    def formatted_name(cls: type) -> str:
        """Return the formatted name for the class."""
        raise NotImplementedError


class Contributor(ExampleEnum):
    RAY_TEAM = "Maintained by the Ray Team"
    COMMUNITY = "Contributed by the Ray Community"

    @property
    def tag(self):
        if self == Contributor.RAY_TEAM:
            return "ray-team"
        return "community"

    @classmethod
    def formatted_name(cls):
        return "All Examples"


class UseCase(ExampleEnum):
    """Use case type for example metadata."""

    LARGE_LANGUAGE_MODELS = "Large Language Models"
    GENERATIVE_AI = "Generative AI"
    COMPUTER_VISION = "Computer Vision"
    NATURAL_LANGUAGE_PROCESSING = "Natural Language Processing"

    @classmethod
    def formatted_name(cls):
        return "Use Case"


class SkillLevel(ExampleEnum):
    """Skill level type for example metadata."""

    BEGINNER = "Beginner"
    INTERMEDIATE = "Intermediate"
    ADVANCED = "Advanced"

    @classmethod
    def formatted_name(cls):
        return "Skill Level"


class Framework(ExampleEnum):
    """Framework type for example metadata."""

    PYTORCH = "PyTorch"
    LIGHTNING = "Lightning"
    TRANSFORMERS = "Transformers"
    ACCELERATE = "Accelerate"
    DEEPSPEED = "DeepSpeed"
    TENSORFLOW = "TensorFlow"
    HOROVOD = "Horovod"
    XGBOOST = "XGBoost"
    HUGGINGFACE = "Hugging Face"
    ANY = "Any"

    @classmethod
    def formatted_name(cls):
        return "Framework"


class Library(ExampleEnum):
    """Library type for example metadata."""

    DATA = "Data"
    SERVE = "Serve"
    TRAIN = "Train"

    @classmethod
    def formatted_name(cls):
        return "Library"

    @classmethod
    def from_path(cls, path: Union[pathlib.Path, str]) -> "Library":
        """Instantiate a Library instance from a path.

        This function works its way up the file tree until it finds a directory that
        matches one of the enum types; if none is found, raise an exception.

        Parameters
        ----------
        path : Union[pathlib.Path, str]
            Path containing a directory named the same as one of the enum types.

        Returns
        -------
        Library
            A new Library instance for the given path.
        """
        if isinstance(path, str):
            path = pathlib.Path(path)

        for part in path.parts:
            try:
                return Library(part)
            except ValueError:
                continue

        raise ValueError(f"Cannot find library name from example config path {path}")


class Example:
    """Class containing metadata about an example to be shown in the exmaple gallery."""

    def __init__(
        self, config: Dict[str, str], library: Library, config_dir: pathlib.Path
    ):
        self.config_dir = config_dir
        self.library = library

        self.frameworks = []
        for framework in config.get("frameworks", []):
            self.frameworks.append(Framework(framework.strip()))

        self.use_cases = []
        for use_case in config.get("use_cases", []):
            self.use_cases.append(UseCase(use_case.strip()))

        contributor = config.get("contributor", "").strip()
        if contributor:
            if contributor == "community":
                self.contributor = Contributor.COMMUNITY
            elif contributor == "ray team":
                self.contributor = Contributor.RAY_TEAM
            else:
                raise ValueError(
                    f"Invalid contributor type specified: {contributor}. Must be "
                    " either 'ray team' or 'community'."
                )
        else:
            self.contributor = Contributor.RAY_TEAM

        self.skill_level = SkillLevel(config.get("skill_level"))
        self.title = config.get("title")

        if "link" not in config:
            raise ValueError(
                "All examples must provide a link to either an external resource "
                "(starting with http://...) or a relative path to an internal page "
                "that is part of the Ray docs."
            )

        link = config["link"]
        if link.startswith("http"):
            self.link = link
        else:
            self.link = str(config_dir / link)

        if self.title is None:
            raise ValueError(f"Title of an example {config} must be set.")

        if self.link is None:
            raise ValueError(
                f"An internal or external link must be specified: {config}"
            )


class ExampleConfig:
    """Object which holds configuration data for a set of library examples."""

    def __init__(
        self, path: Union[pathlib.Path, str], src_dir: Union[pathlib.Path, str]
    ):
        """Parse a config file containing examples to display in the example gallery.

        Parameters
        ----------
        path : Union[pathlib.Path, str]
            Path to the `examples.yml` to be parsed
        root_dir : Union[pathlib.Path, str]
            Path to the root of the docs directory; `app.srcdir`
        """
        if isinstance(path, str):
            path = pathlib.Path(path)

        if not path.exists():
            raise ValueError(f"No configuration file found at {path}.")

        with open(path, "r") as f:
            config = yaml.safe_load(f)

        self.config_path = path
        self.library = Library.from_path(path)
        self.examples = self.parse_examples(config.get("examples", []), src_dir)
        self.text = config.get("text", "")
        self.columns_to_show = self.parse_columns_to_show(
            config.get("columns_to_show", [])
        )

    def parse_columns_to_show(self, columns: str) -> Dict[str, type]:
        """Parse the columns to show in the library example page for the config.

        Note that a link to the example is always shown, and cannot be hidden.

        Parameters
        ----------
        columns : str
            Column names to show; valid names are any subclass of ExampleEnum, e.g.
            UseCase, Framework, etc.

        Returns
        -------
        List[type]
            A list of the ExampleEnum classes for which columns are to be shown
        """
        cols = {}
        for col in columns:
            if col == "use_cases":
                cols["use_cases"] = UseCase
            elif col == "frameworks":
                cols["frameworks"] = Framework
            else:
                raise ValueError(
                    f"Invalid column name {col} specified in {self.config_path}"
                )
        return cols

    def parse_examples(
        self, example_config: List[Dict[str, str]], src_dir: Union[pathlib.Path, str]
    ) -> List[Example]:
        """Parse the examples in the given configuration.

        Raise an exception if duplicate examples are found in the configuration file.

        Parameters
        ----------
        path : pathlib.Path
            Path to the example file to parse

        Returns
        -------
        List[Example]
            List of examples from the parsed configuration file
        """
        links = set()
        examples = []
        for entry in example_config:
            example = Example(
                entry, self.library, self.config_path.relative_to(src_dir).parent
            )
            if example.link in links:
                raise ValueError(
                    f"A duplicate example {example.link} was specified in "
                    f"{self.config_path}. Please remove duplicates and rebuild."
                )
            links.add(example.link)
            examples.append(example)

        return examples

    def __iter__(self):
        yield from self.examples


def setup_context(app, pagename, templatename, context, doctree):
    def render_library_examples(config: pathlib.Path = None) -> bs4.BeautifulSoup:
        """Render a table of links to examples for a given Ray library.

        Duplicate examples will result in an error.

        Parameters
        ----------
        config : pathlib.Path
            Path to the examples.yml file for the Ray library

        Returns
        -------
        bs4.BeautifulSoup
            Table of links to examples for the library, rendered as HTML
        """
        if config is None:
            config = (pathlib.Path(app.confdir) / pagename).parent / "examples.yml"

        # Separate the examples into different skill levels
        examples = {
            SkillLevel.BEGINNER: [],
            SkillLevel.INTERMEDIATE: [],
            SkillLevel.ADVANCED: [],
        }
        # Keep track of whether any of the examples have frameworks metadata; the
        # column will not be shown if no frameworks metadata exists on any example.
        example_config = ExampleConfig(config, app.srcdir)
        for example in example_config:
            examples[example.skill_level].append(example)

        # Construct a table of examples
        soup = bs4.BeautifulSoup()

        # Add the main heading to the page and include the page text
        page_title = soup.new_tag("h1")
        page_title.append(f"{example_config.library.value} Examples")
        soup.append(page_title)

        page_text = soup.new_tag("p")
        page_text.append(example_config.text)
        soup.append(page_text)

        container = soup.new_tag("div", attrs={"class": "example-index"})
        for level, examples in examples.items():
            if not examples:
                continue

            header = soup.new_tag("h2", attrs={"class": "example-header"})
            header.append(level.value)
            container.append(header)

            table = soup.new_tag("table", attrs={"class": ["table", "example-table"]})

            # If there are additional columns to show besides just the example link,
            # include column titles in the table header
            if len(example_config.columns_to_show) > 0:
                thead = soup.new_tag("thead")
                thead_row = soup.new_tag("tr")

                for example_enum in example_config.columns_to_show.values():
                    col_header = soup.new_tag("th")
                    col_label = soup.new_tag("p")
                    col_label.append(example_enum.formatted_name())

                    col_header.append(col_label)
                    thead_row.append(col_header)

                link_col = soup.new_tag("th")
                link_label = soup.new_tag("p")
                link_label.append("Example")
                link_col.append(link_label)
                thead_row.append(link_col)

                thead.append(thead_row)
                table.append(thead)

            tbody = soup.new_tag("tbody")
            for example in examples:
                tr = soup.new_tag("tr")

                # The columns specify which attributes of each example to show;
                # for each attribute, a new cell value is added with the attribute from
                # the example
                if len(example_config.columns_to_show) > 0:
                    for attribute in example_config.columns_to_show:
                        col_td = soup.new_tag("td")
                        col_p = soup.new_tag("p")

                        attribute_value = getattr(example, attribute, "")
                        if isinstance(attribute_value, str):
                            col_p.append(attribute_value)
                        elif isinstance(attribute_value, list):
                            col_p.append(
                                ", ".join(item.value for item in attribute_value)
                            )

                        col_td.append(col_p)
                        tr.append(col_td)

                link_td = soup.new_tag("td")
                link_p = soup.new_tag("p")
                if example.link.startswith("http"):
                    link_href = soup.new_tag("a", attrs={"href": example.link})
                else:
                    link_href = soup.new_tag(
                        "a", attrs={"href": context["pathto"](example.link)}
                    )
                link_span = soup.new_tag("span")
                link_span.append(example.title)
                link_href.append(link_span)
                link_p.append(link_href)
                link_td.append(link_p)
                tr.append(link_td)
                tbody.append(tr)

            table.append(tbody)
            container.append(table)

        soup.append(container)
        return soup

    def render_example_gallery(configs: Iterable[pathlib.Path] = None) -> str:
        """Load and render examples for the example gallery.

        This function grabs examples from the various example gallery indexes.
        Each Ray library team maintains its own yml file which a standardized
        set of metadata with each example to ensure consistency across the
        code base.

        Duplicate examples will not raise an error as long as their fields match
        exactly.

        Parameters
        ----------
        configs : Iterable[pathlib.Path]
            Paths to example gallery files to ingest

        Returns
        -------
        str
            Example gallery examples rendered as HTML
        """
        if configs is None:
            configs = EXAMPLE_GALLERY_CONFIGS

        examples = {}
        for config in configs:
            config_path = pathlib.Path(config).relative_to("source")
            example_config = ExampleConfig(
                pathlib.Path(app.confdir) / config_path, app.srcdir
            )
            for example in example_config:
                # Check the ingested examples for duplicates. If there are duplicates,
                # check that they have the same field values, and keep only one.
                if example.link in examples:
                    existing_example_fields = vars(examples[example.link])
                    for key, value in vars(example):
                        if existing_example_fields.get(key) != value:
                            raise ValueError(
                                "One example was specified twice with different "
                                f"attributes: {vars(example)}\n"
                                f"{existing_example_fields}"
                            )
                else:
                    examples[example.link] = example

        soup = bs4.BeautifulSoup()
        list_area = soup.new_tag("div", attrs={"class": ["example-list-area"]})
        for example in examples.values():
            example_div = soup.new_tag("div", attrs={"class": "example"})

            if example.link.startswith("http"):
                link = example.link
            else:
                link = context["pathto"](example.link)

            example_link = soup.new_tag(
                "a",
                attrs={
                    "class": "example-link",
                    "href": link,
                },
            )
            example_icon_area = soup.new_tag(
                "div", attrs={"class": "example-icon-area"}
            )
            icon = soup.new_tag(
                "img",
                attrs={
                    "class": "example-icon",
                    "src": f"../_static/img/icon_bg_{random.randint(1, 5)}.jpg",
                },
            )
            remix_icon = soup.new_tag(
                "i", attrs={"class": f"{random.choice(REMIX_ICONS)} remix-icon"}
            )
            icon.append(remix_icon)
            example_icon_area.append(icon)
            example_link.append(example_icon_area)

            example_text_area = soup.new_tag(
                "div", attrs={"class": "example-text-area"}
            )

            example_title = soup.new_tag("b", attrs={"class": "example-title"})
            example_title.append(example.title)
            example_text_area.append(example_title)

            example_tags = soup.new_tag("span", attrs={"class": "example-tags"})
            frameworks = [item.value for item in example.frameworks]
            example_tags.append(
                ". ".join(
                    [example.skill_level.value, example.library.value, *frameworks]
                )
                + "."
            )
            example_text_area.append(example_tags)

            other_keywords = soup.new_tag(
                "span", attrs={"class": "example-other-keywords"}
            )
            other_keywords.append(
                " ".join(use_case.value for use_case in example.use_cases)
            )
            other_keywords.append(f" {example.contributor.tag}")
            example_text_area.append(other_keywords)

            # Add the appropriate text if the example comes from the community
            if example.contributor == Contributor.COMMUNITY:
                community_text_area = soup.new_tag(
                    "div", attrs={"class": "community-text-area"}
                )
                community_text = soup.new_tag("i", attrs={"class": "community-text"})
                community_text.append("*Contributed by the Ray Community")
                community_text_area.append(community_text)

                # Add emojis separately; they're not italicized in the mockups
                emojis = soup.new_tag("span", attrs={"class": "community-emojis"})
                emojis.append("💪 ✨")
                community_text_area.append(emojis)
                example_text_area.append(community_text_area)

            example_link.append(example_text_area)
            example_div.append(example_link)
            list_area.append(example_div)

        soup.append(list_area)
        return soup

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

    def render_use_cases_dropdown() -> bs4.BeautifulSoup:
        return render_example_gallery_dropdown(UseCase)

    def render_libraries_dropdown() -> bs4.BeautifulSoup:
        return render_example_gallery_dropdown(Library)

    def render_frameworks_dropdown() -> bs4.BeautifulSoup:
        return render_example_gallery_dropdown(Framework)

    def render_contributor_dropdown() -> bs4.BeautifulSoup:
        return render_example_gallery_dropdown(Contributor)

    context["cached_toctree"] = preload_sidebar_nav(
        context["toctree"],
        context["pathto"],
        context["root_doc"],
        pagename,
    )
    context["render_header_nav_links"] = render_header_nav_links
    context["render_library_examples"] = render_library_examples
    context["render_example_gallery"] = render_example_gallery
    context["render_use_cases_dropdown"] = render_use_cases_dropdown
    context["render_libraries_dropdown"] = render_libraries_dropdown
    context["render_frameworks_dropdown"] = render_frameworks_dropdown
    context["render_contributor_dropdown"] = render_contributor_dropdown

    # Update the HTML page context with a few extra utilities.
    context["pygments_highlight_python"] = lambda code: highlight(
        code, PythonLexer(), HtmlFormatter()
    )


def update_hrefs(input_soup: bs4.BeautifulSoup, n_levels_deep=0):
    soup = copy.copy(input_soup)
    for a in soup.select("a"):
        a["href"] = "../" * n_levels_deep + a["href"]

    return soup


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


def render_example_gallery_dropdown(cls: type) -> bs4.BeautifulSoup:
    """Render a dropdown menu selector for the example gallery.

    Parameters
    ----------
    cls : type
        ExampleEnum class type to use to populate the dropdown

    Returns
    -------
    bs4.BeautifulSoup
        Soup containing the dropdown element
    """
    soup = bs4.BeautifulSoup()

    dropdown_name = cls.formatted_name().lower().replace(" ", "-")
    dropdown_container = soup.new_tag(
        "div", attrs={"class": "filter-dropdown", "id": f"{dropdown_name}-dropdown"}
    )

    dropdown_show_checkbox = soup.new_tag(
        "input",
        attrs={
            "class": "dropdown-checkbox",
            "id": f"{dropdown_name}-checkbox",
            "type": "checkbox",
        },
    )
    dropdown_container.append(dropdown_show_checkbox)

    dropdown_label = soup.new_tag(
        "label", attrs={"class": "dropdown-label", "for": f"{dropdown_name}-checkbox"}
    )
    dropdown_label.append(cls.formatted_name())
    chevron = soup.new_tag("i", attrs={"class": "fa-solid fa-chevron-down"})
    dropdown_label.append(chevron)
    dropdown_container.append(dropdown_label)

    if cls.values():
        dropdown_options = soup.new_tag("div", attrs={"class": "dropdown-content"})

        for member in list(cls):
            label = soup.new_tag("label", attrs={"class": "checkbox-container"})
            label.append(member.value)

            tag = getattr(member, "tag", member.value)
            checkbox = soup.new_tag(
                "input",
                attrs={
                    "id": f"{tag}-checkbox",
                    "class": "filter-checkbox",
                    "type": "checkbox",
                },
            )
            label.append(checkbox)

            checkmark = soup.new_tag("span", attrs={"class": "checkmark"})
            label.append(checkmark)

            dropdown_options.append(label)

        dropdown_container.append(dropdown_options)

    soup.append(dropdown_container)
    return soup


def pregenerate_example_rsts(
    app: sphinx.application.Sphinx, *example_configs: Optional[List[str]]
):
    """Pregenerate RST files for the example page configuration files.

    This generates RST files for displaying example pages for Ray libraries.
    See `add_custom_assets` for more information about the custom template
    that gets rendered from these configuration files.

    Parameters
    ----------
    *example_configs : Optional[List[str]]
        Configuration files for which example pages are to be generated
    """
    if not example_configs:
        example_configs = EXAMPLE_GALLERY_CONFIGS

    for config in example_configs:
        # Depending on where the sphinx build command is run from, the path to the
        # target config file can change. Handle these paths manually to ensure it
        # works on RTD and locally.
        config_path = pathlib.Path(app.confdir) / pathlib.Path(config).relative_to(
            "source"
        )
        page_title = "Examples"
        title_decoration = "=" * len(page_title)
        with open(config_path.with_suffix(".rst"), "w") as f:
            f.write(
                f"{page_title}\n{title_decoration}\n\n"
                "  .. this file is pregenerated; please edit ./examples.yml to "
                "modify examples for this library."
            )


REMIX_ICONS = [
    "ri-24-hours-fill",
    "ri-24-hours-line",
    "ri-4k-fill",
    "ri-4k-line",
    "ri-a-b",
    "ri-account-box-fill",
    "ri-account-box-line",
    "ri-account-circle-fill",
    "ri-account-circle-line",
    "ri-account-pin-box-fill",
    "ri-account-pin-box-line",
    "ri-account-pin-circle-fill",
    "ri-account-pin-circle-line",
    "ri-add-box-fill",
    "ri-add-box-line",
    "ri-add-circle-fill",
    "ri-add-circle-line",
    "ri-add-fill",
    "ri-add-line",
    "ri-admin-fill",
    "ri-admin-line",
    "ri-advertisement-fill",
    "ri-advertisement-line",
    "ri-airplay-fill",
    "ri-airplay-line",
    "ri-alarm-fill",
    "ri-alarm-line",
    "ri-alarm-warning-fill",
    "ri-alarm-warning-line",
    "ri-album-fill",
    "ri-album-line",
    "ri-alert-fill",
    "ri-alert-line",
    "ri-aliens-fill",
    "ri-aliens-line",
    "ri-align-bottom",
    "ri-align-center",
    "ri-align-justify",
    "ri-align-left",
    "ri-align-right",
    "ri-align-top",
    "ri-align-vertically",
    "ri-alipay-fill",
    "ri-alipay-line",
    "ri-amazon-fill",
    "ri-amazon-line",
    "ri-anchor-fill",
    "ri-anchor-line",
    "ri-ancient-gate-fill",
    "ri-ancient-gate-line",
    "ri-ancient-pavilion-fill",
    "ri-ancient-pavilion-line",
    "ri-android-fill",
    "ri-android-line",
    "ri-angularjs-fill",
    "ri-angularjs-line",
    "ri-anticlockwise-2-fill",
    "ri-anticlockwise-2-line",
    "ri-anticlockwise-fill",
    "ri-anticlockwise-line",
    "ri-app-store-fill",
    "ri-app-store-line",
    "ri-apple-fill",
    "ri-apple-line",
    "ri-apps-2-fill",
    "ri-apps-2-line",
    "ri-apps-fill",
    "ri-apps-line",
    "ri-archive-drawer-fill",
    "ri-archive-drawer-line",
    "ri-archive-fill",
    "ri-archive-line",
    "ri-arrow-down-circle-fill",
    "ri-arrow-down-circle-line",
    "ri-arrow-down-fill",
    "ri-arrow-down-line",
    "ri-arrow-down-s-fill",
    "ri-arrow-down-s-line",
    "ri-arrow-drop-down-fill",
    "ri-arrow-drop-down-line",
    "ri-arrow-drop-left-fill",
    "ri-arrow-drop-left-line",
    "ri-arrow-drop-right-fill",
    "ri-arrow-drop-right-line",
    "ri-arrow-drop-up-fill",
    "ri-arrow-drop-up-line",
    "ri-arrow-go-back-fill",
    "ri-arrow-go-back-line",
    "ri-arrow-go-forward-fill",
    "ri-arrow-go-forward-line",
    "ri-arrow-left-circle-fill",
    "ri-arrow-left-circle-line",
    "ri-arrow-left-down-fill",
    "ri-arrow-left-down-line",
    "ri-arrow-left-fill",
    "ri-arrow-left-line",
    "ri-arrow-left-right-fill",
    "ri-arrow-left-right-line",
    "ri-arrow-left-s-fill",
    "ri-arrow-left-s-line",
    "ri-arrow-left-up-fill",
    "ri-arrow-left-up-line",
    "ri-arrow-right-circle-fill",
    "ri-arrow-right-circle-line",
    "ri-arrow-right-down-fill",
    "ri-arrow-right-down-line",
    "ri-arrow-right-fill",
    "ri-arrow-right-line",
    "ri-arrow-right-s-fill",
    "ri-arrow-right-s-line",
    "ri-arrow-right-up-fill",
    "ri-arrow-right-up-line",
    "ri-arrow-up-circle-fill",
    "ri-arrow-up-circle-line",
    "ri-arrow-up-down-fill",
    "ri-arrow-up-down-line",
    "ri-arrow-up-fill",
    "ri-arrow-up-line",
    "ri-arrow-up-s-fill",
    "ri-arrow-up-s-line",
    "ri-artboard-2-fill",
    "ri-artboard-2-line",
    "ri-artboard-fill",
    "ri-artboard-line",
    "ri-article-fill",
    "ri-article-line",
    "ri-aspect-ratio-fill",
    "ri-aspect-ratio-line",
    "ri-asterisk",
    "ri-at-fill",
    "ri-at-line",
    "ri-attachment-2",
    "ri-attachment-fill",
    "ri-attachment-line",
    "ri-auction-fill",
    "ri-auction-line",
    "ri-award-fill",
    "ri-award-line",
    "ri-baidu-fill",
    "ri-baidu-line",
    "ri-ball-pen-fill",
    "ri-ball-pen-line",
    "ri-bank-card-2-fill",
    "ri-bank-card-2-line",
    "ri-bank-card-fill",
    "ri-bank-card-line",
    "ri-bank-fill",
    "ri-bank-line",
    "ri-bar-chart-2-fill",
    "ri-bar-chart-2-line",
    "ri-bar-chart-box-fill",
    "ri-bar-chart-box-line",
    "ri-bar-chart-fill",
    "ri-bar-chart-grouped-fill",
    "ri-bar-chart-grouped-line",
    "ri-bar-chart-horizontal-fill",
    "ri-bar-chart-horizontal-line",
    "ri-bar-chart-line",
    "ri-barcode-box-fill",
    "ri-barcode-box-line",
    "ri-barcode-fill",
    "ri-barcode-line",
    "ri-barricade-fill",
    "ri-barricade-line",
    "ri-base-station-fill",
    "ri-base-station-line",
    "ri-basketball-fill",
    "ri-basketball-line",
    "ri-battery-2-charge-fill",
    "ri-battery-2-charge-line",
    "ri-battery-2-fill",
    "ri-battery-2-line",
    "ri-battery-charge-fill",
    "ri-battery-charge-line",
    "ri-battery-fill",
    "ri-battery-line",
    "ri-battery-low-fill",
    "ri-battery-low-line",
    "ri-battery-saver-fill",
    "ri-battery-saver-line",
    "ri-battery-share-fill",
    "ri-battery-share-line",
    "ri-bear-smile-fill",
    "ri-bear-smile-line",
    "ri-behance-fill",
    "ri-behance-line",
    "ri-bell-fill",
    "ri-bell-line",
    "ri-bike-fill",
    "ri-bike-line",
    "ri-bilibili-fill",
    "ri-bilibili-line",
    "ri-bill-fill",
    "ri-bill-line",
    "ri-billiards-fill",
    "ri-billiards-line",
    "ri-bit-coin-fill",
    "ri-bit-coin-line",
    "ri-blaze-fill",
    "ri-blaze-line",
    "ri-bluetooth-connect-fill",
    "ri-bluetooth-connect-line",
    "ri-bluetooth-fill",
    "ri-bluetooth-line",
    "ri-blur-off-fill",
    "ri-blur-off-line",
    "ri-body-scan-fill",
    "ri-body-scan-line",
    "ri-bold",
    "ri-book-2-fill",
    "ri-book-2-line",
    "ri-book-3-fill",
    "ri-book-3-line",
    "ri-book-fill",
    "ri-book-line",
    "ri-book-marked-fill",
    "ri-book-marked-line",
    "ri-book-open-fill",
    "ri-book-open-line",
    "ri-book-read-fill",
    "ri-book-read-line",
    "ri-booklet-fill",
    "ri-booklet-line",
    "ri-bookmark-2-fill",
    "ri-bookmark-2-line",
    "ri-bookmark-3-fill",
    "ri-bookmark-3-line",
    "ri-bookmark-fill",
    "ri-bookmark-line",
    "ri-boxing-fill",
    "ri-boxing-line",
    "ri-braces-fill",
    "ri-braces-line",
    "ri-brackets-fill",
    "ri-brackets-line",
    "ri-briefcase-2-fill",
    "ri-briefcase-2-line",
    "ri-briefcase-3-fill",
    "ri-briefcase-3-line",
    "ri-briefcase-4-fill",
    "ri-briefcase-4-line",
    "ri-briefcase-5-fill",
    "ri-briefcase-5-line",
    "ri-briefcase-fill",
    "ri-briefcase-line",
    "ri-bring-forward",
    "ri-bring-to-front",
    "ri-broadcast-fill",
    "ri-broadcast-line",
    "ri-brush-2-fill",
    "ri-brush-2-line",
    "ri-brush-3-fill",
    "ri-brush-3-line",
    "ri-brush-4-fill",
    "ri-brush-4-line",
    "ri-brush-fill",
    "ri-brush-line",
    "ri-bubble-chart-fill",
    "ri-bubble-chart-line",
    "ri-bug-2-fill",
    "ri-bug-2-line",
    "ri-bug-fill",
    "ri-bug-line",
    "ri-building-2-fill",
    "ri-building-2-line",
    "ri-building-3-fill",
    "ri-building-3-line",
    "ri-building-4-fill",
    "ri-building-4-line",
    "ri-building-fill",
    "ri-building-line",
    "ri-bus-2-fill",
    "ri-bus-2-line",
    "ri-bus-fill",
    "ri-bus-line",
    "ri-bus-wifi-fill",
    "ri-bus-wifi-line",
    "ri-cactus-fill",
    "ri-cactus-line",
    "ri-cake-2-fill",
    "ri-cake-2-line",
    "ri-cake-3-fill",
    "ri-cake-3-line",
    "ri-cake-fill",
    "ri-cake-line",
    "ri-calculator-fill",
    "ri-calculator-line",
    "ri-calendar-2-fill",
    "ri-calendar-2-line",
    "ri-calendar-check-fill",
    "ri-calendar-check-line",
    "ri-calendar-event-fill",
    "ri-calendar-event-line",
    "ri-calendar-fill",
    "ri-calendar-line",
    "ri-calendar-todo-fill",
    "ri-calendar-todo-line",
    "ri-camera-2-fill",
    "ri-camera-2-line",
    "ri-camera-3-fill",
    "ri-camera-3-line",
    "ri-camera-fill",
    "ri-camera-lens-fill",
    "ri-camera-lens-line",
    "ri-camera-line",
    "ri-camera-off-fill",
    "ri-camera-off-line",
    "ri-camera-switch-fill",
    "ri-camera-switch-line",
    "ri-capsule-fill",
    "ri-capsule-line",
    "ri-car-fill",
    "ri-car-line",
    "ri-car-washing-fill",
    "ri-car-washing-line",
    "ri-caravan-fill",
    "ri-caravan-line",
    "ri-cast-fill",
    "ri-cast-line",
    "ri-cellphone-fill",
    "ri-cellphone-line",
    "ri-celsius-fill",
    "ri-celsius-line",
    "ri-centos-fill",
    "ri-centos-line",
    "ri-character-recognition-fill",
    "ri-character-recognition-line",
    "ri-charging-pile-2-fill",
    "ri-charging-pile-2-line",
    "ri-charging-pile-fill",
    "ri-charging-pile-line",
    "ri-chat-1-fill",
    "ri-chat-1-line",
    "ri-chat-2-fill",
    "ri-chat-2-line",
    "ri-chat-3-fill",
    "ri-chat-3-line",
    "ri-chat-4-fill",
    "ri-chat-4-line",
    "ri-chat-check-fill",
    "ri-chat-check-line",
    "ri-chat-delete-fill",
    "ri-chat-delete-line",
    "ri-chat-download-fill",
    "ri-chat-download-line",
    "ri-chat-follow-up-fill",
    "ri-chat-follow-up-line",
    "ri-chat-forward-fill",
    "ri-chat-forward-line",
    "ri-chat-heart-fill",
    "ri-chat-heart-line",
    "ri-chat-history-fill",
    "ri-chat-history-line",
    "ri-chat-new-fill",
    "ri-chat-new-line",
    "ri-chat-off-fill",
    "ri-chat-off-line",
    "ri-chat-poll-fill",
    "ri-chat-poll-line",
    "ri-chat-private-fill",
    "ri-chat-private-line",
    "ri-chat-quote-fill",
    "ri-chat-quote-line",
    "ri-chat-settings-fill",
    "ri-chat-settings-line",
    "ri-chat-smile-2-fill",
    "ri-chat-smile-2-line",
    "ri-chat-smile-3-fill",
    "ri-chat-smile-3-line",
    "ri-chat-smile-fill",
    "ri-chat-smile-line",
    "ri-chat-upload-fill",
    "ri-chat-upload-line",
    "ri-chat-voice-fill",
    "ri-chat-voice-line",
    "ri-check-double-fill",
    "ri-check-double-line",
    "ri-check-fill",
    "ri-check-line",
    "ri-checkbox-blank-circle-fill",
    "ri-checkbox-blank-circle-line",
    "ri-checkbox-blank-fill",
    "ri-checkbox-blank-line",
    "ri-checkbox-circle-fill",
    "ri-checkbox-circle-line",
    "ri-checkbox-fill",
    "ri-checkbox-indeterminate-fill",
    "ri-checkbox-indeterminate-line",
    "ri-checkbox-line",
    "ri-checkbox-multiple-blank-fill",
    "ri-checkbox-multiple-blank-line",
    "ri-checkbox-multiple-fill",
    "ri-checkbox-multiple-line",
    "ri-china-railway-fill",
    "ri-china-railway-line",
    "ri-chrome-fill",
    "ri-chrome-line",
    "ri-clapperboard-fill",
    "ri-clapperboard-line",
    "ri-clipboard-fill",
    "ri-clipboard-line",
    "ri-clockwise-2-fill",
    "ri-clockwise-2-line",
    "ri-clockwise-fill",
    "ri-clockwise-line",
    "ri-close-circle-fill",
    "ri-close-circle-line",
    "ri-close-fill",
    "ri-close-line",
    "ri-closed-captioning-fill",
    "ri-closed-captioning-line",
    "ri-cloud-fill",
    "ri-cloud-line",
    "ri-cloud-off-fill",
    "ri-cloud-off-line",
    "ri-cloud-windy-fill",
    "ri-cloud-windy-line",
    "ri-cloudy-2-fill",
    "ri-cloudy-2-line",
    "ri-cloudy-fill",
    "ri-cloudy-line",
    "ri-code-box-fill",
    "ri-code-box-line",
    "ri-code-fill",
    "ri-code-line",
    "ri-code-s-fill",
    "ri-code-s-line",
    "ri-code-s-slash-fill",
    "ri-code-s-slash-line",
    "ri-code-view",
    "ri-codepen-fill",
    "ri-codepen-line",
    "ri-coin-fill",
    "ri-coin-line",
    "ri-coins-fill",
    "ri-coins-line",
    "ri-collage-fill",
    "ri-collage-line",
    "ri-command-fill",
    "ri-command-line",
    "ri-community-fill",
    "ri-community-line",
    "ri-compass-2-fill",
    "ri-compass-2-line",
    "ri-compass-3-fill",
    "ri-compass-3-line",
    "ri-compass-4-fill",
    "ri-compass-4-line",
    "ri-compass-discover-fill",
    "ri-compass-discover-line",
    "ri-compass-fill",
    "ri-compass-line",
    "ri-compasses-2-fill",
    "ri-compasses-2-line",
    "ri-compasses-fill",
    "ri-compasses-line",
    "ri-computer-fill",
    "ri-computer-line",
    "ri-contacts-book-2-fill",
    "ri-contacts-book-2-line",
    "ri-contacts-book-fill",
    "ri-contacts-book-line",
    "ri-contacts-book-upload-fill",
    "ri-contacts-book-upload-line",
    "ri-contacts-fill",
    "ri-contacts-line",
    "ri-contrast-2-fill",
    "ri-contrast-2-line",
    "ri-contrast-drop-2-fill",
    "ri-contrast-drop-2-line",
    "ri-contrast-drop-fill",
    "ri-contrast-drop-line",
    "ri-contrast-fill",
    "ri-contrast-line",
    "ri-copper-coin-fill",
    "ri-copper-coin-line",
    "ri-copper-diamond-fill",
    "ri-copper-diamond-line",
    "ri-copyleft-fill",
    "ri-copyleft-line",
    "ri-copyright-fill",
    "ri-copyright-line",
    "ri-coreos-fill",
    "ri-coreos-line",
    "ri-coupon-2-fill",
    "ri-coupon-2-line",
    "ri-coupon-3-fill",
    "ri-coupon-3-line",
    "ri-coupon-4-fill",
    "ri-coupon-4-line",
    "ri-coupon-5-fill",
    "ri-coupon-5-line",
    "ri-coupon-fill",
    "ri-coupon-line",
    "ri-cpu-fill",
    "ri-cpu-line",
    "ri-creative-commons-by-fill",
    "ri-creative-commons-by-line",
    "ri-creative-commons-fill",
    "ri-creative-commons-line",
    "ri-creative-commons-nc-fill",
    "ri-creative-commons-nc-line",
    "ri-creative-commons-nd-fill",
    "ri-creative-commons-nd-line",
    "ri-creative-commons-sa-fill",
    "ri-creative-commons-sa-line",
    "ri-creative-commons-zero-fill",
    "ri-creative-commons-zero-line",
    "ri-criminal-fill",
    "ri-criminal-line",
    "ri-crop-2-fill",
    "ri-crop-2-line",
    "ri-crop-fill",
    "ri-crop-line",
    "ri-css3-fill",
    "ri-css3-line",
    "ri-cup-fill",
    "ri-cup-line",
    "ri-currency-fill",
    "ri-currency-line",
    "ri-cursor-fill",
    "ri-cursor-line",
    "ri-customer-service-2-fill",
    "ri-customer-service-2-line",
    "ri-customer-service-fill",
    "ri-customer-service-line",
    "ri-dashboard-2-fill",
    "ri-dashboard-2-line",
    "ri-dashboard-3-fill",
    "ri-dashboard-3-line",
    "ri-dashboard-fill",
    "ri-dashboard-line",
    "ri-database-2-fill",
    "ri-database-2-line",
    "ri-database-fill",
    "ri-database-line",
    "ri-delete-back-2-fill",
    "ri-delete-back-2-line",
    "ri-delete-back-fill",
    "ri-delete-back-line",
    "ri-delete-bin-2-fill",
    "ri-delete-bin-2-line",
    "ri-delete-bin-3-fill",
    "ri-delete-bin-3-line",
    "ri-delete-bin-4-fill",
    "ri-delete-bin-4-line",
    "ri-delete-bin-5-fill",
    "ri-delete-bin-5-line",
    "ri-delete-bin-6-fill",
    "ri-delete-bin-6-line",
    "ri-delete-bin-7-fill",
    "ri-delete-bin-7-line",
    "ri-delete-bin-fill",
    "ri-delete-bin-line",
    "ri-delete-column",
    "ri-delete-row",
    "ri-device-fill",
    "ri-device-line",
    "ri-device-recover-fill",
    "ri-device-recover-line",
    "ri-dingding-fill",
    "ri-dingding-line",
    "ri-direction-fill",
    "ri-direction-line",
    "ri-disc-fill",
    "ri-disc-line",
    "ri-discord-fill",
    "ri-discord-line",
    "ri-discuss-fill",
    "ri-discuss-line",
    "ri-dislike-fill",
    "ri-dislike-line",
    "ri-disqus-fill",
    "ri-disqus-line",
    "ri-divide-fill",
    "ri-divide-line",
    "ri-donut-chart-fill",
    "ri-donut-chart-line",
    "ri-door-closed-fill",
    "ri-door-closed-line",
    "ri-door-fill",
    "ri-door-line",
    "ri-door-lock-box-fill",
    "ri-door-lock-box-line",
    "ri-door-lock-fill",
    "ri-door-lock-line",
    "ri-door-open-fill",
    "ri-door-open-line",
    "ri-dossier-fill",
    "ri-dossier-line",
    "ri-douban-fill",
    "ri-douban-line",
    "ri-double-quotes-l",
    "ri-double-quotes-r",
    "ri-download-2-fill",
    "ri-download-2-line",
    "ri-download-cloud-2-fill",
    "ri-download-cloud-2-line",
    "ri-download-cloud-fill",
    "ri-download-cloud-line",
    "ri-download-fill",
    "ri-download-line",
    "ri-draft-fill",
    "ri-draft-line",
    "ri-drag-drop-fill",
    "ri-drag-drop-line",
    "ri-drag-move-2-fill",
    "ri-drag-move-2-line",
    "ri-drag-move-fill",
    "ri-drag-move-line",
    "ri-dribbble-fill",
    "ri-dribbble-line",
    "ri-drive-fill",
    "ri-drive-line",
    "ri-drizzle-fill",
    "ri-drizzle-line",
    "ri-drop-fill",
    "ri-drop-line",
    "ri-dropbox-fill",
    "ri-dropbox-line",
    "ri-dual-sim-1-fill",
    "ri-dual-sim-1-line",
    "ri-dual-sim-2-fill",
    "ri-dual-sim-2-line",
    "ri-dv-fill",
    "ri-dv-line",
    "ri-dvd-fill",
    "ri-dvd-line",
    "ri-e-bike-2-fill",
    "ri-e-bike-2-line",
    "ri-e-bike-fill",
    "ri-e-bike-line",
    "ri-earth-fill",
    "ri-earth-line",
    "ri-earthquake-fill",
    "ri-earthquake-line",
    "ri-edge-fill",
    "ri-edge-line",
    "ri-edit-2-fill",
    "ri-edit-2-line",
    "ri-edit-box-fill",
    "ri-edit-box-line",
    "ri-edit-circle-fill",
    "ri-edit-circle-line",
    "ri-edit-fill",
    "ri-edit-line",
    "ri-eject-fill",
    "ri-eject-line",
    "ri-emotion-2-fill",
    "ri-emotion-2-line",
    "ri-emotion-fill",
    "ri-emotion-happy-fill",
    "ri-emotion-happy-line",
    "ri-emotion-laugh-fill",
    "ri-emotion-laugh-line",
    "ri-emotion-line",
    "ri-emotion-normal-fill",
    "ri-emotion-normal-line",
    "ri-emotion-sad-fill",
    "ri-emotion-sad-line",
    "ri-emotion-unhappy-fill",
    "ri-emotion-unhappy-line",
    "ri-empathize-fill",
    "ri-empathize-line",
    "ri-emphasis-cn",
    "ri-emphasis",
    "ri-english-input",
    "ri-equalizer-fill",
    "ri-equalizer-line",
    "ri-eraser-fill",
    "ri-eraser-line",
    "ri-error-warning-fill",
    "ri-error-warning-line",
    "ri-evernote-fill",
    "ri-evernote-line",
    "ri-exchange-box-fill",
    "ri-exchange-box-line",
    "ri-exchange-cny-fill",
    "ri-exchange-cny-line",
    "ri-exchange-dollar-fill",
    "ri-exchange-dollar-line",
    "ri-exchange-fill",
    "ri-exchange-funds-fill",
    "ri-exchange-funds-line",
    "ri-exchange-line",
    "ri-external-link-fill",
    "ri-external-link-line",
    "ri-eye-2-fill",
    "ri-eye-2-line",
    "ri-eye-close-fill",
    "ri-eye-close-line",
    "ri-eye-fill",
    "ri-eye-line",
    "ri-eye-off-fill",
    "ri-eye-off-line",
    "ri-facebook-box-fill",
    "ri-facebook-box-line",
    "ri-facebook-circle-fill",
    "ri-facebook-circle-line",
    "ri-facebook-fill",
    "ri-facebook-line",
    "ri-fahrenheit-fill",
    "ri-fahrenheit-line",
    "ri-feedback-fill",
    "ri-feedback-line",
    "ri-file-2-fill",
    "ri-file-2-line",
    "ri-file-3-fill",
    "ri-file-3-line",
    "ri-file-4-fill",
    "ri-file-4-line",
    "ri-file-add-fill",
    "ri-file-add-line",
    "ri-file-chart-2-fill",
    "ri-file-chart-2-line",
    "ri-file-chart-fill",
    "ri-file-chart-line",
    "ri-file-cloud-fill",
    "ri-file-cloud-line",
    "ri-file-code-fill",
    "ri-file-code-line",
    "ri-file-copy-2-fill",
    "ri-file-copy-2-line",
    "ri-file-copy-fill",
    "ri-file-copy-line",
    "ri-file-damage-fill",
    "ri-file-damage-line",
    "ri-file-download-fill",
    "ri-file-download-line",
    "ri-file-edit-fill",
    "ri-file-edit-line",
    "ri-file-excel-2-fill",
    "ri-file-excel-2-line",
    "ri-file-excel-fill",
    "ri-file-excel-line",
    "ri-file-fill",
    "ri-file-forbid-fill",
    "ri-file-forbid-line",
    "ri-file-gif-fill",
    "ri-file-gif-line",
    "ri-file-history-fill",
    "ri-file-history-line",
    "ri-file-hwp-fill",
    "ri-file-hwp-line",
    "ri-file-info-fill",
    "ri-file-info-line",
    "ri-file-line",
    "ri-file-list-2-fill",
    "ri-file-list-2-line",
    "ri-file-list-3-fill",
    "ri-file-list-3-line",
    "ri-file-list-fill",
    "ri-file-list-line",
    "ri-file-lock-fill",
    "ri-file-lock-line",
    "ri-file-marked-fill",
    "ri-file-marked-line",
    "ri-file-music-fill",
    "ri-file-music-line",
    "ri-file-paper-2-fill",
    "ri-file-paper-2-line",
    "ri-file-paper-fill",
    "ri-file-paper-line",
    "ri-file-pdf-fill",
    "ri-file-pdf-line",
    "ri-file-ppt-2-fill",
    "ri-file-ppt-2-line",
    "ri-file-ppt-fill",
    "ri-file-ppt-line",
    "ri-file-reduce-fill",
    "ri-file-reduce-line",
    "ri-file-search-fill",
    "ri-file-search-line",
    "ri-file-settings-fill",
    "ri-file-settings-line",
    "ri-file-shield-2-fill",
    "ri-file-shield-2-line",
    "ri-file-shield-fill",
    "ri-file-shield-line",
    "ri-file-shred-fill",
    "ri-file-shred-line",
    "ri-file-text-fill",
    "ri-file-text-line",
    "ri-file-transfer-fill",
    "ri-file-transfer-line",
    "ri-file-unknow-fill",
    "ri-file-unknow-line",
    "ri-file-upload-fill",
    "ri-file-upload-line",
    "ri-file-user-fill",
    "ri-file-user-line",
    "ri-file-warning-fill",
    "ri-file-warning-line",
    "ri-file-word-2-fill",
    "ri-file-word-2-line",
    "ri-file-word-fill",
    "ri-file-word-line",
    "ri-file-zip-fill",
    "ri-file-zip-line",
    "ri-film-fill",
    "ri-film-line",
    "ri-filter-2-fill",
    "ri-filter-2-line",
    "ri-filter-3-fill",
    "ri-filter-3-line",
    "ri-filter-fill",
    "ri-filter-line",
    "ri-filter-off-fill",
    "ri-filter-off-line",
    "ri-find-replace-fill",
    "ri-find-replace-line",
    "ri-finder-fill",
    "ri-finder-line",
    "ri-fingerprint-2-fill",
    "ri-fingerprint-2-line",
    "ri-fingerprint-fill",
    "ri-fingerprint-line",
    "ri-fire-fill",
    "ri-fire-line",
    "ri-firefox-fill",
    "ri-firefox-line",
    "ri-first-aid-kit-fill",
    "ri-first-aid-kit-line",
    "ri-flag-2-fill",
    "ri-flag-2-line",
    "ri-flag-fill",
    "ri-flag-line",
    "ri-flashlight-fill",
    "ri-flashlight-line",
    "ri-flask-fill",
    "ri-flask-line",
    "ri-flight-land-fill",
    "ri-flight-land-line",
    "ri-flight-takeoff-fill",
    "ri-flight-takeoff-line",
    "ri-flood-fill",
    "ri-flood-line",
    "ri-flow-chart",
    "ri-flutter-fill",
    "ri-flutter-line",
    "ri-focus-2-fill",
    "ri-focus-2-line",
    "ri-focus-3-fill",
    "ri-focus-3-line",
    "ri-focus-fill",
    "ri-focus-line",
    "ri-foggy-fill",
    "ri-foggy-line",
    "ri-folder-2-fill",
    "ri-folder-2-line",
    "ri-folder-3-fill",
    "ri-folder-3-line",
    "ri-folder-4-fill",
    "ri-folder-4-line",
    "ri-folder-5-fill",
    "ri-folder-5-line",
    "ri-folder-add-fill",
    "ri-folder-add-line",
    "ri-folder-chart-2-fill",
    "ri-folder-chart-2-line",
    "ri-folder-chart-fill",
    "ri-folder-chart-line",
    "ri-folder-download-fill",
    "ri-folder-download-line",
    "ri-folder-fill",
    "ri-folder-forbid-fill",
    "ri-folder-forbid-line",
    "ri-folder-history-fill",
    "ri-folder-history-line",
    "ri-folder-info-fill",
    "ri-folder-info-line",
    "ri-folder-keyhole-fill",
    "ri-folder-keyhole-line",
    "ri-folder-line",
    "ri-folder-lock-fill",
    "ri-folder-lock-line",
    "ri-folder-music-fill",
    "ri-folder-music-line",
    "ri-folder-open-fill",
    "ri-folder-open-line",
    "ri-folder-received-fill",
    "ri-folder-received-line",
    "ri-folder-reduce-fill",
    "ri-folder-reduce-line",
    "ri-folder-settings-fill",
    "ri-folder-settings-line",
    "ri-folder-shared-fill",
    "ri-folder-shared-line",
    "ri-folder-shield-2-fill",
    "ri-folder-shield-2-line",
    "ri-folder-shield-fill",
    "ri-folder-shield-line",
    "ri-folder-transfer-fill",
    "ri-folder-transfer-line",
    "ri-folder-unknow-fill",
    "ri-folder-unknow-line",
    "ri-folder-upload-fill",
    "ri-folder-upload-line",
    "ri-folder-user-fill",
    "ri-folder-user-line",
    "ri-folder-warning-fill",
    "ri-folder-warning-line",
    "ri-folder-zip-fill",
    "ri-folder-zip-line",
    "ri-folders-fill",
    "ri-folders-line",
    "ri-font-color",
    "ri-font-size-2",
    "ri-font-size",
    "ri-football-fill",
    "ri-football-line",
    "ri-footprint-fill",
    "ri-footprint-line",
    "ri-forbid-2-fill",
    "ri-forbid-2-line",
    "ri-forbid-fill",
    "ri-forbid-line",
    "ri-format-clear",
    "ri-fridge-fill",
    "ri-fridge-line",
    "ri-fullscreen-exit-fill",
    "ri-fullscreen-exit-line",
    "ri-fullscreen-fill",
    "ri-fullscreen-line",
    "ri-function-fill",
    "ri-function-line",
    "ri-functions",
    "ri-funds-box-fill",
    "ri-funds-box-line",
    "ri-funds-fill",
    "ri-funds-line",
    "ri-gallery-fill",
    "ri-gallery-line",
    "ri-gallery-upload-fill",
    "ri-gallery-upload-line",
    "ri-game-fill",
    "ri-game-line",
    "ri-gamepad-fill",
    "ri-gamepad-line",
    "ri-gas-station-fill",
    "ri-gas-station-line",
    "ri-gatsby-fill",
    "ri-gatsby-line",
    "ri-genderless-fill",
    "ri-genderless-line",
    "ri-ghost-2-fill",
    "ri-ghost-2-line",
    "ri-ghost-fill",
    "ri-ghost-line",
    "ri-ghost-smile-fill",
    "ri-ghost-smile-line",
    "ri-gift-2-fill",
    "ri-gift-2-line",
    "ri-gift-fill",
    "ri-gift-line",
    "ri-git-branch-fill",
    "ri-git-branch-line",
    "ri-git-commit-fill",
    "ri-git-commit-line",
    "ri-git-merge-fill",
    "ri-git-merge-line",
    "ri-git-pull-request-fill",
    "ri-git-pull-request-line",
    "ri-git-repository-commits-fill",
    "ri-git-repository-commits-line",
    "ri-git-repository-fill",
    "ri-git-repository-line",
    "ri-git-repository-private-fill",
    "ri-git-repository-private-line",
    "ri-github-fill",
    "ri-github-line",
    "ri-gitlab-fill",
    "ri-gitlab-line",
    "ri-global-fill",
    "ri-global-line",
    "ri-globe-fill",
    "ri-globe-line",
    "ri-goblet-fill",
    "ri-goblet-line",
    "ri-google-fill",
    "ri-google-line",
    "ri-google-play-fill",
    "ri-google-play-line",
    "ri-government-fill",
    "ri-government-line",
    "ri-gps-fill",
    "ri-gps-line",
    "ri-gradienter-fill",
    "ri-gradienter-line",
    "ri-grid-fill",
    "ri-grid-line",
    "ri-group-2-fill",
    "ri-group-2-line",
    "ri-group-fill",
    "ri-group-line",
    "ri-guide-fill",
    "ri-guide-line",
    "ri-h-1",
    "ri-h-2",
    "ri-h-3",
    "ri-h-4",
    "ri-h-5",
    "ri-h-6",
    "ri-hail-fill",
    "ri-hail-line",
    "ri-hammer-fill",
    "ri-hammer-line",
    "ri-hand-coin-fill",
    "ri-hand-coin-line",
    "ri-hand-heart-fill",
    "ri-hand-heart-line",
    "ri-hand-sanitizer-fill",
    "ri-hand-sanitizer-line",
    "ri-handbag-fill",
    "ri-handbag-line",
    "ri-hard-drive-2-fill",
    "ri-hard-drive-2-line",
    "ri-hard-drive-fill",
    "ri-hard-drive-line",
    "ri-hashtag",
    "ri-haze-2-fill",
    "ri-haze-2-line",
    "ri-haze-fill",
    "ri-haze-line",
    "ri-hd-fill",
    "ri-hd-line",
    "ri-heading",
    "ri-headphone-fill",
    "ri-headphone-line",
    "ri-health-book-fill",
    "ri-health-book-line",
    "ri-heart-2-fill",
    "ri-heart-2-line",
    "ri-heart-3-fill",
    "ri-heart-3-line",
    "ri-heart-add-fill",
    "ri-heart-add-line",
    "ri-heart-fill",
    "ri-heart-line",
    "ri-heart-pulse-fill",
    "ri-heart-pulse-line",
    "ri-hearts-fill",
    "ri-hearts-line",
    "ri-heavy-showers-fill",
    "ri-heavy-showers-line",
    "ri-history-fill",
    "ri-history-line",
    "ri-home-2-fill",
    "ri-home-2-line",
    "ri-home-3-fill",
    "ri-home-3-line",
    "ri-home-4-fill",
    "ri-home-4-line",
    "ri-home-5-fill",
    "ri-home-5-line",
    "ri-home-6-fill",
    "ri-home-6-line",
    "ri-home-7-fill",
    "ri-home-7-line",
    "ri-home-8-fill",
    "ri-home-8-line",
    "ri-home-fill",
    "ri-home-gear-fill",
    "ri-home-gear-line",
    "ri-home-heart-fill",
    "ri-home-heart-line",
    "ri-home-line",
    "ri-home-smile-2-fill",
    "ri-home-smile-2-line",
    "ri-home-smile-fill",
    "ri-home-smile-line",
    "ri-home-wifi-fill",
    "ri-home-wifi-line",
    "ri-honor-of-kings-fill",
    "ri-honor-of-kings-line",
    "ri-honour-fill",
    "ri-honour-line",
    "ri-hospital-fill",
    "ri-hospital-line",
    "ri-hotel-bed-fill",
    "ri-hotel-bed-line",
    "ri-hotel-fill",
    "ri-hotel-line",
    "ri-hotspot-fill",
    "ri-hotspot-line",
    "ri-hq-fill",
    "ri-hq-line",
    "ri-html5-fill",
    "ri-html5-line",
    "ri-ie-fill",
    "ri-ie-line",
    "ri-image-2-fill",
    "ri-image-2-line",
    "ri-image-add-fill",
    "ri-image-add-line",
    "ri-image-edit-fill",
    "ri-image-edit-line",
    "ri-image-fill",
    "ri-image-line",
    "ri-inbox-archive-fill",
    "ri-inbox-archive-line",
    "ri-inbox-fill",
    "ri-inbox-line",
    "ri-inbox-unarchive-fill",
    "ri-inbox-unarchive-line",
    "ri-increase-decrease-fill",
    "ri-increase-decrease-line",
    "ri-indent-decrease",
    "ri-indent-increase",
    "ri-indeterminate-circle-fill",
    "ri-indeterminate-circle-line",
    "ri-information-fill",
    "ri-information-line",
    "ri-infrared-thermometer-fill",
    "ri-infrared-thermometer-line",
    "ri-ink-bottle-fill",
    "ri-ink-bottle-line",
    "ri-input-cursor-move",
    "ri-input-method-fill",
    "ri-input-method-line",
    "ri-insert-column-left",
    "ri-insert-column-right",
    "ri-insert-row-bottom",
    "ri-insert-row-top",
    "ri-instagram-fill",
    "ri-instagram-line",
    "ri-install-fill",
    "ri-install-line",
    "ri-invision-fill",
    "ri-invision-line",
    "ri-italic",
    "ri-kakao-talk-fill",
    "ri-kakao-talk-line",
    "ri-key-2-fill",
    "ri-key-2-line",
    "ri-key-fill",
    "ri-key-line",
    "ri-keyboard-box-fill",
    "ri-keyboard-box-line",
    "ri-keyboard-fill",
    "ri-keyboard-line",
    "ri-keynote-fill",
    "ri-keynote-line",
    "ri-knife-blood-fill",
    "ri-knife-blood-line",
    "ri-knife-fill",
    "ri-knife-line",
    "ri-landscape-fill",
    "ri-landscape-line",
    "ri-layout-2-fill",
    "ri-layout-2-line",
    "ri-layout-3-fill",
    "ri-layout-3-line",
    "ri-layout-4-fill",
    "ri-layout-4-line",
    "ri-layout-5-fill",
    "ri-layout-5-line",
    "ri-layout-6-fill",
    "ri-layout-6-line",
    "ri-layout-bottom-2-fill",
    "ri-layout-bottom-2-line",
    "ri-layout-bottom-fill",
    "ri-layout-bottom-line",
    "ri-layout-column-fill",
    "ri-layout-column-line",
    "ri-layout-fill",
    "ri-layout-grid-fill",
    "ri-layout-grid-line",
    "ri-layout-left-2-fill",
    "ri-layout-left-2-line",
    "ri-layout-left-fill",
    "ri-layout-left-line",
    "ri-layout-line",
    "ri-layout-masonry-fill",
    "ri-layout-masonry-line",
    "ri-layout-right-2-fill",
    "ri-layout-right-2-line",
    "ri-layout-right-fill",
    "ri-layout-right-line",
    "ri-layout-row-fill",
    "ri-layout-row-line",
    "ri-layout-top-2-fill",
    "ri-layout-top-2-line",
    "ri-layout-top-fill",
    "ri-layout-top-line",
    "ri-leaf-fill",
    "ri-leaf-line",
    "ri-lifebuoy-fill",
    "ri-lifebuoy-line",
    "ri-lightbulb-fill",
    "ri-lightbulb-flash-fill",
    "ri-lightbulb-flash-line",
    "ri-lightbulb-line",
    "ri-line-chart-fill",
    "ri-line-chart-line",
    "ri-line-fill",
    "ri-line-height",
    "ri-line-line",
    "ri-link-m",
    "ri-link-unlink-m",
    "ri-link-unlink",
    "ri-link",
    "ri-linkedin-box-fill",
    "ri-linkedin-box-line",
    "ri-linkedin-fill",
    "ri-linkedin-line",
    "ri-links-fill",
    "ri-links-line",
    "ri-list-check-2",
    "ri-list-check",
    "ri-list-ordered",
    "ri-list-settings-fill",
    "ri-list-settings-line",
    "ri-list-unordered",
    "ri-live-fill",
    "ri-live-line",
    "ri-loader-2-fill",
    "ri-loader-2-line",
    "ri-loader-3-fill",
    "ri-loader-3-line",
    "ri-loader-4-fill",
    "ri-loader-4-line",
    "ri-loader-5-fill",
    "ri-loader-5-line",
    "ri-loader-fill",
    "ri-loader-line",
    "ri-lock-2-fill",
    "ri-lock-2-line",
    "ri-lock-fill",
    "ri-lock-line",
    "ri-lock-password-fill",
    "ri-lock-password-line",
    "ri-lock-unlock-fill",
    "ri-lock-unlock-line",
    "ri-login-box-fill",
    "ri-login-box-line",
    "ri-login-circle-fill",
    "ri-login-circle-line",
    "ri-logout-box-fill",
    "ri-logout-box-line",
    "ri-logout-box-r-fill",
    "ri-logout-box-r-line",
    "ri-logout-circle-fill",
    "ri-logout-circle-line",
    "ri-logout-circle-r-fill",
    "ri-logout-circle-r-line",
    "ri-luggage-cart-fill",
    "ri-luggage-cart-line",
    "ri-luggage-deposit-fill",
    "ri-luggage-deposit-line",
    "ri-lungs-fill",
    "ri-lungs-line",
    "ri-mac-fill",
    "ri-mac-line",
    "ri-macbook-fill",
    "ri-macbook-line",
    "ri-magic-fill",
    "ri-magic-line",
    "ri-mail-add-fill",
    "ri-mail-add-line",
    "ri-mail-check-fill",
    "ri-mail-check-line",
    "ri-mail-close-fill",
    "ri-mail-close-line",
    "ri-mail-download-fill",
    "ri-mail-download-line",
    "ri-mail-fill",
    "ri-mail-forbid-fill",
    "ri-mail-forbid-line",
    "ri-mail-line",
    "ri-mail-lock-fill",
    "ri-mail-lock-line",
    "ri-mail-open-fill",
    "ri-mail-open-line",
    "ri-mail-send-fill",
    "ri-mail-send-line",
    "ri-mail-settings-fill",
    "ri-mail-settings-line",
    "ri-mail-star-fill",
    "ri-mail-star-line",
    "ri-mail-unread-fill",
    "ri-mail-unread-line",
    "ri-mail-volume-fill",
    "ri-mail-volume-line",
    "ri-map-2-fill",
    "ri-map-2-line",
    "ri-map-fill",
    "ri-map-line",
    "ri-map-pin-2-fill",
    "ri-map-pin-2-line",
    "ri-map-pin-3-fill",
    "ri-map-pin-3-line",
    "ri-map-pin-4-fill",
    "ri-map-pin-4-line",
    "ri-map-pin-5-fill",
    "ri-map-pin-5-line",
    "ri-map-pin-add-fill",
    "ri-map-pin-add-line",
    "ri-map-pin-fill",
    "ri-map-pin-line",
    "ri-map-pin-range-fill",
    "ri-map-pin-range-line",
    "ri-map-pin-time-fill",
    "ri-map-pin-time-line",
    "ri-map-pin-user-fill",
    "ri-map-pin-user-line",
    "ri-mark-pen-fill",
    "ri-mark-pen-line",
    "ri-markdown-fill",
    "ri-markdown-line",
    "ri-markup-fill",
    "ri-markup-line",
    "ri-mastercard-fill",
    "ri-mastercard-line",
    "ri-mastodon-fill",
    "ri-mastodon-line",
    "ri-medal-2-fill",
    "ri-medal-2-line",
    "ri-medal-fill",
    "ri-medal-line",
    "ri-medicine-bottle-fill",
    "ri-medicine-bottle-line",
    "ri-medium-fill",
    "ri-medium-line",
    "ri-men-fill",
    "ri-men-line",
    "ri-mental-health-fill",
    "ri-mental-health-line",
    "ri-menu-2-fill",
    "ri-menu-2-line",
    "ri-menu-3-fill",
    "ri-menu-3-line",
    "ri-menu-4-fill",
    "ri-menu-4-line",
    "ri-menu-5-fill",
    "ri-menu-5-line",
    "ri-menu-add-fill",
    "ri-menu-add-line",
    "ri-menu-fill",
    "ri-menu-fold-fill",
    "ri-menu-fold-line",
    "ri-menu-line",
    "ri-menu-unfold-fill",
    "ri-menu-unfold-line",
    "ri-merge-cells-horizontal",
    "ri-merge-cells-vertical",
    "ri-message-2-fill",
    "ri-message-2-line",
    "ri-message-3-fill",
    "ri-message-3-line",
    "ri-message-fill",
    "ri-message-line",
    "ri-messenger-fill",
    "ri-messenger-line",
    "ri-meteor-fill",
    "ri-meteor-line",
    "ri-mic-2-fill",
    "ri-mic-2-line",
    "ri-mic-fill",
    "ri-mic-line",
    "ri-mic-off-fill",
    "ri-mic-off-line",
    "ri-mickey-fill",
    "ri-mickey-line",
    "ri-microscope-fill",
    "ri-microscope-line",
    "ri-microsoft-fill",
    "ri-microsoft-line",
    "ri-mind-map",
    "ri-mini-program-fill",
    "ri-mini-program-line",
    "ri-mist-fill",
    "ri-mist-line",
    "ri-money-cny-box-fill",
    "ri-money-cny-box-line",
    "ri-money-cny-circle-fill",
    "ri-money-cny-circle-line",
    "ri-money-dollar-box-fill",
    "ri-money-dollar-box-line",
    "ri-money-dollar-circle-fill",
    "ri-money-dollar-circle-line",
    "ri-money-euro-box-fill",
    "ri-money-euro-box-line",
    "ri-money-euro-circle-fill",
    "ri-money-euro-circle-line",
    "ri-money-pound-box-fill",
    "ri-money-pound-box-line",
    "ri-money-pound-circle-fill",
    "ri-money-pound-circle-line",
    "ri-moon-clear-fill",
    "ri-moon-clear-line",
    "ri-moon-cloudy-fill",
    "ri-moon-cloudy-line",
    "ri-moon-fill",
    "ri-moon-foggy-fill",
    "ri-moon-foggy-line",
    "ri-moon-line",
    "ri-more-2-fill",
    "ri-more-2-line",
    "ri-more-fill",
    "ri-more-line",
    "ri-motorbike-fill",
    "ri-motorbike-line",
    "ri-mouse-fill",
    "ri-mouse-line",
    "ri-movie-2-fill",
    "ri-movie-2-line",
    "ri-movie-fill",
    "ri-movie-line",
    "ri-music-2-fill",
    "ri-music-2-line",
    "ri-music-fill",
    "ri-music-line",
    "ri-mv-fill",
    "ri-mv-line",
    "ri-navigation-fill",
    "ri-navigation-line",
    "ri-netease-cloud-music-fill",
    "ri-netease-cloud-music-line",
    "ri-netflix-fill",
    "ri-netflix-line",
    "ri-newspaper-fill",
    "ri-newspaper-line",
    "ri-node-tree",
    "ri-notification-2-fill",
    "ri-notification-2-line",
    "ri-notification-3-fill",
    "ri-notification-3-line",
    "ri-notification-4-fill",
    "ri-notification-4-line",
    "ri-notification-badge-fill",
    "ri-notification-badge-line",
    "ri-notification-fill",
    "ri-notification-line",
    "ri-notification-off-fill",
    "ri-notification-off-line",
    "ri-npmjs-fill",
    "ri-npmjs-line",
    "ri-number-0",
    "ri-number-1",
    "ri-number-2",
    "ri-number-3",
    "ri-number-4",
    "ri-number-5",
    "ri-number-6",
    "ri-number-7",
    "ri-number-8",
    "ri-number-9",
    "ri-numbers-fill",
    "ri-numbers-line",
    "ri-nurse-fill",
    "ri-nurse-line",
    "ri-oil-fill",
    "ri-oil-line",
    "ri-omega",
    "ri-open-arm-fill",
    "ri-open-arm-line",
    "ri-open-source-fill",
    "ri-open-source-line",
    "ri-opera-fill",
    "ri-opera-line",
    "ri-order-play-fill",
    "ri-order-play-line",
    "ri-organization-chart",
    "ri-outlet-2-fill",
    "ri-outlet-2-line",
    "ri-outlet-fill",
    "ri-outlet-line",
    "ri-page-separator",
    "ri-pages-fill",
    "ri-pages-line",
    "ri-paint-brush-fill",
    "ri-paint-brush-line",
    "ri-paint-fill",
    "ri-paint-line",
    "ri-palette-fill",
    "ri-palette-line",
    "ri-pantone-fill",
    "ri-pantone-line",
    "ri-paragraph",
    "ri-parent-fill",
    "ri-parent-line",
    "ri-parentheses-fill",
    "ri-parentheses-line",
    "ri-parking-box-fill",
    "ri-parking-box-line",
    "ri-parking-fill",
    "ri-parking-line",
    "ri-passport-fill",
    "ri-passport-line",
    "ri-patreon-fill",
    "ri-patreon-line",
    "ri-pause-circle-fill",
    "ri-pause-circle-line",
    "ri-pause-fill",
    "ri-pause-line",
    "ri-pause-mini-fill",
    "ri-pause-mini-line",
    "ri-paypal-fill",
    "ri-paypal-line",
    "ri-pen-nib-fill",
    "ri-pen-nib-line",
    "ri-pencil-fill",
    "ri-pencil-line",
    "ri-pencil-ruler-2-fill",
    "ri-pencil-ruler-2-line",
    "ri-pencil-ruler-fill",
    "ri-pencil-ruler-line",
    "ri-percent-fill",
    "ri-percent-line",
    "ri-phone-camera-fill",
    "ri-phone-camera-line",
    "ri-phone-fill",
    "ri-phone-find-fill",
    "ri-phone-find-line",
    "ri-phone-line",
    "ri-phone-lock-fill",
    "ri-phone-lock-line",
    "ri-picture-in-picture-2-fill",
    "ri-picture-in-picture-2-line",
    "ri-picture-in-picture-exit-fill",
    "ri-picture-in-picture-exit-line",
    "ri-picture-in-picture-fill",
    "ri-picture-in-picture-line",
    "ri-pie-chart-2-fill",
    "ri-pie-chart-2-line",
    "ri-pie-chart-box-fill",
    "ri-pie-chart-box-line",
    "ri-pie-chart-fill",
    "ri-pie-chart-line",
    "ri-pin-distance-fill",
    "ri-pin-distance-line",
    "ri-ping-pong-fill",
    "ri-ping-pong-line",
    "ri-pinterest-fill",
    "ri-pinterest-line",
    "ri-pinyin-input",
    "ri-pixelfed-fill",
    "ri-pixelfed-line",
    "ri-plane-fill",
    "ri-plane-line",
    "ri-plant-fill",
    "ri-plant-line",
    "ri-play-circle-fill",
    "ri-play-circle-line",
    "ri-play-fill",
    "ri-play-line",
    "ri-play-list-2-fill",
    "ri-play-list-2-line",
    "ri-play-list-add-fill",
    "ri-play-list-add-line",
    "ri-play-list-fill",
    "ri-play-list-line",
    "ri-play-mini-fill",
    "ri-play-mini-line",
    "ri-playstation-fill",
    "ri-playstation-line",
    "ri-plug-2-fill",
    "ri-plug-2-line",
    "ri-plug-fill",
    "ri-plug-line",
    "ri-polaroid-2-fill",
    "ri-polaroid-2-line",
    "ri-polaroid-fill",
    "ri-polaroid-line",
    "ri-police-car-fill",
    "ri-police-car-line",
    "ri-price-tag-2-fill",
    "ri-price-tag-2-line",
    "ri-price-tag-3-fill",
    "ri-price-tag-3-line",
    "ri-price-tag-fill",
    "ri-price-tag-line",
    "ri-printer-cloud-fill",
    "ri-printer-cloud-line",
    "ri-printer-fill",
    "ri-printer-line",
    "ri-product-hunt-fill",
    "ri-product-hunt-line",
    "ri-profile-fill",
    "ri-profile-line",
    "ri-projector-2-fill",
    "ri-projector-2-line",
    "ri-projector-fill",
    "ri-projector-line",
    "ri-psychotherapy-fill",
    "ri-psychotherapy-line",
    "ri-pulse-fill",
    "ri-pulse-line",
    "ri-pushpin-2-fill",
    "ri-pushpin-2-line",
    "ri-pushpin-fill",
    "ri-pushpin-line",
    "ri-qq-fill",
    "ri-qq-line",
    "ri-qr-code-fill",
    "ri-qr-code-line",
    "ri-qr-scan-2-fill",
    "ri-qr-scan-2-line",
    "ri-qr-scan-fill",
    "ri-qr-scan-line",
    "ri-question-answer-fill",
    "ri-question-answer-line",
    "ri-question-fill",
    "ri-question-line",
    "ri-question-mark",
    "ri-questionnaire-fill",
    "ri-questionnaire-line",
    "ri-quill-pen-fill",
    "ri-quill-pen-line",
    "ri-radar-fill",
    "ri-radar-line",
    "ri-radio-2-fill",
    "ri-radio-2-line",
    "ri-radio-button-fill",
    "ri-radio-button-line",
    "ri-radio-fill",
    "ri-radio-line",
    "ri-rainbow-fill",
    "ri-rainbow-line",
    "ri-rainy-fill",
    "ri-rainy-line",
    "ri-reactjs-fill",
    "ri-reactjs-line",
    "ri-record-circle-fill",
    "ri-record-circle-line",
    "ri-record-mail-fill",
    "ri-record-mail-line",
    "ri-recycle-fill",
    "ri-recycle-line",
    "ri-red-packet-fill",
    "ri-red-packet-line",
    "ri-reddit-fill",
    "ri-reddit-line",
    "ri-refresh-fill",
    "ri-refresh-line",
    "ri-refund-2-fill",
    "ri-refund-2-line",
    "ri-refund-fill",
    "ri-refund-line",
    "ri-registered-fill",
    "ri-registered-line",
    "ri-remixicon-fill",
    "ri-remixicon-line",
    "ri-remote-control-2-fill",
    "ri-remote-control-2-line",
    "ri-remote-control-fill",
    "ri-remote-control-line",
    "ri-repeat-2-fill",
    "ri-repeat-2-line",
    "ri-repeat-fill",
    "ri-repeat-line",
    "ri-repeat-one-fill",
    "ri-repeat-one-line",
    "ri-reply-all-fill",
    "ri-reply-all-line",
    "ri-reply-fill",
    "ri-reply-line",
    "ri-reserved-fill",
    "ri-reserved-line",
    "ri-rest-time-fill",
    "ri-rest-time-line",
    "ri-restart-fill",
    "ri-restart-line",
    "ri-restaurant-2-fill",
    "ri-restaurant-2-line",
    "ri-restaurant-fill",
    "ri-restaurant-line",
    "ri-rewind-fill",
    "ri-rewind-line",
    "ri-rewind-mini-fill",
    "ri-rewind-mini-line",
    "ri-rhythm-fill",
    "ri-rhythm-line",
    "ri-riding-fill",
    "ri-riding-line",
    "ri-road-map-fill",
    "ri-road-map-line",
    "ri-roadster-fill",
    "ri-roadster-line",
    "ri-robot-fill",
    "ri-robot-line",
    "ri-rocket-2-fill",
    "ri-rocket-2-line",
    "ri-rocket-fill",
    "ri-rocket-line",
    "ri-rotate-lock-fill",
    "ri-rotate-lock-line",
    "ri-rounded-corner",
    "ri-route-fill",
    "ri-route-line",
    "ri-router-fill",
    "ri-router-line",
    "ri-rss-fill",
    "ri-rss-line",
    "ri-ruler-2-fill",
    "ri-ruler-2-line",
    "ri-ruler-fill",
    "ri-ruler-line",
    "ri-run-fill",
    "ri-run-line",
    "ri-safari-fill",
    "ri-safari-line",
    "ri-safe-2-fill",
    "ri-safe-2-line",
    "ri-safe-fill",
    "ri-safe-line",
    "ri-sailboat-fill",
    "ri-sailboat-line",
    "ri-save-2-fill",
    "ri-save-2-line",
    "ri-save-3-fill",
    "ri-save-3-line",
    "ri-save-fill",
    "ri-save-line",
    "ri-scales-2-fill",
    "ri-scales-2-line",
    "ri-scales-3-fill",
    "ri-scales-3-line",
    "ri-scales-fill",
    "ri-scales-line",
    "ri-scan-2-fill",
    "ri-scan-2-line",
    "ri-scan-fill",
    "ri-scan-line",
    "ri-scissors-2-fill",
    "ri-scissors-2-line",
    "ri-scissors-cut-fill",
    "ri-scissors-cut-line",
    "ri-scissors-fill",
    "ri-scissors-line",
    "ri-screenshot-2-fill",
    "ri-screenshot-2-line",
    "ri-screenshot-fill",
    "ri-screenshot-line",
    "ri-sd-card-fill",
    "ri-sd-card-line",
    "ri-sd-card-mini-fill",
    "ri-sd-card-mini-line",
    "ri-search-2-fill",
    "ri-search-2-line",
    "ri-search-eye-fill",
    "ri-search-eye-line",
    "ri-search-fill",
    "ri-search-line",
    "ri-secure-payment-fill",
    "ri-secure-payment-line",
    "ri-seedling-fill",
    "ri-seedling-line",
    "ri-send-backward",
    "ri-send-plane-2-fill",
    "ri-send-plane-2-line",
    "ri-send-plane-fill",
    "ri-send-plane-line",
    "ri-send-to-back",
    "ri-sensor-fill",
    "ri-sensor-line",
    "ri-separator",
    "ri-server-fill",
    "ri-server-line",
    "ri-service-fill",
    "ri-service-line",
    "ri-settings-2-fill",
    "ri-settings-2-line",
    "ri-settings-3-fill",
    "ri-settings-3-line",
    "ri-settings-4-fill",
    "ri-settings-4-line",
    "ri-settings-5-fill",
    "ri-settings-5-line",
    "ri-settings-6-fill",
    "ri-settings-6-line",
    "ri-settings-fill",
    "ri-settings-line",
    "ri-shape-2-fill",
    "ri-shape-2-line",
    "ri-shape-fill",
    "ri-shape-line",
    "ri-share-box-fill",
    "ri-share-box-line",
    "ri-share-circle-fill",
    "ri-share-circle-line",
    "ri-share-fill",
    "ri-share-forward-2-fill",
    "ri-share-forward-2-line",
    "ri-share-forward-box-fill",
    "ri-share-forward-box-line",
    "ri-share-forward-fill",
    "ri-share-forward-line",
    "ri-share-line",
    "ri-shield-check-fill",
    "ri-shield-check-line",
    "ri-shield-cross-fill",
    "ri-shield-cross-line",
    "ri-shield-fill",
    "ri-shield-flash-fill",
    "ri-shield-flash-line",
    "ri-shield-keyhole-fill",
    "ri-shield-keyhole-line",
    "ri-shield-line",
    "ri-shield-star-fill",
    "ri-shield-star-line",
    "ri-shield-user-fill",
    "ri-shield-user-line",
    "ri-ship-2-fill",
    "ri-ship-2-line",
    "ri-ship-fill",
    "ri-ship-line",
    "ri-shirt-fill",
    "ri-shirt-line",
    "ri-shopping-bag-2-fill",
    "ri-shopping-bag-2-line",
    "ri-shopping-bag-3-fill",
    "ri-shopping-bag-3-line",
    "ri-shopping-bag-fill",
    "ri-shopping-bag-line",
    "ri-shopping-basket-2-fill",
    "ri-shopping-basket-2-line",
    "ri-shopping-basket-fill",
    "ri-shopping-basket-line",
    "ri-shopping-cart-2-fill",
    "ri-shopping-cart-2-line",
    "ri-shopping-cart-fill",
    "ri-shopping-cart-line",
    "ri-showers-fill",
    "ri-showers-line",
    "ri-shuffle-fill",
    "ri-shuffle-line",
    "ri-shut-down-fill",
    "ri-shut-down-line",
    "ri-side-bar-fill",
    "ri-side-bar-line",
    "ri-signal-tower-fill",
    "ri-signal-tower-line",
    "ri-signal-wifi-1-fill",
    "ri-signal-wifi-1-line",
    "ri-signal-wifi-2-fill",
    "ri-signal-wifi-2-line",
    "ri-signal-wifi-3-fill",
    "ri-signal-wifi-3-line",
    "ri-signal-wifi-error-fill",
    "ri-signal-wifi-error-line",
    "ri-signal-wifi-fill",
    "ri-signal-wifi-line",
    "ri-signal-wifi-off-fill",
    "ri-signal-wifi-off-line",
    "ri-sim-card-2-fill",
    "ri-sim-card-2-line",
    "ri-sim-card-fill",
    "ri-sim-card-line",
    "ri-single-quotes-l",
    "ri-single-quotes-r",
    "ri-sip-fill",
    "ri-sip-line",
    "ri-skip-back-fill",
    "ri-skip-back-line",
    "ri-skip-back-mini-fill",
    "ri-skip-back-mini-line",
    "ri-skip-forward-fill",
    "ri-skip-forward-line",
    "ri-skip-forward-mini-fill",
    "ri-skip-forward-mini-line",
    "ri-skull-2-fill",
    "ri-skull-2-line",
    "ri-skull-fill",
    "ri-skull-line",
    "ri-skype-fill",
    "ri-skype-line",
    "ri-slack-fill",
    "ri-slack-line",
    "ri-slice-fill",
    "ri-slice-line",
    "ri-slideshow-2-fill",
    "ri-slideshow-2-line",
    "ri-slideshow-3-fill",
    "ri-slideshow-3-line",
    "ri-slideshow-4-fill",
    "ri-slideshow-4-line",
    "ri-slideshow-fill",
    "ri-slideshow-line",
    "ri-smartphone-fill",
    "ri-smartphone-line",
    "ri-snapchat-fill",
    "ri-snapchat-line",
    "ri-snowy-fill",
    "ri-snowy-line",
    "ri-sort-asc",
    "ri-sort-desc",
    "ri-sound-module-fill",
    "ri-sound-module-line",
    "ri-soundcloud-fill",
    "ri-soundcloud-line",
    "ri-space-ship-fill",
    "ri-space-ship-line",
    "ri-space",
    "ri-spam-2-fill",
    "ri-spam-2-line",
    "ri-spam-3-fill",
    "ri-spam-3-line",
    "ri-spam-fill",
    "ri-spam-line",
    "ri-speaker-2-fill",
    "ri-speaker-2-line",
    "ri-speaker-3-fill",
    "ri-speaker-3-line",
    "ri-speaker-fill",
    "ri-speaker-line",
    "ri-spectrum-fill",
    "ri-spectrum-line",
    "ri-speed-fill",
    "ri-speed-line",
    "ri-speed-mini-fill",
    "ri-speed-mini-line",
    "ri-split-cells-horizontal",
    "ri-split-cells-vertical",
    "ri-spotify-fill",
    "ri-spotify-line",
    "ri-spy-fill",
    "ri-spy-line",
    "ri-stack-fill",
    "ri-stack-line",
    "ri-stack-overflow-fill",
    "ri-stack-overflow-line",
    "ri-stackshare-fill",
    "ri-stackshare-line",
    "ri-star-fill",
    "ri-star-half-fill",
    "ri-star-half-line",
    "ri-star-half-s-fill",
    "ri-star-half-s-line",
    "ri-star-line",
    "ri-star-s-fill",
    "ri-star-s-line",
    "ri-star-smile-fill",
    "ri-star-smile-line",
    "ri-steam-fill",
    "ri-steam-line",
    "ri-steering-2-fill",
    "ri-steering-2-line",
    "ri-steering-fill",
    "ri-steering-line",
    "ri-stethoscope-fill",
    "ri-stethoscope-line",
    "ri-sticky-note-2-fill",
    "ri-sticky-note-2-line",
    "ri-sticky-note-fill",
    "ri-sticky-note-line",
    "ri-stock-fill",
    "ri-stock-line",
    "ri-stop-circle-fill",
    "ri-stop-circle-line",
    "ri-stop-fill",
    "ri-stop-line",
    "ri-stop-mini-fill",
    "ri-stop-mini-line",
    "ri-store-2-fill",
    "ri-store-2-line",
    "ri-store-3-fill",
    "ri-store-3-line",
    "ri-store-fill",
    "ri-store-line",
    "ri-strikethrough-2",
    "ri-strikethrough",
    "ri-subscript-2",
    "ri-subscript",
    "ri-subtract-fill",
    "ri-subtract-line",
    "ri-subway-fill",
    "ri-subway-line",
    "ri-subway-wifi-fill",
    "ri-subway-wifi-line",
    "ri-suitcase-2-fill",
    "ri-suitcase-2-line",
    "ri-suitcase-3-fill",
    "ri-suitcase-3-line",
    "ri-suitcase-fill",
    "ri-suitcase-line",
    "ri-sun-cloudy-fill",
    "ri-sun-cloudy-line",
    "ri-sun-fill",
    "ri-sun-foggy-fill",
    "ri-sun-foggy-line",
    "ri-sun-line",
    "ri-superscript-2",
    "ri-superscript",
    "ri-surgical-mask-fill",
    "ri-surgical-mask-line",
    "ri-surround-sound-fill",
    "ri-surround-sound-line",
    "ri-survey-fill",
    "ri-survey-line",
    "ri-swap-box-fill",
    "ri-swap-box-line",
    "ri-swap-fill",
    "ri-swap-line",
    "ri-switch-fill",
    "ri-switch-line",
    "ri-sword-fill",
    "ri-sword-line",
    "ri-syringe-fill",
    "ri-syringe-line",
    "ri-t-box-fill",
    "ri-t-box-line",
    "ri-t-shirt-2-fill",
    "ri-t-shirt-2-line",
    "ri-t-shirt-air-fill",
    "ri-t-shirt-air-line",
    "ri-t-shirt-fill",
    "ri-t-shirt-line",
    "ri-table-2",
    "ri-table-alt-fill",
    "ri-table-alt-line",
    "ri-table-fill",
    "ri-table-line",
    "ri-tablet-fill",
    "ri-tablet-line",
    "ri-takeaway-fill",
    "ri-takeaway-line",
    "ri-taobao-fill",
    "ri-taobao-line",
    "ri-tape-fill",
    "ri-tape-line",
    "ri-task-fill",
    "ri-task-line",
    "ri-taxi-fill",
    "ri-taxi-line",
    "ri-taxi-wifi-fill",
    "ri-taxi-wifi-line",
    "ri-team-fill",
    "ri-team-line",
    "ri-telegram-fill",
    "ri-telegram-line",
    "ri-temp-cold-fill",
    "ri-temp-cold-line",
    "ri-temp-hot-fill",
    "ri-temp-hot-line",
    "ri-terminal-box-fill",
    "ri-terminal-box-line",
    "ri-terminal-fill",
    "ri-terminal-line",
    "ri-terminal-window-fill",
    "ri-terminal-window-line",
    "ri-test-tube-fill",
    "ri-test-tube-line",
    "ri-text-direction-l",
    "ri-text-direction-r",
    "ri-text-spacing",
    "ri-text-wrap",
    "ri-text",
    "ri-thermometer-fill",
    "ri-thermometer-line",
    "ri-thumb-down-fill",
    "ri-thumb-down-line",
    "ri-thumb-up-fill",
    "ri-thumb-up-line",
    "ri-thunderstorms-fill",
    "ri-thunderstorms-line",
    "ri-ticket-2-fill",
    "ri-ticket-2-line",
    "ri-ticket-fill",
    "ri-ticket-line",
    "ri-time-fill",
    "ri-time-line",
    "ri-timer-2-fill",
    "ri-timer-2-line",
    "ri-timer-fill",
    "ri-timer-flash-fill",
    "ri-timer-flash-line",
    "ri-timer-line",
    "ri-todo-fill",
    "ri-todo-line",
    "ri-toggle-fill",
    "ri-toggle-line",
    "ri-tools-fill",
    "ri-tools-line",
    "ri-tornado-fill",
    "ri-tornado-line",
    "ri-trademark-fill",
    "ri-trademark-line",
    "ri-traffic-light-fill",
    "ri-traffic-light-line",
    "ri-train-fill",
    "ri-train-line",
    "ri-train-wifi-fill",
    "ri-train-wifi-line",
    "ri-translate-2",
    "ri-translate",
    "ri-travesti-fill",
    "ri-travesti-line",
    "ri-treasure-map-fill",
    "ri-treasure-map-line",
    "ri-trello-fill",
    "ri-trello-line",
    "ri-trophy-fill",
    "ri-trophy-line",
    "ri-truck-fill",
    "ri-truck-line",
    "ri-tumblr-fill",
    "ri-tumblr-line",
    "ri-tv-2-fill",
    "ri-tv-2-line",
    "ri-tv-fill",
    "ri-tv-line",
    "ri-twitch-fill",
    "ri-twitch-line",
    "ri-twitter-fill",
    "ri-twitter-line",
    "ri-typhoon-fill",
    "ri-typhoon-line",
    "ri-u-disk-fill",
    "ri-u-disk-line",
    "ri-ubuntu-fill",
    "ri-ubuntu-line",
    "ri-umbrella-fill",
    "ri-umbrella-line",
    "ri-underline",
    "ri-uninstall-fill",
    "ri-uninstall-line",
    "ri-unsplash-fill",
    "ri-unsplash-line",
    "ri-upload-2-fill",
    "ri-upload-2-line",
    "ri-upload-cloud-2-fill",
    "ri-upload-cloud-2-line",
    "ri-upload-cloud-fill",
    "ri-upload-cloud-line",
    "ri-upload-fill",
    "ri-upload-line",
    "ri-usb-fill",
    "ri-usb-line",
    "ri-user-2-fill",
    "ri-user-2-line",
    "ri-user-3-fill",
    "ri-user-3-line",
    "ri-user-4-fill",
    "ri-user-4-line",
    "ri-user-5-fill",
    "ri-user-5-line",
    "ri-user-6-fill",
    "ri-user-6-line",
    "ri-user-add-fill",
    "ri-user-add-line",
    "ri-user-fill",
    "ri-user-follow-fill",
    "ri-user-follow-line",
    "ri-user-heart-fill",
    "ri-user-heart-line",
    "ri-user-line",
    "ri-user-location-fill",
    "ri-user-location-line",
    "ri-user-received-2-fill",
    "ri-user-received-2-line",
    "ri-user-received-fill",
    "ri-user-received-line",
    "ri-user-search-fill",
    "ri-user-search-line",
    "ri-user-settings-fill",
    "ri-user-settings-line",
    "ri-user-shared-2-fill",
    "ri-user-shared-2-line",
    "ri-user-shared-fill",
    "ri-user-shared-line",
    "ri-user-smile-fill",
    "ri-user-smile-line",
    "ri-user-star-fill",
    "ri-user-star-line",
    "ri-user-unfollow-fill",
    "ri-user-unfollow-line",
    "ri-user-voice-fill",
    "ri-user-voice-line",
    "ri-video-add-fill",
    "ri-video-add-line",
    "ri-video-chat-fill",
    "ri-video-chat-line",
    "ri-video-download-fill",
    "ri-video-download-line",
    "ri-video-fill",
    "ri-video-line",
    "ri-video-upload-fill",
    "ri-video-upload-line",
    "ri-vidicon-2-fill",
    "ri-vidicon-2-line",
    "ri-vidicon-fill",
    "ri-vidicon-line",
    "ri-vimeo-fill",
    "ri-vimeo-line",
    "ri-vip-crown-2-fill",
    "ri-vip-crown-2-line",
    "ri-vip-crown-fill",
    "ri-vip-crown-line",
    "ri-vip-diamond-fill",
    "ri-vip-diamond-line",
    "ri-vip-fill",
    "ri-vip-line",
    "ri-virus-fill",
    "ri-virus-line",
    "ri-visa-fill",
    "ri-visa-line",
    "ri-voice-recognition-fill",
    "ri-voice-recognition-line",
    "ri-voiceprint-fill",
    "ri-voiceprint-line",
    "ri-volume-down-fill",
    "ri-volume-down-line",
    "ri-volume-mute-fill",
    "ri-volume-mute-line",
    "ri-volume-off-vibrate-fill",
    "ri-volume-off-vibrate-line",
    "ri-volume-up-fill",
    "ri-volume-up-line",
    "ri-volume-vibrate-fill",
    "ri-volume-vibrate-line",
    "ri-vuejs-fill",
    "ri-vuejs-line",
    "ri-walk-fill",
    "ri-walk-line",
    "ri-wallet-2-fill",
    "ri-wallet-2-line",
    "ri-wallet-3-fill",
    "ri-wallet-3-line",
    "ri-wallet-fill",
    "ri-wallet-line",
    "ri-water-flash-fill",
    "ri-water-flash-line",
    "ri-webcam-fill",
    "ri-webcam-line",
    "ri-wechat-2-fill",
    "ri-wechat-2-line",
    "ri-wechat-fill",
    "ri-wechat-line",
    "ri-wechat-pay-fill",
    "ri-wechat-pay-line",
    "ri-weibo-fill",
    "ri-weibo-line",
    "ri-whatsapp-fill",
    "ri-whatsapp-line",
    "ri-wheelchair-fill",
    "ri-wheelchair-line",
    "ri-wifi-fill",
    "ri-wifi-line",
    "ri-wifi-off-fill",
    "ri-wifi-off-line",
    "ri-window-2-fill",
    "ri-window-2-line",
    "ri-window-fill",
    "ri-window-line",
    "ri-windows-fill",
    "ri-windows-line",
    "ri-windy-fill",
    "ri-windy-line",
    "ri-wireless-charging-fill",
    "ri-wireless-charging-line",
    "ri-women-fill",
    "ri-women-line",
    "ri-wubi-input",
    "ri-xbox-fill",
    "ri-xbox-line",
    "ri-xing-fill",
    "ri-xing-line",
    "ri-youtube-fill",
    "ri-youtube-line",
    "ri-zcool-fill",
    "ri-zcool-line",
    "ri-zhihu-fill",
    "ri-zhihu-line",
    "ri-zoom-in-fill",
    "ri-zoom-in-line",
    "ri-zoom-out-fill",
    "ri-zoom-out-line",
    "ri-zzz-fill",
    "ri-zzz-line",
    "ri-arrow-down-double-fill",
    "ri-arrow-down-double-line",
    "ri-arrow-left-double-fill",
    "ri-arrow-left-double-line",
    "ri-arrow-right-double-fill",
    "ri-arrow-right-double-line",
    "ri-arrow-turn-back-fill",
    "ri-arrow-turn-back-line",
    "ri-arrow-turn-forward-fill",
    "ri-arrow-turn-forward-line",
    "ri-arrow-up-double-fill",
    "ri-arrow-up-double-line",
    "ri-bard-fill",
    "ri-bard-line",
    "ri-bootstrap-fill",
    "ri-bootstrap-line",
    "ri-box-1-fill",
    "ri-box-1-line",
    "ri-box-2-fill",
    "ri-box-2-line",
    "ri-box-3-fill",
    "ri-box-3-line",
    "ri-brain-fill",
    "ri-brain-line",
    "ri-candle-fill",
    "ri-candle-line",
    "ri-cash-fill",
    "ri-cash-line",
    "ri-contract-left-fill",
    "ri-contract-left-line",
    "ri-contract-left-right-fill",
    "ri-contract-left-right-line",
    "ri-contract-right-fill",
    "ri-contract-right-line",
    "ri-contract-up-down-fill",
    "ri-contract-up-down-line",
    "ri-copilot-fill",
    "ri-copilot-line",
    "ri-corner-down-left-fill",
    "ri-corner-down-left-line",
    "ri-corner-down-right-fill",
    "ri-corner-down-right-line",
    "ri-corner-left-down-fill",
    "ri-corner-left-down-line",
    "ri-corner-left-up-fill",
    "ri-corner-left-up-line",
    "ri-corner-right-down-fill",
    "ri-corner-right-down-line",
    "ri-corner-right-up-fill",
    "ri-corner-right-up-line",
    "ri-corner-up-left-double-fill",
    "ri-corner-up-left-double-line",
    "ri-corner-up-left-fill",
    "ri-corner-up-left-line",
    "ri-corner-up-right-double-fill",
    "ri-corner-up-right-double-line",
    "ri-corner-up-right-fill",
    "ri-corner-up-right-line",
    "ri-cross-fill",
    "ri-cross-line",
    "ri-edge-new-fill",
    "ri-edge-new-line",
    "ri-equal-fill",
    "ri-equal-line",
    "ri-expand-left-fill",
    "ri-expand-left-line",
    "ri-expand-left-right-fill",
    "ri-expand-left-right-line",
    "ri-expand-right-fill",
    "ri-expand-right-line",
    "ri-expand-up-down-fill",
    "ri-expand-up-down-line",
    "ri-flickr-fill",
    "ri-flickr-line",
    "ri-forward-10-fill",
    "ri-forward-10-line",
    "ri-forward-15-fill",
    "ri-forward-15-line",
    "ri-forward-30-fill",
    "ri-forward-30-line",
    "ri-forward-5-fill",
    "ri-forward-5-line",
    "ri-graduation-cap-fill",
    "ri-graduation-cap-line",
    "ri-home-office-fill",
    "ri-home-office-line",
    "ri-hourglass-2-fill",
    "ri-hourglass-2-line",
    "ri-hourglass-fill",
    "ri-hourglass-line",
    "ri-javascript-fill",
    "ri-javascript-line",
    "ri-loop-left-fill",
    "ri-loop-left-line",
    "ri-loop-right-fill",
    "ri-loop-right-line",
    "ri-memories-fill",
    "ri-memories-line",
    "ri-meta-fill",
    "ri-meta-line",
    "ri-microsoft-loop-fill",
    "ri-microsoft-loop-line",
    "ri-nft-fill",
    "ri-nft-line",
    "ri-notion-fill",
    "ri-notion-line",
    "ri-openai-fill",
    "ri-openai-line",
    "ri-overline",
    "ri-p2p-fill",
    "ri-p2p-line",
    "ri-presentation-fill",
    "ri-presentation-line",
    "ri-replay-10-fill",
    "ri-replay-10-line",
    "ri-replay-15-fill",
    "ri-replay-15-line",
    "ri-replay-30-fill",
    "ri-replay-30-line",
    "ri-replay-5-fill",
    "ri-replay-5-line",
    "ri-school-fill",
    "ri-school-line",
    "ri-shining-2-fill",
    "ri-shining-2-line",
    "ri-shining-fill",
    "ri-shining-line",
    "ri-sketching",
    "ri-skip-down-fill",
    "ri-skip-down-line",
    "ri-skip-left-fill",
    "ri-skip-left-line",
    "ri-skip-right-fill",
    "ri-skip-right-line",
    "ri-skip-up-fill",
    "ri-skip-up-line",
    "ri-slow-down-fill",
    "ri-slow-down-line",
    "ri-sparkling-2-fill",
    "ri-sparkling-2-line",
    "ri-sparkling-fill",
    "ri-sparkling-line",
    "ri-speak-fill",
    "ri-speak-line",
    "ri-speed-up-fill",
    "ri-speed-up-line",
    "ri-tiktok-fill",
    "ri-tiktok-line",
    "ri-token-swap-fill",
    "ri-token-swap-line",
    "ri-unpin-fill",
    "ri-unpin-line",
    "ri-wechat-channels-fill",
    "ri-wechat-channels-line",
    "ri-wordpress-fill",
    "ri-wordpress-line",
    "ri-blender-fill",
    "ri-blender-line",
    "ri-emoji-sticker-fill",
    "ri-emoji-sticker-line",
    "ri-git-close-pull-request-fill",
    "ri-git-close-pull-request-line",
    "ri-instance-fill",
    "ri-instance-line",
    "ri-megaphone-fill",
    "ri-megaphone-line",
    "ri-pass-expired-fill",
    "ri-pass-expired-line",
    "ri-pass-pending-fill",
    "ri-pass-pending-line",
    "ri-pass-valid-fill",
    "ri-pass-valid-line",
    "ri-ai-generate",
    "ri-calendar-close-fill",
    "ri-calendar-close-line",
    "ri-draggable",
    "ri-font-family",
    "ri-font-mono",
    "ri-font-sans-serif",
    "ri-font-sans",
    "ri-hard-drive-3-fill",
    "ri-hard-drive-3-line",
    "ri-kick-fill",
    "ri-kick-line",
    "ri-list-check-3",
    "ri-list-indefinite",
    "ri-list-ordered-2",
    "ri-list-radio",
    "ri-openbase-fill",
    "ri-openbase-line",
    "ri-planet-fill",
    "ri-planet-line",
    "ri-prohibited-fill",
    "ri-prohibited-line",
    "ri-quote-text",
    "ri-seo-fill",
    "ri-seo-line",
    "ri-slash-commands",
    "ri-archive-2-fill",
    "ri-archive-2-line",
    "ri-inbox-2-fill",
    "ri-inbox-2-line",
    "ri-shake-hands-fill",
    "ri-shake-hands-line",
    "ri-supabase-fill",
    "ri-supabase-line",
    "ri-water-percent-fill",
    "ri-water-percent-line",
    "ri-yuque-fill",
    "ri-yuque-line",
    "ri-crosshair-2-fill",
    "ri-crosshair-2-line",
    "ri-crosshair-fill",
    "ri-crosshair-line",
    "ri-file-close-fill",
    "ri-file-close-line",
    "ri-infinity-fill",
    "ri-infinity-line",
    "ri-rfid-fill",
    "ri-rfid-line",
    "ri-slash-commands-2",
    "ri-user-forbid-fill",
    "ri-user-forbid-line",
    "ri-beer-fill",
    "ri-beer-line",
    "ri-circle-fill",
    "ri-circle-line",
    "ri-dropdown-list",
    "ri-file-image-fill",
    "ri-file-image-line",
    "ri-file-pdf-2-fill",
    "ri-file-pdf-2-line",
    "ri-file-video-fill",
    "ri-file-video-line",
    "ri-folder-image-fill",
    "ri-folder-image-line",
    "ri-folder-video-fill",
    "ri-folder-video-line",
    "ri-hexagon-fill",
    "ri-hexagon-line",
    "ri-menu-search-fill",
    "ri-menu-search-line",
    "ri-octagon-fill",
    "ri-octagon-line",
    "ri-pentagon-fill",
    "ri-pentagon-line",
    "ri-rectangle-fill",
    "ri-rectangle-line",
    "ri-robot-2-fill",
    "ri-robot-2-line",
    "ri-shapes-fill",
    "ri-shapes-line",
    "ri-square-fill",
    "ri-square-line",
    "ri-tent-fill",
    "ri-tent-line",
    "ri-threads-fill",
    "ri-threads-line",
    "ri-tree-fill",
    "ri-tree-line",
    "ri-triangle-fill",
    "ri-triangle-line",
    "ri-twitter-x-fill",
    "ri-twitter-x-line",
    "ri-verified-badge-fill",
    "ri-verified-badge-line",
    "ri-armchair-fill",
    "ri-armchair-line",
    "ri-bnb-fill",
    "ri-bnb-line",
    "ri-bread-fill",
    "ri-bread-line",
    "ri-btc-fill",
    "ri-btc-line",
    "ri-calendar-schedule-fill",
    "ri-calendar-schedule-line",
    "ri-dice-1-fill",
    "ri-dice-1-line",
    "ri-dice-2-fill",
    "ri-dice-2-line",
    "ri-dice-3-fill",
    "ri-dice-3-line",
    "ri-dice-4-fill",
    "ri-dice-4-line",
    "ri-dice-5-fill",
    "ri-dice-5-line",
    "ri-dice-6-fill",
    "ri-dice-6-line",
    "ri-dice-fill",
    "ri-dice-line",
    "ri-drinks-fill",
    "ri-drinks-line",
    "ri-equalizer-2-fill",
    "ri-equalizer-2-line",
    "ri-equalizer-3-fill",
    "ri-equalizer-3-line",
    "ri-eth-fill",
    "ri-eth-line",
    "ri-flower-fill",
    "ri-flower-line",
    "ri-glasses-2-fill",
    "ri-glasses-2-line",
    "ri-glasses-fill",
    "ri-glasses-line",
    "ri-goggles-fill",
    "ri-goggles-line",
    "ri-image-circle-fill",
    "ri-image-circle-line",
    "ri-info-i",
    "ri-money-rupee-circle-fill",
    "ri-money-rupee-circle-line",
    "ri-news-fill",
    "ri-news-line",
    "ri-robot-3-fill",
    "ri-robot-3-line",
    "ri-share-2-fill",
    "ri-share-2-line",
    "ri-sofa-fill",
    "ri-sofa-line",
    "ri-svelte-fill",
    "ri-svelte-line",
    "ri-vk-fill",
    "ri-vk-line",
    "ri-xrp-fill",
    "ri-xrp-line",
    "ri-xtz-fill",
    "ri-xtz-line",
    "ri-archive-stack-fill",
    "ri-archive-stack-line",
    "ri-bowl-fill",
    "ri-bowl-line",
    "ri-calendar-view",
    "ri-carousel-view",
    "ri-code-block",
    "ri-color-filter-fill",
    "ri-color-filter-line",
    "ri-contacts-book-3-fill",
    "ri-contacts-book-3-line",
    "ri-contract-fill",
    "ri-contract-line",
    "ri-drinks-2-fill",
    "ri-drinks-2-line",
    "ri-export-fill",
    "ri-export-line",
    "ri-file-check-fill",
    "ri-file-check-line",
    "ri-focus-mode",
    "ri-folder-6-fill",
    "ri-folder-6-line",
    "ri-folder-check-fill",
    "ri-folder-check-line",
    "ri-folder-close-fill",
    "ri-folder-close-line",
    "ri-folder-cloud-fill",
    "ri-folder-cloud-line",
    "ri-gallery-view-2",
    "ri-gallery-view",
    "ri-hand",
    "ri-import-fill",
    "ri-import-line",
    "ri-information-2-fill",
    "ri-information-2-line",
    "ri-kanban-view-2",
    "ri-kanban-view",
    "ri-list-view",
    "ri-lock-star-fill",
    "ri-lock-star-line",
    "ri-puzzle-2-fill",
    "ri-puzzle-2-line",
    "ri-puzzle-fill",
    "ri-puzzle-line",
    "ri-ram-2-fill",
    "ri-ram-2-line",
    "ri-ram-fill",
    "ri-ram-line",
    "ri-receipt-fill",
    "ri-receipt-line",
    "ri-shadow-fill",
    "ri-shadow-line",
    "ri-sidebar-fold-fill",
    "ri-sidebar-fold-line",
    "ri-sidebar-unfold-fill",
    "ri-sidebar-unfold-line",
    "ri-slideshow-view",
    "ri-sort-alphabet-asc",
    "ri-sort-alphabet-desc",
    "ri-sort-number-asc",
    "ri-sort-number-desc",
    "ri-stacked-view",
    "ri-sticky-note-add-fill",
    "ri-sticky-note-add-line",
    "ri-swap-2-fill",
    "ri-swap-2-line",
    "ri-swap-3-fill",
    "ri-swap-3-line",
    "ri-table-3",
    "ri-table-view",
    "ri-text-block",
    "ri-text-snippet",
    "ri-timeline-view",
    "ri-blogger-fill",
    "ri-blogger-line",
    "ri-chat-thread-fill",
    "ri-chat-thread-line",
    "ri-discount-percent-fill",
    "ri-discount-percent-line",
    "ri-exchange-2-fill",
    "ri-exchange-2-line",
    "ri-git-fork-fill",
    "ri-git-fork-line",
    "ri-input-field",
    "ri-progress-1-fill",
    "ri-progress-1-line",
    "ri-progress-2-fill",
    "ri-progress-2-line",
    "ri-progress-3-fill",
    "ri-progress-3-line",
    "ri-progress-4-fill",
    "ri-progress-4-line",
    "ri-progress-5-fill",
    "ri-progress-5-line",
    "ri-progress-6-fill",
    "ri-progress-6-line",
    "ri-progress-7-fill",
    "ri-progress-7-line",
    "ri-progress-8-fill",
    "ri-progress-8-line",
    "ri-remix-run-fill",
    "ri-remix-run-line",
    "ri-signpost-fill",
    "ri-signpost-line",
    "ri-time-zone-fill",
    "ri-time-zone-line",
    "ri-arrow-down-wide-fill",
    "ri-arrow-down-wide-line",
    "ri-arrow-left-wide-fill",
    "ri-arrow-left-wide-line",
    "ri-arrow-right-wide-fill",
    "ri-arrow-right-wide-line",
    "ri-arrow-up-wide-fill",
    "ri-arrow-up-wide-line",
    "ri-bluesky-fill",
    "ri-bluesky-line",
    "ri-expand-height-fill",
    "ri-expand-height-line",
    "ri-expand-width-fill",
    "ri-expand-width-line",
    "ri-forward-end-fill",
    "ri-forward-end-line",
    "ri-forward-end-mini-fill",
    "ri-forward-end-mini-line",
    "ri-friendica-fill",
    "ri-friendica-line",
    "ri-git-pr-draft-fill",
    "ri-git-pr-draft-line",
    "ri-play-reverse-fill",
    "ri-play-reverse-line",
    "ri-play-reverse-mini-fill",
    "ri-play-reverse-mini-line",
    "ri-rewind-start-fill",
    "ri-rewind-start-line",
    "ri-rewind-start-mini-fill",
    "ri-rewind-start-mini-line",
    "ri-scroll-to-bottom-fill",
    "ri-scroll-to-bottom-line",
]
