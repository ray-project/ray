import logging
import logging.handlers
import urllib
import urllib.request
from pathlib import Path
from queue import Queue
from urllib.parse import urlparse
import yaml
import requests
from preprocess_github_markdown import preprocess_github_markdown_file
from sphinx.util import logging as sphinx_logging
from sphinx.util.console import red  # type: ignore

__all__ = [
    "DownloadAndPreprocessEcosystemDocs",
    "update_context",
    "LinkcheckSummarizer",
    "build_gallery",
]

GALLERIES = ["ray-overview/eco-gallery.yml"]

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
        self.base_path = Path(base_path).absolute()
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


def build_gallery(app):
    for gallery in GALLERIES:
        panel_items = []
        source = yaml.safe_load((Path(app.srcdir) / gallery).read_text())

        meta = source["meta"]
        grid = meta.pop("grid")
        projects = source["projects"]
        classes = source["classes"]

        for item in projects:
            ref = "button-link"
            website = item["website"]
            if "://" not in website:  # if it has no http/s protocol, it's a "ref"
                ref = ref.replace("link", "ref")

            if not item.get("image"):
                item["image"] = "https://docs.ray.io/_images/ray_logo.png"
            gh_stars = ""
            if item["repo"]:
                try:
                    url = urlparse(item["repo"])
                    if url.netloc == "github.com":
                        _, org, repo = url.path.rstrip("/").split("/")
                        gh_stars = (
                            f".. image:: https://img.shields.io/github/"
                            f"stars/{org}/{repo}?style=social)]\n"
                            f"\t\t\t:target: {item['repo']}"
                        )
                except Exception:
                    pass

            item = f"""
    .. grid-item-card::
        :img-top: {item["image"]}
        :class-img-top: {classes["class-img-top"]}

        {gh_stars}

        {item["description"]}

        +++
        .. {ref}:: {item["website"]}
            :color: primary
            :outline:
            :expand:

            {item["name"]}
            """

            panel_items.append(item)

        panel_header = f".. grid:: {grid}\n"
        for k, v in meta.items():
            panel_header += f"    :{k}: {v}\n"

        panel_items = "\n".join(panel_items)
        panels = panel_header + panel_items

        gallery_out = gallery.replace(".yml", ".txt")
        (Path(app.srcdir) / gallery_out).write_text(panels)
