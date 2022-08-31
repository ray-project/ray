import logging
import logging.handlers
import sys
import urllib
import urllib.request
from pathlib import Path
from queue import Queue

import requests
import scipy.linalg  # noqa: F401

# Note: the scipy import has to stay here, it's used implicitly down the line
import scipy.stats  # noqa: F401
from preprocess_github_markdown import preprocess_github_markdown_file
from sphinx.util import logging as sphinx_logging
from sphinx.util.console import red  # type: ignore

import mock

__all__ = [
    "DownloadAndPreprocessEcosystemDocs",
    "mock_modules",
    "update_context",
    "LinkcheckSummarizer",
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


MOCK_MODULES = [
    "ax",
    "ax.service.ax_client",
    "ConfigSpace",
    "dask.distributed",
    "datasets",
    "datasets.iterable_dataset",
    "gym",
    "gym.spaces",
    "horovod",
    "horovod.runner",
    "horovod.runner.common",
    "horovod.runner.common.util",
    "horovod.ray",
    "horovod.ray.runner",
    "horovod.ray.utils",
    "horovod.torch",
    "hyperopt",
    "hyperopt.hp" "kubernetes",
    "mlflow",
    "modin",
    "mxnet",
    "mxnet.model",
    "optuna",
    "optuna.distributions",
    "optuna.samplers",
    "optuna.trial",
    "psutil",
    "ray._raylet",
    "ray.core.generated",
    "ray.core.generated.common_pb2",
    "ray.core.generated.runtime_env_common_pb2",
    "ray.core.generated.gcs_pb2",
    "ray.core.generated.logging_pb2",
    "ray.core.generated.ray.protocol.Task",
    "ray.serve.generated",
    "ray.serve.generated.serve_pb2",
    "scipy.signal",
    "scipy.stats",
    "setproctitle",
    "tensorflow_probability",
    "tensorflow.contrib",
    "tensorflow.contrib.all_reduce",
    "tensorflow.contrib.all_reduce.python",
    "tensorflow.contrib.layers",
    "tensorflow.contrib.rnn",
    "tensorflow.contrib.slim",
    "tree",
    "wandb",
    "zoopt",
]


def make_typing_mock(module, name):
    class Object:
        pass

    Object.__module__ = module
    Object.__qualname__ = name
    Object.__name__ = name

    return Object


def mock_modules():
    for mod_name in MOCK_MODULES:
        mock_module = mock.MagicMock()
        mock_module.__spec__ = mock.MagicMock()
        sys.modules[mod_name] = mock_module

    sys.modules["ray._raylet"].ObjectRef = make_typing_mock("ray", "ObjectRef")


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
