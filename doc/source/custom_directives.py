import urllib
import mock
import sys
from preprocess_github_markdown import preprocess_github_markdown_file

# Note: the scipy import has to stay here, it's used implicitly down the line
import scipy.stats  # noqa: F401
import scipy.linalg  # noqa: F401

__all__ = [
    "fix_xgb_lgbm_docs",
    "mock_modules",
    "update_context",
    "download_and_preprocess_ecosystem_docs",
]

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


def fix_xgb_lgbm_docs(app, what, name, obj, options, lines):
    """Fix XGBoost-Ray and LightGBM-Ray docstrings.

    For ``app.connect('autodoc-process-docstring')``.
    See https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

    Removes references to XGBoost ``callback_api`` and sets explicit module
    references to classes and functions that are named the same way in both
    XGBoost-Ray and LightGBM-Ray.
    """

    def _remove_xgboost_refs(replacements: list):
        """Remove ``callback_api`` ref to XGBoost docs.

        Fixes ``undefined label: callback_api (if the link has no caption
        the label must precede a section header)``
        """
        if name.startswith("xgboost_ray"):
            replacements.append((":ref:`callback_api`", "Callback API"))

    def _replace_ray_params(replacements: list):
        """Replaces references to ``RayParams`` with module-specific ones.

        Fixes ``more than one target found for cross-reference 'RayParams'``.
        """
        if name.startswith("xgboost_ray"):
            replacements.append(("RayParams", "xgboost_ray.RayParams"))
        elif name.startswith("lightgbm_ray"):
            replacements.append(("RayParams", "lightgbm_ray.RayParams"))

    replacements = []
    _remove_xgboost_refs(replacements)
    _replace_ray_params(replacements)
    if replacements:
        for i, _ in enumerate(lines):
            for replacement in replacements:
                lines[i] = lines[i].replace(*replacement)


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
    "blist",
    "ConfigSpace",
    "dask.distributed",
    "gym",
    "gym.spaces",
    "horovod",
    "horovod.runner",
    "horovod.runner.common",
    "horovod.runner.common.util",
    "horovod.ray",
    "horovod.ray.runner",
    "horovod.ray.utils",
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
    "tensorflow",
    "tensorflow.contrib",
    "tensorflow.contrib.all_reduce",
    "tree",
    "tensorflow.contrib.all_reduce.python",
    "tensorflow.contrib.layers",
    "tensorflow.contrib.rnn",
    "tensorflow.contrib.slim",
    "tensorflow.core",
    "tensorflow.core.util",
    "tensorflow.keras",
    "tensorflow.python",
    "tensorflow.python.client",
    "tensorflow.python.util",
    "torch",
    "torch.cuda.amp",
    "torch.distributed",
    "torch.nn",
    "torch.nn.parallel",
    "torch.optim",
    "torch.profiler",
    "torch.utils.data",
    "torch.utils.data.distributed",
    "wandb",
    "zoopt",
]

CHILD_MOCK_MODULES = [
    "pytorch_lightning",
    "pytorch_lightning.accelerators",
    "pytorch_lightning.plugins",
    "pytorch_lightning.plugins.environments",
    "pytorch_lightning.utilities",
    "tensorflow.keras.callbacks",
]


class ChildClassMock(mock.Mock):
    @classmethod
    def __getattr__(cls, name):
        return mock.Mock


def mock_modules():
    for mod_name in MOCK_MODULES:
        sys.modules[mod_name] = mock.Mock()

    sys.modules["tensorflow"].VERSION = "9.9.9"

    for mod_name in CHILD_MOCK_MODULES:
        sys.modules[mod_name] = ChildClassMock()


# Add doc files from external repositories to be downloaded during build here
# (repo, ref, path to get, path to save on disk)
EXTERNAL_MARKDOWN_FILES = [
    ("ray-project/xgboost_ray", "master", "README.md", "ray-more-libs/xgboost-ray.md"),
    (
        "ray-project/lightgbm_ray",
        "master",
        "README.md",
        "ray-more-libs/lightgbm-ray.md",
    ),
]


def download_and_preprocess_ecosystem_docs():
    """
    This function downloads markdown readme files for various
    ecosystem libraries, saves them in specified locations and preprocesses
    them before sphinx build starts.

    If you have ecosystem libraries that live in a separate repo from Ray,
    adding them here will allow for their docs to be present in Ray docs
    without the need for duplicate files. For more details, see ``doc/README.md``.
    """

    import urllib.request
    import requests

    def get_latest_release_tag(repo: str) -> str:
        """repo is just the repo name, eg. ray-project/ray"""
        response = requests.get(f"https://api.github.com/repos/{repo}/releases/latest")
        return response.json()["tag_name"]

    def get_file_from_github(
        repo: str, ref: str, path_to_get: str, path_to_save_on_disk: str
    ) -> None:
        """If ``ref == "latest"``, use latest release"""
        if ref == "latest":
            ref = get_latest_release_tag(repo)
        urllib.request.urlretrieve(
            f"https://raw.githubusercontent.com/{repo}/{ref}/{path_to_get}",
            path_to_save_on_disk,
        )

    for x in EXTERNAL_MARKDOWN_FILES:
        get_file_from_github(*x)
        preprocess_github_markdown_file(x[-1])
