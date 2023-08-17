import importlib
import logging
import sys
import textwrap
from functools import wraps
from typing import Any, Callable, Iterable, Optional, TypeVar, Union

from packaging.version import Version

from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.util.annotations import DeveloperAPI
from ray.widgets import Template

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@DeveloperAPI
def make_table_html_repr(
    obj: Any, title: Optional[str] = None, max_height: str = "none"
) -> str:
    """Generate a generic html repr using a table.

    Args:
        obj: Object for which a repr is to be generated
        title: If present, a title for the section is included
        max_height: Maximum height of the table; valid values
            are given by the max-height CSS property

    Returns:
        HTML representation of the object
    """
    data = {}
    for k, v in vars(obj).items():
        if isinstance(v, (str, bool, int, float)):
            data[k] = str(v)

        elif isinstance(v, dict) or hasattr(v, "__dict__"):
            data[k] = Template("scrollableTable.html.j2").render(
                table=tabulate(
                    v.items() if isinstance(v, dict) else vars(v).items(),
                    tablefmt="html",
                    showindex=False,
                    headers=["Setting", "Value"],
                ),
                max_height="none",
            )

    table = Template("scrollableTable.html.j2").render(
        table=tabulate(
            data.items(),
            tablefmt="unsafehtml",
            showindex=False,
            headers=["Setting", "Value"],
        ),
        max_height=max_height,
    )

    if title:
        content = Template("title_data.html.j2").render(title=title, data=table)
    else:
        content = table

    return content


def _has_missing(
    *deps: Iterable[Union[str, Optional[str]]], message: Optional[str] = None
):
    """Return a list of missing dependencies.

    Args:
        deps: Dependencies to check for
        message: Message to be emitted if a dependency isn't found

    Returns:
        A list of dependencies which can't be found, if any
    """
    missing = []
    for (lib, _) in deps:
        if importlib.util.find_spec(lib) is None:
            missing.append(lib)

    if missing:
        if not message:
            message = f"Run `pip install {' '.join(missing)}` for rich notebook output."

        if sys.version_info < (3, 8):
            logger.info(f"Missing packages: {missing}. {message}")
        else:
            # stacklevel=3: First level is this function, then ensure_notebook_deps,
            # then the actual function affected.
            logger.info(f"Missing packages: {missing}. {message}", stacklevel=3)

    return missing


def _has_outdated(
    *deps: Iterable[Union[str, Optional[str]]], message: Optional[str] = None
):
    outdated = []
    for (lib, version) in deps:
        try:
            module = importlib.import_module(lib)
            if version and Version(module.__version__) < Version(version):
                outdated.append([lib, version, module.__version__])
        except ImportError:
            pass

    if outdated:
        outdated_strs = []
        install_args = []
        for lib, version, installed in outdated:
            outdated_strs.append(f"{lib}=={installed} found, needs {lib}>={version}")
            install_args.append(f"{lib}>={version}")

        outdated_str = textwrap.indent("\n".join(outdated_strs), "  ")
        install_str = " ".join(install_args)

        if not message:
            message = f"Run `pip install -U {install_str}` for rich notebook output."

        if sys.version_info < (3, 8):
            logger.info(f"Outdated packages:\n{outdated_str}\n{message}")
        else:
            # stacklevel=3: First level is this function, then ensure_notebook_deps,
            # then the actual function affected.
            logger.info(f"Outdated packages:\n{outdated_str}\n{message}", stacklevel=3)

    return outdated


@DeveloperAPI
def repr_with_fallback(
    *notebook_deps: Iterable[Union[str, Optional[str]]]
) -> Callable[[F], F]:
    """Decorator which strips rich notebook output from mimebundles in certain cases.

    Fallback to plaintext and don't use rich output in the following cases:
    1. In a notebook environment and the appropriate dependencies are not installed.
    2. In a ipython shell environment.
    3. In Google Colab environment.
        See https://github.com/googlecolab/colabtools/ issues/60 for more information
        about the status of this issue.

    Args:
        notebook_deps: The required dependencies and version for notebook environment.

    Returns:
        A function that returns the usual _repr_mimebundle_, unless any of the 3
        conditions above hold, in which case it returns a mimebundle that only contains
        a single text/plain mimetype.
    """
    message = (
        "Run `pip install -U ipywidgets`, then restart "
        "the notebook server for rich notebook output."
    )
    if _can_display_ipywidgets(*notebook_deps, message=message):

        def wrapper(func: F) -> F:
            @wraps(func)
            def wrapped(self, *args, **kwargs):
                return func(self, *args, **kwargs)

            return wrapped

    else:

        def wrapper(func: F) -> F:
            @wraps(func)
            def wrapped(self, *args, **kwargs):
                return {"text/plain": repr(self)}

            return wrapped

    return wrapper


def _get_ipython_shell_name() -> str:
    if "IPython" in sys.modules:
        from IPython import get_ipython

        return get_ipython().__class__.__name__
    return ""


def _can_display_ipywidgets(*deps, message) -> bool:
    # Default to safe behavior: only display widgets if running in a notebook
    # that has valid dependencies
    if in_notebook() and not (
        _has_missing(*deps, message=message) or _has_outdated(*deps, message=message)
    ):
        return True

    return False


@DeveloperAPI
def in_notebook(shell_name: Optional[str] = None) -> bool:
    """Return whether we are in a Jupyter notebook or qtconsole."""
    if not shell_name:
        shell_name = _get_ipython_shell_name()
    return shell_name == "ZMQInteractiveShell"


@DeveloperAPI
def in_ipython_shell(shell_name: Optional[str] = None) -> bool:
    """Return whether we are in a terminal running IPython"""
    if not shell_name:
        shell_name = _get_ipython_shell_name()
    return shell_name == "TerminalInteractiveShell"
