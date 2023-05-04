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


@DeveloperAPI
def ensure_notebook_deps(
    *deps: Iterable[Union[str, Optional[str]]],
    missing_message: Optional[str] = None,
    outdated_message: Optional[str] = None,
) -> Callable[[F], F]:
    """Generate a decorator which checks for soft dependencies.

    This decorator is meant to wrap _ipython_display_. If the dependency is not found,
    or a version is specified here and the version of the package is older than the
    specified version, the wrapped function is not executed and None is returned. If
    the dependency is missing or the version is old, a log message is displayed.

    Args:
        *deps: Iterable of (dependency name, min version (optional))
        missing_message: Message to log if missing package is found
        outdated_message: Message to log if outdated package is found

    Returns:
        Wrapped function. Guaranteed to be safe to import soft dependencies specified
        above.
    """

    def wrapper(func: F) -> F:
        @wraps(func)
        def wrapped(*args, **kwargs):
            if _has_missing(*deps, message=missing_message) or _has_outdated(
                *deps, message=outdated_message
            ):
                return None
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def _has_missing(
    *deps: Iterable[Union[str, Optional[str]]], message: Optional[str] = None
):
    missing = []
    for (lib, _) in deps:
        try:
            importlib.import_module(lib)
        except ImportError:
            missing.append(lib)

    if missing:
        if not message:
            message = f"Run `pip install {' '.join(missing)}` for rich notebook output."

        if sys.version_info < (3, 8):
            logger.warning(f"Missing packages: {missing}. {message}")
        else:
            # stacklevel=3: First level is this function, then ensure_notebook_deps,
            # then the actual function affected.
            logger.warning(f"Missing packages: {missing}. {message}", stacklevel=3)

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

        # stacklevel=3: First level is this function, then ensure_notebook_deps, then
        # the actual function affected.
        logger.warning(f"Outdated packages:\n{outdated_str}\n{message}", stacklevel=3)

    return outdated


@DeveloperAPI
def fallback_if_colab(func: F) -> Callable[[F], F]:
    try:
        ipython = get_ipython()
    except NameError:
        ipython = None

    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if ipython and "google.colab" not in str(ipython):
            return func(self, *args, **kwargs)
        elif hasattr(self, "__repr__"):
            return print(self.__repr__(*args, **kwargs))
        else:
            return None

    return wrapped


@DeveloperAPI
def in_notebook() -> bool:
    """Return whether we are in a Jupyter notebook."""
    try:
        class_name = get_ipython().__class__.__name__
        is_notebook = True if "Terminal" not in class_name else False
    except NameError:
        is_notebook = False
    return is_notebook
