import inspect
import sys
import warnings
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast, overload

# TypeVar for preserving function/class signatures through decorators.
# Note: These decorators also accept properties, but we use Callable for the
# common case. Properties work at runtime but won't get full type inference.
F = TypeVar("F", bound=Callable[..., Any])


class AnnotationType(Enum):
    PUBLIC_API = "PublicAPI"
    DEVELOPER_API = "DeveloperAPI"
    DEPRECATED = "Deprecated"
    UNKNOWN = "Unknown"


@overload
def PublicAPI(obj: F) -> F:
    ...


@overload
def PublicAPI(
    *, stability: str = "stable", api_group: str = "Others"
) -> Callable[[F], F]:
    ...


def PublicAPI(*args: Any, **kwargs: Any):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray.

    If ``stability="alpha"``, the API can be used by advanced users who are
    tolerant to and expect breaking changes.

    If ``stability="beta"``, the API is still public and can be used by early
    users, but are subject to change.

    If ``stability="stable"``, the APIs will remain backwards compatible across
    minor Ray releases (e.g., Ray 1.4 -> 1.8).

    For a full definition of the stability levels, please refer to the
    :ref:`Ray API Stability definitions <api-stability>`.

    Args:
        *args: When used as a bare ``@PublicAPI`` decorator, contains the
            wrapped function or class as the single positional argument.
        **kwargs: Supported keyword arguments are ``stability`` (one of
            ``"stable"``, ``"beta"``, ``"alpha"``) and ``api_group`` (used
            only for doc rendering; APIs in the same group are grouped
            together in the API doc pages).

    Returns:
        Either the annotated object (when used as ``@PublicAPI``) or a
        decorator that annotates an object (when used as
        ``@PublicAPI(...)``).

    Examples:
        >>> from ray.util.annotations import PublicAPI
        >>> @PublicAPI
        ... def func(x):
        ...     return x

        >>> @PublicAPI(stability="beta")
        ... def func(y):
        ...     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI(stability="stable", api_group="Others")(args[0])

    if "stability" in kwargs:
        stability = kwargs["stability"]
        assert stability in ["stable", "beta", "alpha"], stability
    else:
        stability = "stable"
    api_group = kwargs.get("api_group", "Others")

    def wrap(obj: F) -> F:
        if stability in ["alpha", "beta"]:
            message = (
                f"**PublicAPI ({stability}):** This API is in {stability} "
                "and may change before becoming stable."
            )
            _append_doc(obj, message=message)

        _mark_annotated(obj, type=AnnotationType.PUBLIC_API, api_group=api_group)
        return obj

    return wrap


@overload
def DeveloperAPI(obj: F) -> F:
    ...


@overload
def DeveloperAPI() -> Callable[[F], F]:
    ...


def DeveloperAPI(*args: Any, **kwargs: Any):
    """Annotation for documenting developer APIs.

    Developer APIs are lower-level methods explicitly exposed to advanced Ray
    users and library developers. Their interfaces may change across minor
    Ray releases.

    Args:
        *args: When used as a bare ``@DeveloperAPI`` decorator, contains the
            wrapped function or class as the single positional argument.
        **kwargs: Reserved for future use; no keyword arguments are currently
            supported.

    Returns:
        Either the annotated object (when used as ``@DeveloperAPI``) or a
        decorator that annotates an object (when used as
        ``@DeveloperAPI()``).

    Examples:
        >>> from ray.util.annotations import DeveloperAPI
        >>> @DeveloperAPI
        ... def func(x):
        ...     return x
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return DeveloperAPI()(args[0])

    def wrap(obj: F) -> F:
        _append_doc(
            obj,
            message="**DeveloperAPI:** This API may change across minor Ray releases.",
        )
        _mark_annotated(obj, type=AnnotationType.DEVELOPER_API)
        return obj

    return wrap


class RayDeprecationWarning(DeprecationWarning):
    """Specialized Deprecation Warning for fine grained filtering control"""

    pass


# By default, print the first occurrence of matching warnings for
# each module where the warning is issued (regardless of line number)
if not sys.warnoptions:
    warnings.filterwarnings("module", category=RayDeprecationWarning)


@overload
def Deprecated(obj: F) -> F:
    ...


@overload
def Deprecated(*, message: str = ..., warning: bool = False) -> Callable[[F], F]:
    ...


def Deprecated(*args: Any, **kwargs: Any):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.

    Args:
        *args: When used as a bare ``@Deprecated`` decorator, contains the
            wrapped function or class as the single positional argument.
        **kwargs: Supported keyword arguments are ``message`` (a string to
            help users understand the reason for the deprecation and provide
            a migration path) and ``warning`` (whether to also emit a
            ``RayDeprecationWarning`` at runtime; defaults to ``False``).

    Returns:
        Either the annotated object (when used as ``@Deprecated``) or a
        decorator that annotates an object (when used as
        ``@Deprecated(...)``).

    Examples:
        >>> from ray.util.annotations import Deprecated
        >>> @Deprecated
        ... def func(x):
        ...     return x

        >>> @Deprecated(message="g() is deprecated because the API is error "
        ...   "prone. Please call h() instead.")
        ... def g(y):
        ...     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return Deprecated()(args[0])

    doc_message = (
        "**DEPRECATED**: This API is deprecated and may be removed "
        "in future Ray releases."
    )
    warning_message = (
        "This API is deprecated and may be removed in future Ray releases. "
        "You could suppress this warning by setting env variable "
        'PYTHONWARNINGS="ignore::DeprecationWarning"'
    )

    warning = kwargs.pop("warning", False)

    if "message" in kwargs:
        doc_message = doc_message + "\n" + kwargs["message"]
        warning_message = warning_message + "\n" + kwargs["message"]
        del kwargs["message"]

    if kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))

    def inner(obj: F) -> F:
        _append_doc(obj, message=doc_message, directive="warning")
        _mark_annotated(obj, type=AnnotationType.DEPRECATED)

        if not warning:
            return obj

        if inspect.isclass(obj):
            obj_init = obj.__init__

            def patched_init(*args, **kwargs):
                warnings.warn(warning_message, RayDeprecationWarning, stacklevel=2)
                return obj_init(*args, **kwargs)

            obj.__init__ = patched_init
            return obj
        else:
            # class method or function.
            def wrapper(*args, **kwargs):
                warnings.warn(warning_message, RayDeprecationWarning, stacklevel=2)
                return obj(*args, **kwargs)

            # Only apply @wraps for actual callables, not properties/descriptors.
            # Setting __wrapped__ on a property causes inspect.unwrap() to return
            # the property, which breaks inspect.signature() in the tracing helper.
            if callable(obj):
                wrapper = wraps(obj)(wrapper)

            return cast(F, wrapper)

    return inner


def _append_doc(obj, *, message: str, directive: Optional[str] = None) -> None:
    if not obj.__doc__:
        obj.__doc__ = ""

    obj.__doc__ = obj.__doc__.rstrip()

    indent = _get_indent(obj.__doc__)
    obj.__doc__ += "\n\n"

    if directive is not None:
        obj.__doc__ += f"{' ' * indent}.. {directive}::\n\n"

        message = message.replace("\n", "\n" + " " * (indent + 4))
        obj.__doc__ += f"{' ' * (indent + 4)}{message}"
    else:
        message = message.replace("\n", "\n" + " " * (indent + 4))
        obj.__doc__ += f"{' ' * indent}{message}"
    obj.__doc__ += f"\n{' ' * indent}"


def _get_indent(docstring: str) -> int:
    """Return the indentation level (in spaces) of the docstring body.

    Args:
        docstring: The docstring whose body indentation should be measured.

    Returns:
        The number of leading whitespace characters on the second non-empty
        line of the docstring (i.e. the indentation of the body), or 0 if
        the docstring is empty or contains only a summary line.

    Example:
        >>> def f():
        ...     '''Docstring summary.'''
        >>> f.__doc__
        'Docstring summary.'
        >>> _get_indent(f.__doc__)
        0

        >>> def g(foo):
        ...     '''Docstring summary.
        ...
        ...     Args:
        ...         foo: Does bar.
        ...     '''
        >>> g.__doc__
        'Docstring summary.\\n\\n    Args:\\n        foo: Does bar.\\n    '
        >>> _get_indent(g.__doc__)
        4

        >>> class A:
        ...     def h():
        ...         '''Docstring summary.
        ...
        ...         Returns:
        ...             None.
        ...         '''
        >>> A.h.__doc__
        'Docstring summary.\\n\\n        Returns:\\n            None.\\n        '
        >>> _get_indent(A.h.__doc__)
        8
    """
    if not docstring:
        return 0

    non_empty_lines = [line for line in docstring.splitlines() if line]
    if len(non_empty_lines) == 1:
        # Docstring contains summary only.
        return 0

    # The docstring summary isn't indented, so check the indentation of the second
    # non-empty line.
    return len(non_empty_lines[1]) - len(non_empty_lines[1].lstrip())


def _mark_annotated(
    obj, type: AnnotationType = AnnotationType.UNKNOWN, api_group="Others"
) -> None:
    # Set magic token for check_api_annotations linter.
    if hasattr(obj, "__name__"):
        obj._annotated = obj.__name__
        obj._annotated_type = type
        obj._annotated_api_group = api_group


def _is_annotated(obj) -> bool:
    # Check the magic token exists and applies to this class (not a subclass).
    return hasattr(obj, "_annotated") and obj._annotated == obj.__name__


def _get_annotation_type(obj) -> Optional[str]:
    if not _is_annotated(obj):
        return None

    return obj._annotated_type.value
