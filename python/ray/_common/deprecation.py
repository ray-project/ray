import importlib
import inspect
import logging
import warnings
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from ray.util import log_once
from ray.util.annotations import RayDeprecationWarning, _mark_annotated

# TypeVar for preserving function/class signatures through decorators.
# Note: These decorators also accept properties, but we use Callable for the
# common case. Properties work at runtime but won't get full type inference.
F = TypeVar("F", bound=Callable[..., Any])

logger = logging.getLogger(__name__)

# A constant to use for any configuration that should be deprecated
# (to check, whether this config has actually been assigned a proper value or
# not).
DEPRECATED_VALUE = -1


def deprecation_warning(
    old: str,
    new: Optional[str] = None,
    *,
    help: Optional[str] = None,
    error: Optional[Union[bool, type[Exception]]] = None,
    stacklevel: int = 2,
) -> None:
    """Warns (via the `logger` object) or throws a deprecation warning/error.

    Args:
        old: A description of the "thing" that is to be deprecated.
        new: A description of the new "thing" that replaces it.
        help: An optional help text to tell the user, what to
            do instead of using `old`.
        error: Whether or which exception to raise. If True, raise ValueError.
            If False, just warn. If `error` is-a subclass of Exception,
            raise that Exception.
        stacklevel: The stacklevel to use for the warning message.
            Use 2 to point to where this function is called, 3+ to point
            further up the stack.

    Raises:
        ValueError: If `error=True`.
        Exception: Of type `error`, iff `error` is a sub-class of `Exception`.
    """
    msg = "`{}` has been deprecated.{}".format(
        old, (" Use `{}` instead.".format(new) if new else f" {help}" if help else "")
    )

    if error:
        if not isinstance(error, bool) and issubclass(error, Exception):
            # error is an Exception
            raise error(msg)
        else:
            # error is a boolean, construct ValueError ourselves
            raise ValueError(msg)
    else:
        logger.warning(
            "DeprecationWarning: " + msg + " This will raise an error in the future!",
            stacklevel=stacklevel,
        )


@overload
def Deprecated(
    old: None = None,
    *,
    new: Optional[str] = None,
    help: Optional[str] = None,
    error: Union[bool, type[Exception]],
) -> Callable[[F], F]:
    ...


@overload
def Deprecated(
    old: str,
    *,
    new: Optional[str] = None,
    help: Optional[str] = None,
    error: Union[bool, type[Exception]],
) -> Callable[[F], F]:
    ...


def Deprecated(
    old: Optional[str] = None,
    *,
    new: Optional[str] = None,
    help: Optional[str] = None,
    error: Union[bool, type[Exception]],
) -> Callable[[F], F]:
    """Decorator for documenting a deprecated class, method, or function.

    Automatically adds a `deprecation.deprecation_warning(old=...,
    error=False)` to not break existing code at this point to the decorated
    class' constructor, method, or function.

    In a next major release, this warning should then be made an error
    (by setting error=True), which means at this point that the
    class/method/function is no longer supported, but will still inform
    the user about the deprecation event.

    In a further major release, the class, method, function should be erased
    entirely from the codebase.


    .. testcode::
        :skipif: True

        from ray._common.deprecation import Deprecated
        # Deprecated class: Patches the constructor to warn if the class is
        # used.
        @Deprecated(new="NewAndMuchCoolerClass", error=False)
        class OldAndUncoolClass:
            ...

        # Deprecated class method: Patches the method to warn if called.
        class StillCoolClass:
            ...
            @Deprecated(new="StillCoolClass.new_and_much_cooler_method()",
                        error=False)
            def old_and_uncool_method(self, uncool_arg):
                ...

        # Deprecated function: Patches the function to warn if called.
        @Deprecated(new="new_and_much_cooler_function", error=False)
        def old_and_uncool_function(*uncool_args):
            ...
    """

    def _inner(obj: F) -> F:
        # A deprecated class.
        if inspect.isclass(obj):
            # Patch the class' init method to raise the warning/error.
            obj_init = obj.__init__

            def patched_init(*args, **kwargs):
                if log_once(old or obj.__name__):
                    deprecation_warning(
                        old=old or obj.__name__,
                        new=new,
                        help=help,
                        error=error,
                        stacklevel=3,
                    )
                return obj_init(*args, **kwargs)

            obj.__init__ = patched_init
            _mark_annotated(obj)
            # Return the patched class (with the warning/error when
            # instantiated).
            return obj

        # A deprecated class method or function.
        # Patch with the warning/error at the beginning.
        def _ctor(*args, **kwargs):
            if log_once(old or obj.__name__):
                deprecation_warning(
                    old=old or obj.__name__,
                    new=new,
                    help=help,
                    error=error,
                    stacklevel=3,
                )
            # Call the deprecated method/function.
            return obj(*args, **kwargs)

        # Only apply @wraps for actual callables, not properties/descriptors.
        # Setting __wrapped__ on a property causes inspect.unwrap() to return
        # the property, which breaks inspect.signature() in the tracing helper.
        if callable(obj):
            _ctor = wraps(obj)(_ctor)

        # Return the patched class method/function.
        return cast(F, _ctor)

    # Return the prepared decorator.
    return _inner


class DeprecatedAlias(NamedTuple):
    """A deprecated public name and where users should import it from instead."""

    # Where the object lives today, as "module:attribute". The alias returns this
    # exact object, so using the deprecated path changes nothing except the warning.
    target: str
    # Public paths the warning tells users to switch to, each with an optional
    # "when to use it" suffix, e.g. ("ray.tune.RunConfig", "with Ray Tune").
    replacements: Tuple[Tuple[str, str], ...]
    # Extra guidance appended to the message, e.g. a pointer to a migration guide.
    note: str = ""


def deprecated_alias_message(
    old_path: str, alias: DeprecatedAlias, removal_version: str
) -> str:
    """Build the warning for a deprecated import path.

    Args:
        old_path: The deprecated path, e.g. ``"ray.air.RunConfig"``.
        alias: The alias that ``old_path`` resolves to.
        removal_version: The Ray version that removes ``old_path``, e.g. ``"3.0"``.

    Returns:
        A message that names the deprecated path, the version that removes it and
        the paths to use instead, followed by the alias's note.
    """
    replacements = " or ".join(
        f"`{path}` {when}".rstrip() for path, when in alias.replacements
    )
    message = (
        f"`{old_path}` is deprecated and will be removed in Ray "
        f"{removal_version}. Use {replacements} instead."
    )
    if alias.note:
        message += f" {alias.note}"
    return message


def make_deprecated_getattr(
    module_name: str, aliases: Dict[str, DeprecatedAlias], removal_version: str
) -> Callable[[str], Any]:
    """Build a module ``__getattr__`` that warns when a deprecated name is used.

    A module ``__getattr__`` only runs for names that the module doesn't define,
    so the deprecated names must be kept out of the module's globals. The object
    isn't cached there either, so every access goes through the warnings
    machinery, which decides whether to show the warning again.

    Args:
        module_name: The ``__name__`` of the module that serves the aliases.
        aliases: The deprecated names of that module, and what each resolves to.
        removal_version: The Ray version that removes the aliases, e.g. ``"3.0"``.

    Returns:
        A function to assign to the module's ``__getattr__``. It emits a
        ``RayDeprecationWarning`` attributed to the caller and returns the
        alias's target, or raises ``AttributeError`` for any other name.
    """

    def __getattr__(name: str) -> Any:
        alias = aliases.get(name)
        if alias is None:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
        warnings.warn(
            deprecated_alias_message(f"{module_name}.{name}", alias, removal_version),
            RayDeprecationWarning,
            stacklevel=2,
        )
        module, attr = alias.target.split(":")
        return getattr(importlib.import_module(module), attr)

    return __getattr__
