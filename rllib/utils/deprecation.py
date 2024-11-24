import inspect
import logging
from typing import Optional, Union

from ray.util import log_once
from ray.util.annotations import _mark_annotated

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
    error: Optional[Union[bool, Exception]] = None,
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

    Raises:
        ValueError: If `error=True`.
        Exception: Of type `error`, iff `error` is a sub-class of `Exception`.
    """
    msg = "`{}` has been deprecated.{}".format(
        old, (" Use `{}` instead.".format(new) if new else f" {help}" if help else "")
    )

    if error:
        if not type(error) is bool and issubclass(error, Exception):
            # error is an Exception
            raise error(msg)
        else:
            # error is a boolean, construct ValueError ourselves
            raise ValueError(msg)
    else:
        logger.warning(
            "DeprecationWarning: " + msg + " This will raise an error in the future!"
        )


def Deprecated(old=None, *, new=None, help=None, error):
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

        from ray.rllib.utils.deprecation import Deprecated
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

    def _inner(obj):
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
                )
            # Call the deprecated method/function.
            return obj(*args, **kwargs)

        # Return the patched class method/function.
        return _ctor

    # Return the prepared decorator.
    return _inner
