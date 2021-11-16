import inspect
import logging
from typing import Optional, Union

from ray.util import log_once

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
        error: Optional[Union[bool, Exception]] = None) -> None:
    """Warns (via the `logger` object) or throws a deprecation warning/error.

    Args:
        old (str): A description of the "thing" that is to be deprecated.
        new (Optional[str]): A description of the new "thing" that replaces it.
        help (Optional[str]): An optional help text to tell the user, what to
            do instead of using `old`.
        error (Optional[Union[bool, Exception]]): Whether or which exception to
            raise. If True, raise ValueError. If False, just warn.
            If error is-a subclass of Exception, raise that Exception.

    Raises:
        ValueError: If `error=True`.
        Exception: Of type `error`, iff error is-a Exception subclass.
    """
    msg = "`{}` has been deprecated.{}".format(
        old, (" Use `{}` instead.".format(new) if new else f" {help}"
              if help else ""))

    if error is True:
        raise ValueError(msg)
    elif error and issubclass(error, Exception):
        raise error(msg)
    else:
        logger.warning("DeprecationWarning: " + msg +
                       " This will raise an error in the future!")


def Deprecated(old=None, *, new=None, help=None, error):
    """Annotation for documenting a (soon-to-be) deprecated method.

    Methods tagged with this decorator should produce a
    `ray.rllib.utils.deprecation.deprecation_warning(old=..., error=False)`
    to not break existing code at this point.
    In a next major release, this warning can then be made an error
    (error=True), which means at this point that the method is already
    no longer supported but will still inform the user about the
    deprecation event.
    In a further major release, the method should be erased.
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
