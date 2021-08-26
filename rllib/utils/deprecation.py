import logging
from typing import Optional, Union

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
            throw. If True, throw ValueError. If False, just warn.
            If Exception, throw that Exception.
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
