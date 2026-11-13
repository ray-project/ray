"""Deprecation aliases for ``ray.air`` public paths, which are removed in Ray 3.0.

A module ``__getattr__`` only runs for names that the module doesn't define, so a
deprecated name is kept out of the module's globals and served by the
``__getattr__`` that :func:`make_deprecated_getattr` builds.
"""

import importlib
import warnings
from typing import Any, Callable, Dict, NamedTuple, Tuple

from ray.util.annotations import RayDeprecationWarning

AIR_REMOVAL_VERSION = "3.0"


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


def deprecation_message(old_path: str, alias: DeprecatedAlias) -> str:
    replacements = " or ".join(
        f"`{path}` {when}".rstrip() for path, when in alias.replacements
    )
    message = (
        f"`{old_path}` is deprecated and will be removed in Ray "
        f"{AIR_REMOVAL_VERSION}. Use {replacements} instead."
    )
    if alias.note:
        message += f" {alias.note}"
    return message


def _resolve(target: str) -> Any:
    module_name, attr = target.split(":")
    return getattr(importlib.import_module(module_name), attr)


def make_deprecated_getattr(
    module_name: str, aliases: Dict[str, DeprecatedAlias]
) -> Callable[[str], Any]:
    """Build a module ``__getattr__`` that warns when a deprecated name is used.

    The object isn't cached in the module's globals, so every access goes through
    the warnings machinery, which decides whether to show it again.
    """

    def __getattr__(name: str) -> Any:
        alias = aliases.get(name)
        if alias is None:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
        warnings.warn(
            deprecation_message(f"{module_name}.{name}", alias),
            RayDeprecationWarning,
            stacklevel=2,
        )
        return _resolve(alias.target)

    return __getattr__
