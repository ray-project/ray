"""How the Modal-compatible exceptions relate to Python's builtin ones.

Modal's errors derive from ``Exception`` through ``modal.exception.Error`` and
from no builtin exception besides: its ``TimeoutError`` is not the builtin
``TimeoutError``, nor its ``ConnectionError`` the builtin ``ConnectionError``.
Matching that keeps a ported ``except`` clause catching what it caught on
Modal, and nothing more.
"""

import inspect
import sys

import pytest

from ray.experimental.sandbox.modal import exception

# Exported classes that do derive from builtins beyond Exception, and which.
EXPECTED_BUILTIN_BASES = {
    # A warning, as upstream's is.
    "AsyncUsageWarning": {"UserWarning", "Warning"},
    # Ray's own refusal of a Modal feature, with no Modal counterpart: a
    # NotImplementedError on purpose, so code that already catches that still
    # does.
    "NotSupportedError": {"NotImplementedError", "RuntimeError"},
}


def _exported_classes():
    return sorted(
        name
        for name in exception.__all__
        if inspect.isclass(getattr(exception, name))
        and issubclass(getattr(exception, name), BaseException)
    )


def _builtin_bases(cls):
    """The builtin classes in ``cls``'s MRO besides those every error has."""
    return {
        base.__name__
        for base in cls.__mro__
        if base.__module__ == "builtins"
        and base not in (object, BaseException, Exception)
    }


@pytest.mark.parametrize("name", _exported_classes())
def test_only_the_expected_classes_carry_builtin_bases(name):
    expected = EXPECTED_BUILTIN_BASES.get(name, set())
    assert _builtin_bases(getattr(exception, name)) == expected


def test_builtin_bases_match_modal():
    upstream = pytest.importorskip("modal.exception")
    shared = [name for name in _exported_classes() if hasattr(upstream, name)]
    assert shared
    mismatched = {
        name: (
            _builtin_bases(getattr(exception, name)),
            _builtin_bases(getattr(upstream, name)),
        )
        for name in shared
        if _builtin_bases(getattr(exception, name))
        != _builtin_bases(getattr(upstream, name))
    }
    assert not mismatched


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
