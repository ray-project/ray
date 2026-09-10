from ci.ray_ci.doc.api import _OVERRIDE_HOOK_MARKER, AnnotationType
from ci.ray_ci.doc.mock._internal import (  # noqa: F401
    MockInternalOnlyClass,
    MockReexportedClass,
)

# Mirrors a library head module such as ray.data: the declared public surface
# names a class whose implementation lives in a private module. Both classes
# above are importable from here; only MockReexportedClass is exported, so the
# pair covers the exempt and the still-flagged case.
__all__ = [
    "MockClass",
    "MockDeprecatedClass",
    "MockReexportedClass",
    "mock_function",
    "mock_w00t",
]


def PublicAPI(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI()(args[0])

    def wrap(obj):
        obj._annotated = obj.__name__
        obj._annotated_type = AnnotationType.PUBLIC_API
        return obj

    return wrap


def Deprecated(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return Deprecated()(args[0])

    def wrap(obj):
        obj._annotated = obj.__name__
        obj._annotated_type = AnnotationType.DEPRECATED
        return obj

    return wrap


def OverrideToImplementCustomLogic(obj):
    # Mirrors rllib.utils.annotations.OverrideToImplementCustomLogic: tags a
    # method as a template-method override hook by setting the override marker.
    # The API check reads the attribute generically, so the test fixture does
    # not import RLlib.
    setattr(obj, _OVERRIDE_HOOK_MARKER, False)
    return obj


@PublicAPI
class MockClass:
    """
    This class is used for testing purpose only. It should not be used in production.
    """

    def mock_method(self):
        """
        A method that is documented (for example in an autosummary) but is not
        itself annotated -- it is public by virtue of its annotated class.
        The check must accept it as long as it resolves.
        """
        pass

    @OverrideToImplementCustomLogic
    def _mock_forward(self):
        """
        A documented override hook: underscore-named but a declared public
        extension point. The check must accept it despite the leading
        underscore because it carries the override-hook marker.
        """
        pass

    def _mock_private(self):
        """
        A plain underscore-named method with no override-hook marker. The check
        must still flag it as non-public -- the exemption must not weaken
        detection of genuinely private symbols.
        """
        pass


class InheritedAnnotation(MockClass):
    """An undecorated subclass must not inherit MockClass's API annotation."""

    pass


@Deprecated
class MockDeprecatedClass:
    """
    A directly-deprecated class. Documenting it is an error the check must catch.
    """

    pass


class MockDeprecatedSubclass(MockDeprecatedClass):
    """
    An undecorated subclass of a deprecated class. It inherits ``_annotated_type``
    as a plain class attribute, so reading that attribute without an ownership
    check would classify it as deprecated and flag a documented name that nobody
    deprecated.
    """

    pass


@Deprecated
def mock_function():
    """
    This function is used for testing purpose only. It should not be used in production.
    """
    pass


@PublicAPI
def mock_w00t():
    """
    This function is used for testing purpose only. It should not be used in production.
    """
    pass
