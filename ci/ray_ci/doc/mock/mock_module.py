from ci.ray_ci.doc.api import _OVERRIDE_HOOK_MARKER, AnnotationType


def PublicAPI(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI()(args[0])

    def wrap(obj):
        obj._annotated = None
        obj._annotated_type = AnnotationType.PUBLIC_API
        return obj

    return wrap


def Deprecated(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return Deprecated()(args[0])

    def wrap(obj):
        obj._annotated = None
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
