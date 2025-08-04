from ci.ray_ci.doc.api import AnnotationType


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


@PublicAPI
class MockClass:
    """
    This class is used for testing purpose only. It should not be used in production.
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
