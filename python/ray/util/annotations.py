def PublicAPI(obj):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray. You
    can expect these APIs to remain stable even across major Ray releases.
    """

    # TODO(ekl) it would be nice to inject PublicAPI text into the docstring
    return obj


def DeveloperAPI(obj):
    """Annotation for documenting developer APIs.

    Developer APIs are lower-level methods explicitly exposed to advanced Ray
    users and library developers. Their interfaces may change across minor
    Ray releases.

    Over time, DeveloperAPI methods may be promoted to PublicAPI.
    """

    # TODO(ekl) it would be nice to inject DeveloperAPI text into the docstring
    return obj


def Deprecated(obj):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.
    """

    # TODO(ekl) it would be nice to inject deprecation text into the docstring
    return obj
