def PublicAPI(obj):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray. You
    can expect these APIs to remain stable even across major Ray releases.
    """

    obj.__doc__ += "\nPublicAPI: This API is stable across Ray releases."
    return obj


def DeveloperAPI(obj):
    """Annotation for documenting developer APIs.

    Developer APIs are lower-level methods explicitly exposed to advanced Ray
    users and library developers. Their interfaces may change across minor
    Ray releases.

    Over time, DeveloperAPI methods may be promoted to PublicAPI.
    """

    obj.__doc__ += (
        "\nDeveloperAPI: This API may change across minor Ray releases.")
    return obj


def Deprecated(obj):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.
    """

    obj.__doc__ += ("\nDEPRECATED: This API is deprecated and may be "
                    "removed in future Ray releases.")
    return obj
