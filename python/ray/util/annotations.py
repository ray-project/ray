def PublicAPI(*args, **kwargs):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray.

    If ``stability="alpha"``, the API can be used by advanced users who are
    tolerant to and expect breaking changes.

    If ``stability="beta"``, the API is still public and can be used by early
    users, but are subject to change.

    If ``stability="stable"``, the APIs will remain backwards compatible across
    minor Ray releases (e.g., Ray 1.4 -> 1.8).

    For a full definition of the stability levels, please refer to the
    :ref:`Ray API Stability definitions <api-stability>`.

    Args:
        stability: One of {"stable", "beta", "alpha"}.

    Examples:
        >>> from ray.util.annotations import PublicAPI
        >>> @PublicAPI
        ... def func(x):
        ...     return x

        >>> @PublicAPI(stability="beta")
        ... def func(y):
        ...     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI(stability="stable")(args[0])

    if "stability" in kwargs:
        stability = kwargs["stability"]
        assert stability in ["stable", "beta", "alpha"], stability
    elif kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))
    else:
        stability = "stable"

    def wrap(obj):
        if not obj.__doc__:
            obj.__doc__ = ""
        if stability in ["alpha", "beta"]:
            obj.__doc__ += (
                f"\n    PublicAPI ({stability}): This API is in {stability} "
                "and may change before becoming stable."
            )
        else:
            obj.__doc__ += "\n    PublicAPI: This API is stable across Ray releases."

        _mark_annotated(obj)
        return obj

    return wrap


def DeveloperAPI(obj):
    """Annotation for documenting developer APIs.

    Developer APIs are lower-level methods explicitly exposed to advanced Ray
    users and library developers. Their interfaces may change across minor
    Ray releases.

    Examples:
        >>> from ray.util.annotations import DeveloperAPI
        >>> @DeveloperAPI
        ... def func(x):
        ...     return x
    """

    if not obj.__doc__:
        obj.__doc__ = ""
    obj.__doc__ += "\n    DeveloperAPI: This API may change across minor Ray releases."
    _mark_annotated(obj)
    return obj


def Deprecated(*args, **kwargs):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.

    Args:
        message: a message to help users understand the reason for the
            deprecation, and provide a migration path.

    Examples:
        >>> from ray.util.annotations import Deprecated
        >>> @Deprecated
        ... def func(x):
        ...     return x

        >>> @Deprecated(message="g() is deprecated because the API is error "
        ...   "prone. Please call h() instead.")
        ... def g(y):
        ...     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return Deprecated()(args[0])

    message = (
        "\n    DEPRECATED: This API is deprecated and may be removed "
        "in future Ray releases."
    )
    if "message" in kwargs:
        message = message + " " + kwargs["message"]
        del kwargs["message"]

    if kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))

    def inner(obj):
        if not obj.__doc__:
            obj.__doc__ = ""
        obj.__doc__ += f"{message}"
        _mark_annotated(obj)
        return obj

    return inner


def _mark_annotated(obj) -> None:
    # Set magic token for check_api_annotations linter.
    if hasattr(obj, "__name__"):
        obj._annotated = obj.__name__


def _is_annotated(obj) -> bool:
    # Check the magic token exists and applies to this class (not a subclass).
    return hasattr(obj, "_annotated") and obj._annotated == obj.__name__
