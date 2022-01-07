def PublicAPI(*args, **kwargs):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray. You
    can expect these APIs to remain backwards compatible across minor Ray
    releases (e.g., Ray 1.4 -> 1.8).

    Args:
        stability: Either "stable" for stable features or "beta" for APIs that
            are intended to be public but still in beta.

    Examples:
        >>> @PublicAPI
        >>> def func(x):
        >>>     return x

        >>> @PublicAPI(stability="beta")
        >>> def func(y):
        >>>     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI(stability="stable")(args[0])

    if "stability" in kwargs:
        stability = kwargs["stability"]
        assert stability in ["stable", "beta"], stability
    elif kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))
    else:
        stability = "stable"

    def wrap(obj):
        if not obj.__doc__:
            obj.__doc__ = ""
        if stability == "beta":
            obj.__doc__ += (
                "\n    PublicAPI (beta): This API is in beta and may change "
                "before becoming stable.")
        else:
            obj.__doc__ += (
                "\n    PublicAPI: This API is stable across Ray releases.")
        return obj

    return wrap


def DeveloperAPI(obj):
    """Annotation for documenting developer APIs.

    Developer APIs are lower-level methods explicitly exposed to advanced Ray
    users and library developers. Their interfaces may change across minor
    Ray releases.

    Examples:
        >>> @DeveloperAPI
        >>> def func(x):
        >>>     return x
    """

    if not obj.__doc__:
        obj.__doc__ = ""
    obj.__doc__ += (
        "\n    DeveloperAPI: This API may change across minor Ray releases.")
    return obj


def Deprecated(*args, **kwargs):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.

    Args:
        message: a message to help users understand the reason for the
            deprecation, and provide a migration path.

    Examples:
        >>> @Deprecated
        >>> def func(x):
        >>>     return x

        >>> @Deprecated(message="g() is deprecated because the API is error "
        "prone. Please call h() instead.")
        >>> def g(y):
        >>>     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return Deprecated()(args[0])

    message = "\n    DEPRECATED: This API is deprecated and may be removed " \
              "in future Ray releases."
    if "message" in kwargs:
        message = message + " " + kwargs["message"]
        del kwargs["message"]

    if kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))

    def inner(obj):
        if not obj.__doc__:
            obj.__doc__ = ""
        obj.__doc__ += f"{message}"
        return obj

    return inner
