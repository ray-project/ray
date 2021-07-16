import functools


def PublicAPI(*args, **kwargs):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Ray. You
    can expect these APIs to remain backwards compatible even across major Ray
    releases.

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
        if stability == "beta":
            obj.__doc__ += (
                "\nPublicAPI (beta): This API is in beta and may change "
                "before becoming stable.")
        else:
            obj.__doc__ += (
                "\nPublicAPI: This API is stable across Ray releases.")
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

    obj.__doc__ += (
        "\nDeveloperAPI: This API may change across minor Ray releases.")
    return obj


def Deprecated(obj):
    """Annotation for documenting a deprecated API.

    Deprecated APIs may be removed in future releases of Ray.

    Examples:
        >>> @Deprecated
        >>> def func(x):
        >>>     return x
    """

    obj.__doc__ += ("\nDEPRECATED: This API is deprecated and may be "
                    "removed in future Ray releases.")
    return obj
