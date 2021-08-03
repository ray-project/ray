from ray.util import log_once
from ray.rllib.utils.deprecation import deprecation_warning


def override(cls):
    """Annotation for documenting method overrides.

    Args:
        cls (type): The superclass that provides the overridden method. If this
            cls does not actually have the method, an error is raised.
    """

    def check_override(method):
        if method.__name__ not in dir(cls):
            raise NameError("{} does not override any method of {}".format(
                method, cls))
        return method

    return check_override


def PublicAPI(obj):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of RLlib. You
    can expect these APIs to remain stable across RLlib releases.

    Subclasses that inherit from a ``@PublicAPI`` base class can be
    assumed part of the RLlib public API as well (e.g., all trainer classes
    are in public API because Trainer is ``@PublicAPI``).

    In addition, you can assume all trainer configurations are part of their
    public API as well.
    """

    return obj


def DeveloperAPI(obj):
    """Annotation for documenting developer APIs.

    Developer APIs are classes and methods explicitly exposed to developers
    for the purposes of building custom algorithms or advanced training
    strategies on top of RLlib internals. You can generally expect these APIs
    to be stable sans minor changes (but less stable than public APIs).

    Subclasses that inherit from a ``@DeveloperAPI`` base class can be
    assumed part of the RLlib developer API as well.
    """

    return obj


def Deprecated(old=None, *, new=None, help=None, error):
    """Annotation for documenting a (soon-to-be) deprecated method.

    Methods tagged with this decorator should produce a
    `ray.rllib.utils.deprecation.deprecation_warning(old=..., error=False)`
    to not break existing code at this point.
    In a next major release, this warning can then be made an error
    (error=True), which means at this point that the method is already
    no longer supported but will still inform the user about the
    deprecation event.
    In a further major release, the method should be erased.
    """

    def _inner(obj):
        def _ctor(*args, **kwargs):
            if log_once(old or obj.__name__):
                deprecation_warning(
                    old=old or obj.__name__,
                    new=new,
                    help=help,
                    error=error,
                )
            return obj(*args, **kwargs)

        return _ctor

    return _inner
