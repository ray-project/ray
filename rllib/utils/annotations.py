from ray.rllib.utils.deprecation import Deprecated


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


def ExperimentalAPI(obj):
    """Annotation for documenting experimental APIs.

    Experimental APIs are classes and methods that are in development and may
    change at any time in their development process. You should not expect
    these APIs to be stable until their tag is changed to `DeveloperAPI` or
    `PublicAPI`.

    Subclasses that inherit from a ``@ExperimentalAPI`` base class can be
    assumed experimental as well.
    """

    return obj


def OverrideToImplementCustomLogic(obj):
    """Users should override this in their sub-classes to implement custom logic.

    Used in Trainer and Policy to tag methods that need overriding, e.g.
    `Policy.loss()`.
    """
    return obj


def OverrideToImplementCustomLogic_CallToSuperRecommended(obj):
    """Users should override this in their sub-classes to implement custom logic.

    Thereby, it is recommended (but not required) to call the super-class'
    corresponding method.

    Used in Trainer and Policy to tag methods that need overriding, but the
    super class' method should still be called, e.g.
    `Trainer.setup()`.

    Examples:
    >>> @overrides(Trainable)
    ... @OverrideToImplementCustomLogic_CallToSuperRecommended
    ... def setup(self, config):
    ...     # implement custom setup logic here ...
    ...     super().setup(config)
    ...     # ... or here (after having called super()'s setup method.
    """
    return obj


# Backward compatibility.
Deprecated = Deprecated
