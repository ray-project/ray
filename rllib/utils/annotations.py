from ray.rllib.utils.deprecation import Deprecated


def override(cls):
    """Decorator for documenting method overrides.

    Args:
        cls (type): The superclass that provides the overridden method. If this
            cls does not actually have the method, an error is raised.

    Examples:
        >>> class TorchPolicy(Policy):
        ... ...
        ...     # Indicates that `TorchPolicy.loss()` overrides the parent
        ...     # Policy class' own `loss method. Leads to an error if Policy
        ...     # does not have a `loss` method.
        ...     @override(Policy)
        ...     def loss(self, model, action_dist, train_batch):
        ...         # ...
    """

    def check_override(method):
        if method.__name__ not in dir(cls):
            raise NameError("{} does not override any method of {}".format(
                method, cls))
        return method

    return check_override


def PublicAPI(obj):
    """Decorator for documenting public APIs.

    Public APIs are classes and methods exposed to end users of RLlib. You
    can expect these APIs to remain stable across RLlib releases.

    Subclasses that inherit from a ``@PublicAPI`` base class can be
    assumed part of the RLlib public API as well (e.g., all trainer classes
    are in public API because Trainer is ``@PublicAPI``).

    In addition, you can assume all trainer configurations are part of their
    public API as well.

    Examples:
        >>> # Indicates that the `Trainer` class is exposed to end users
        ... # of RLlib and will remain stable across RLlib releases.
        ... @PublicAPI
        ... class Trainer(tune.Trainable):
        ... ...
    """

    return obj


def DeveloperAPI(obj):
    """Decorator for documenting developer APIs.

    Developer APIs are classes and methods explicitly exposed to developers
    for the purposes of building custom algorithms or advanced training
    strategies on top of RLlib internals. You can generally expect these APIs
    to be stable sans minor changes (but less stable than public APIs).

    Subclasses that inherit from a ``@DeveloperAPI`` base class can be
    assumed part of the RLlib developer API as well.

    Examples:
        >>> # Indicates that the `TorchPolicy` class is exposed to end users
        ... # of RLlib and will remain (relatively) stable across RLlib
        ... # releases.
        ... @DeveloperAPI
        ... class TorchPolicy(Policy):
        ... ...
    """

    return obj


def ExperimentalAPI(obj):
    """Decorator for documenting experimental APIs.

    Experimental APIs are classes and methods that are in development and may
    change at any time in their development process. You should not expect
    these APIs to be stable until their tag is changed to `DeveloperAPI` or
    `PublicAPI`.

    Subclasses that inherit from a ``@ExperimentalAPI`` base class can be
    assumed experimental as well.

    Examples:
        >>> class TorchPolicy(Policy):
        ...     ...
        ...     # Indicates that the `TorchPolicy.loss` method is a new and
        ...     # experimental API and may change frequently in future
        ...     # releases.
        ...     @ExperimentalAPI
        ...     def loss(self, model, action_dist, train_batch):
        ...         # ...
    """

    return obj


def OverrideToImplementCustomLogic(obj):
    """Users should override this in their sub-classes to implement custom logic.

    Used in Trainer and Policy to tag methods that need overriding, e.g.
    `Policy.loss()`.

    Examples:
        >>> @overrides(TorchPolicy)
        ... @OverrideToImplementCustomLogic
        ... def loss(self, ...):
        ...     # implement custom loss function here ...
        ...     # ... w/o calling the corresponding `super().loss()` method.
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
