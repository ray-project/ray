from ray.rllib.utils.deprecation import Deprecated
from ray.util.annotations import _mark_annotated


def override(parent_cls):
    """Decorator for documenting method overrides.

    Args:
        parent_cls: The superclass that provides the overridden method. If
            `parent_class` does not actually have the method or the class, in which
            method is defined is not a subclass of `parent_class`, an error is raised.

    .. testcode::
        :skipif: True

        from ray.rllib.policy import Policy
        class TorchPolicy(Policy):
            ...
            # Indicates that `TorchPolicy.loss()` overrides the parent
            # Policy class' own `loss method. Leads to an error if Policy
            # does not have a `loss` method.

            @override(Policy)
            def loss(self, model, action_dist, train_batch):
                ...

    """

    class OverrideCheck:
        def __init__(self, func, expected_parent_cls):
            self.func = func
            self.expected_parent_cls = expected_parent_cls

        def __set_name__(self, owner, name):
            # Check if the owner (the class) is a subclass of the expected base class
            if not issubclass(owner, self.expected_parent_cls):
                raise TypeError(
                    f"When using the @override decorator, {owner.__name__} must be a "
                    f"subclass of {parent_cls.__name__}!"
                )
            # Set the function as a regular method on the class.
            setattr(owner, name, self.func)

    def decorator(method):
        # Check, whether `method` is actually defined by the parent class.
        if method.__name__ not in dir(parent_cls):
            raise NameError(
                f"When using the @override decorator, {method.__name__} must override "
                f"the respective method (with the same name) of {parent_cls.__name__}!"
            )

        # Check if the class is a subclass of the expected base class
        OverrideCheck(method, parent_cls)
        return method

    return decorator


def PublicAPI(obj):
    """Decorator for documenting public APIs.

    Public APIs are classes and methods exposed to end users of RLlib. You
    can expect these APIs to remain stable across RLlib releases.

    Subclasses that inherit from a ``@PublicAPI`` base class can be
    assumed part of the RLlib public API as well (e.g., all Algorithm classes
    are in public API because Algorithm is ``@PublicAPI``).

    In addition, you can assume all algo configurations are part of their
    public API as well.

    .. testcode::
        :skipif: True

        # Indicates that the `Algorithm` class is exposed to end users
        # of RLlib and will remain stable across RLlib releases.
        from ray import tune
        @PublicAPI
        class Algorithm(tune.Trainable):
            ...
    """

    _mark_annotated(obj)
    return obj


def DeveloperAPI(obj):
    """Decorator for documenting developer APIs.

    Developer APIs are classes and methods explicitly exposed to developers
    for the purposes of building custom algorithms or advanced training
    strategies on top of RLlib internals. You can generally expect these APIs
    to be stable sans minor changes (but less stable than public APIs).

    Subclasses that inherit from a ``@DeveloperAPI`` base class can be
    assumed part of the RLlib developer API as well.

    .. testcode::
        :skipif: True

        # Indicates that the `TorchPolicy` class is exposed to end users
        # of RLlib and will remain (relatively) stable across RLlib
        # releases.
        from ray.rllib.policy import Policy
        @DeveloperAPI
        class TorchPolicy(Policy):
            ...
    """

    _mark_annotated(obj)
    return obj


def ExperimentalAPI(obj):
    """Decorator for documenting experimental APIs.

    Experimental APIs are classes and methods that are in development and may
    change at any time in their development process. You should not expect
    these APIs to be stable until their tag is changed to `DeveloperAPI` or
    `PublicAPI`.

    Subclasses that inherit from a ``@ExperimentalAPI`` base class can be
    assumed experimental as well.

    .. testcode::
        :skipif: True

        from ray.rllib.policy import Policy
        class TorchPolicy(Policy):
            ...
            # Indicates that the `TorchPolicy.loss` method is a new and
            # experimental API and may change frequently in future
            # releases.
            @ExperimentalAPI
            def loss(self, model, action_dist, train_batch):
                ...
    """

    _mark_annotated(obj)
    return obj


def OldAPIStack(obj):
    """Decorator for classes/methods/functions belonging to the old API stack.

    These should be deprecated at some point after Ray 3.0 (RLlib GA).
    It is recommended for users to start exploring (and coding against) the new API
    stack instead.
    """
    # No effect yet.

    _mark_annotated(obj)
    return obj


def OverrideToImplementCustomLogic(obj):
    """Users should override this in their sub-classes to implement custom logic.

    Used in Algorithm and Policy to tag methods that need overriding, e.g.
    `Policy.loss()`.

    .. testcode::
        :skipif: True

        from ray.rllib.policy.torch_policy import TorchPolicy
        @overrides(TorchPolicy)
        @OverrideToImplementCustomLogic
        def loss(self, ...):
            # implement custom loss function here ...
            # ... w/o calling the corresponding `super().loss()` method.
            ...

    """
    obj.__is_overridden__ = False
    return obj


def OverrideToImplementCustomLogic_CallToSuperRecommended(obj):
    """Users should override this in their sub-classes to implement custom logic.

    Thereby, it is recommended (but not required) to call the super-class'
    corresponding method.

    Used in Algorithm and Policy to tag methods that need overriding, but the
    super class' method should still be called, e.g.
    `Algorithm.setup()`.

    .. testcode::
        :skipif: True

        from ray import tune
        @overrides(tune.Trainable)
        @OverrideToImplementCustomLogic_CallToSuperRecommended
        def setup(self, config):
            # implement custom setup logic here ...
            super().setup(config)
            # ... or here (after having called super()'s setup method.
    """
    obj.__is_overridden__ = False
    return obj


def is_overridden(obj):
    """Check whether a function has been overridden.

    Note, this only works for API calls decorated with OverrideToImplementCustomLogic
    or OverrideToImplementCustomLogic_CallToSuperRecommended.
    """
    return getattr(obj, "__is_overridden__", True)


# Backward compatibility.
Deprecated = Deprecated
