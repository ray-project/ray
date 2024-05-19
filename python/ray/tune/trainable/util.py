import inspect
import logging
import types
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

import ray
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.tune.registry import _ParameterRegistry
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
def with_parameters(trainable: Union[Type["Trainable"], Callable], **kwargs):
    """Wrapper for trainables to pass arbitrary large data objects.

    This wrapper function will store all passed parameters in the Ray
    object store and retrieve them when calling the function. It can thus
    be used to pass arbitrary data, even datasets, to Tune trainables.

    This can also be used as an alternative to ``functools.partial`` to pass
    default arguments to trainables.

    When used with the function API, the trainable function is called with
    the passed parameters as keyword arguments. When used with the class API,
    the ``Trainable.setup()`` method is called with the respective kwargs.

    If the data already exists in the object store (are instances of
    ObjectRef), using ``tune.with_parameters()`` is not necessary. You can
    instead pass the object refs to the training function via the ``config``
    or use Python partials.

    Args:
        trainable: Trainable to wrap.
        **kwargs: parameters to store in object store.

    Function API example:

    .. code-block:: python

        from ray import train, tune

        def train_fn(config, data=None):
            for sample in data:
                loss = update_model(sample)
                train.report(loss=loss)

        data = HugeDataset(download=True)

        tuner = Tuner(
            tune.with_parameters(train_fn, data=data),
            # ...
        )
        tuner.fit()

    Class API example:

    .. code-block:: python

        from ray import tune

        class MyTrainable(tune.Trainable):
            def setup(self, config, data=None):
                self.data = data
                self.iter = iter(self.data)
                self.next_sample = next(self.iter)

            def step(self):
                loss = update_model(self.next_sample)
                try:
                    self.next_sample = next(self.iter)
                except StopIteration:
                    return {"loss": loss, done: True}
                return {"loss": loss}

        data = HugeDataset(download=True)

        tuner = Tuner(
            tune.with_parameters(MyTrainable, data=data),
            # ...
        )
    """
    from ray.tune.trainable import Trainable

    if not callable(trainable) or (
        inspect.isclass(trainable) and not issubclass(trainable, Trainable)
    ):
        raise ValueError(
            f"`tune.with_parameters() only works with function trainables "
            f"or classes that inherit from `tune.Trainable()`. Got type: "
            f"{type(trainable)}."
        )

    parameter_registry = _ParameterRegistry()
    ray._private.worker._post_init_hooks.append(parameter_registry.flush)

    # Objects are moved into the object store
    prefix = f"{str(trainable)}_"
    for k, v in kwargs.items():
        parameter_registry.put(prefix + k, v)

    trainable_name = getattr(trainable, "__name__", "tune_with_parameters")
    keys = set(kwargs.keys())

    if inspect.isclass(trainable):
        # Class trainable

        class _Inner(trainable):
            def setup(self, config):
                setup_kwargs = {}
                for k in keys:
                    setup_kwargs[k] = parameter_registry.get(prefix + k)
                super(_Inner, self).setup(config, **setup_kwargs)

        trainable_with_params = _Inner
    else:
        # Function trainable

        def inner(config):
            fn_kwargs = {}
            for k in keys:
                fn_kwargs[k] = parameter_registry.get(prefix + k)
            return trainable(config, **fn_kwargs)

        trainable_with_params = inner

        if hasattr(trainable, "__mixins__"):
            trainable_with_params.__mixins__ = trainable.__mixins__

        # If the trainable has been wrapped with `tune.with_resources`, we should
        # keep the `_resources` attribute around
        if hasattr(trainable, "_resources"):
            trainable_with_params._resources = trainable._resources

    trainable_with_params.__name__ = trainable_name
    return trainable_with_params


@PublicAPI(stability="beta")
def with_resources(
    trainable: Union[Type["Trainable"], Callable],
    resources: Union[
        Dict[str, float],
        PlacementGroupFactory,
        Callable[[dict], PlacementGroupFactory],
    ],
):
    """Wrapper for trainables to specify resource requests.

    This wrapper allows specification of resource requirements for a specific
    trainable. It will override potential existing resource requests (use
    with caution!).

    The main use case is to request resources for function trainables when used
    with the Tuner() API.

    Class trainables should usually just implement the ``default_resource_request()``
    method.

    Args:
        trainable: Trainable to wrap.
        resources: Resource dict, placement group factory, or callable that takes
            in a config dict and returns a placement group factory.

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.tuner import Tuner

        def train_fn(config):
            return len(ray.get_gpu_ids())  # Returns 2

        tuner = Tuner(
            tune.with_resources(train_fn, resources={"gpu": 2}),
            # ...
        )
        results = tuner.fit()

    """
    from ray.tune.trainable import Trainable

    if not callable(trainable) or (
        inspect.isclass(trainable) and not issubclass(trainable, Trainable)
    ):
        raise ValueError(
            f"`tune.with_resources() only works with function trainables "
            f"or classes that inherit from `tune.Trainable()`. Got type: "
            f"{type(trainable)}."
        )

    if isinstance(resources, PlacementGroupFactory):
        pgf = resources
    elif isinstance(resources, dict):
        pgf = resource_dict_to_pg_factory(resources)
    elif callable(resources):
        pgf = resources
    else:
        raise ValueError(
            f"Invalid resource type for `with_resources()`: {type(resources)}"
        )

    if not inspect.isclass(trainable):
        if isinstance(trainable, types.MethodType):
            # Methods cannot set arbitrary attributes, so we have to wrap them
            def _trainable(config):
                return trainable(config)

            _trainable._resources = pgf
            return _trainable

        # Just set an attribute. This will be resolved later in `wrap_function()`.
        try:
            trainable._resources = pgf
        except AttributeError as e:
            raise RuntimeError(
                "Could not use `tune.with_resources()` on the supplied trainable. "
                "Wrap your trainable in a regular function before passing it "
                "to Ray Tune."
            ) from e
    else:

        class ResourceTrainable(trainable):
            @classmethod
            def default_resource_request(
                cls, config: Dict[str, Any]
            ) -> Optional[PlacementGroupFactory]:
                if not isinstance(pgf, PlacementGroupFactory) and callable(pgf):
                    return pgf(config)
                return pgf

        ResourceTrainable.__name__ = trainable.__name__
        trainable = ResourceTrainable

    return trainable
