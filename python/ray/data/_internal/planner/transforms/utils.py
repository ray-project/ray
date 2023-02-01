import itertools
from typing import (
    Any,
    Union,
    Callable,
    Optional,
    Iterable,
    Dict,
    Iterator,
    Tuple,
    TypeVar,
)

import ray
from ray.data.block import CallableClass
from ray.data._internal.compute import (
    ComputeStrategy,
    ActorPoolStrategy,
    TaskPoolStrategy,
)


U = TypeVar("U")


def _pairwise(iterable: Iterable[U]) -> Iterator[Tuple[U, U]]:
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def _wrap_callable_class(
    fn: Union[Callable, CallableClass],
    compute: ComputeStrategy,
    fn_constructor_args: Optional[Iterable[Any]] = None,
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
) -> Callable:
    if isinstance(fn, CallableClass):
        if isinstance(compute, TaskPoolStrategy):
            raise ValueError(
                "``compute`` must be specified when using a callable class, "
                "and must specify the actor compute strategy. "
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )
        assert isinstance(compute, ActorPoolStrategy)

        def fn_(item: Any) -> Any:
            # Wrapper providing cached instantiation of stateful callable class
            # UDFs.
            if ray.data._cached_fn is None:
                ray.data._cached_cls = fn
                ray.data._cached_fn = fn(*fn_constructor_args, **fn_constructor_kwargs)
            else:
                # A worker is destroyed when its actor is killed, so we
                # shouldn't have any worker reuse across different UDF
                # applications (i.e. different map operators).
                assert ray.data._cached_cls == fn

        return fn_
    return fn
