from __future__ import annotations

from typing import TYPE_CHECKING, overload

import ray

__all__ = ["remote_method", "ActorMixin", "Actor"]

if TYPE_CHECKING:
    from typing import Any, Callable, Coroutine, Generic, TypeVar, Union

    from typing_extensions import Concatenate, Literal, ParamSpec, TypedDict, Unpack

    from ray.dag import ClassNode
    from ray.util import scheduling_strategies

    _P = ParamSpec("_P")
    _R = TypeVar("_R")

    _T = TypeVar("_T")
    _T0 = TypeVar("_T0")
    _T1 = TypeVar("_T1")
    _T2 = TypeVar("_T2")
    _T3 = TypeVar("_T3")
    _T4 = TypeVar("_T4")
    _T5 = TypeVar("_T5")
    _T6 = TypeVar("_T6")
    _T7 = TypeVar("_T7")
    _T8 = TypeVar("_T8")
    _T9 = TypeVar("_T9")

    _Method_co = TypeVar("_Method_co", covariant=True, bound=Callable)

    RemoteArg = Union[_T, ray.ObjectRef[_T]]
    RemoteRet = Union[_R, Coroutine[Any, Any, _R]]

    _ClassT = TypeVar("_ClassT")

    class Actor(Generic[_ClassT]):
        def __init__(self, actor_handle) -> None:
            ...

        @property
        def methods(self) -> type[_ClassT]:
            ...

    class ActorClass(Generic[_P, _R]):
        def __init__(self, klass: Callable[_P, _R], default_opts: ActorOptions):
            ...

        remote: ActorClassRemote[Callable[_P, _R]]

        def options(self, **opts: Unpack[ActorOptions]) -> ActorClassWrapper[_P, _R]:
            ...

        def bind(self, *args, **kwargs) -> ClassNode:
            ...

    class ActorClassWrapper(Generic[_P, _R]):
        def __init__(self, klass: Callable[_P, _R], opts: ActorOptions):
            ...

        remote: ActorClassRemote[Callable[_P, _R]]

        def bind(self, *args, **kwargs) -> ClassNode:
            ...

    class ActorClassRemote(Generic[_Method_co]):
        @overload
        def __call__(self: ActorClassRemote[Callable[[], _R]]) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[Callable[Concatenate[_T0, ...], _R]],
            __arg0: RemoteArg[_T0],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[Callable[Concatenate[_T0, _T1, ...], _R]],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[Callable[Concatenate[_T0, _T1, _T2, ...], _R]],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[Callable[Concatenate[_T0, _T1, _T2, _T3, ...], _R]],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, ...], _R]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, ...], _R]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, ...], _R]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, ...], _R]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8, ...], _R
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            __arg8: RemoteArg[_T8],
            **kwargs,
        ) -> Actor[_R]:
            ...

        @overload
        def __call__(
            self: ActorClassRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8, _T9, ...],
                    _R,
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            __arg8: RemoteArg[_T8],
            __arg9: RemoteArg[_T9],
            **kwargs,
        ) -> Actor[_R]:
            ...

        def __call__(self, *args, **kwargs) -> Actor:
            ...

    class ActorOptions(TypedDict, total=False):
        num_cpus: float
        num_gpus: float
        resources: dict[str, float]
        accelerator_type: str
        memory: float
        object_store_memory: float
        max_restarts: int
        max_task_retries: int
        max_pending_calls: int
        max_concurrency: int
        name: str
        namespace: str
        lifetime: Literal["detached"] | None
        runtime_env: dict[str, Any]
        concurrency_groups: dict[str, int]
        scheduling_strategy: scheduling_strategies.SchedulingStrategyT

    class ActorMethod(Generic[_P, _R]):
        remote: ActorMethodRemote[Callable[_P, _R]]

        def options(
            self, name: str = ..., concurrency_group: str = ...
        ) -> ActorMethodWrapper[_P, _R]:
            ...

        def bind(self, *args, **kwargs) -> ClassNode:
            ...

        def __call__(self, *args: _P.args, **kwds: _P.kwargs) -> _R:
            ...

    class ActorMethodRemote(Generic[_Method_co]):
        @overload
        def __call__(
            self: ActorMethodRemote[Callable[[], RemoteRet[_R]]], **kwargs
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[Callable[Concatenate[_T0, ...], RemoteRet[_R]]],
            __arg0: RemoteArg[_T0],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[Concatenate[_T0, _T1, ...], RemoteRet[_R]]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[Concatenate[_T0, _T1, _T2, ...], RemoteRet[_R]]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, ...], RemoteRet[_R]]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, ...], RemoteRet[_R]]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, ...], RemoteRet[_R]]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, ...], RemoteRet[_R]
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, ...],
                    RemoteRet[_R],
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8, ...],
                    RemoteRet[_R],
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            __arg8: RemoteArg[_T8],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[
                Callable[
                    Concatenate[_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8, _T9, ...],
                    RemoteRet[_R],
                ]
            ],
            __arg0: RemoteArg[_T0],
            __arg1: RemoteArg[_T1],
            __arg2: RemoteArg[_T2],
            __arg3: RemoteArg[_T3],
            __arg4: RemoteArg[_T4],
            __arg5: RemoteArg[_T5],
            __arg6: RemoteArg[_T6],
            __arg7: RemoteArg[_T7],
            __arg8: RemoteArg[_T8],
            __arg9: RemoteArg[_T9],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        @overload
        def __call__(
            self: ActorMethodRemote[Callable[..., RemoteRet[_R]]],
            **kwargs,
        ) -> ray.ObjectRef[_R]:
            ...

        def __call__(self, *args: Any, **kwds: Any) -> ray.ObjectRef:
            ...

    class ActorMethodWrapper(Generic[_P, _R]):
        remote: ActorMethodRemote[Callable[_P, _R]]

        def bind(self, *args, **kwargs) -> ClassNode:
            ...

else:

    class Actor:
        def __init__(self, actor_handle):
            self._actor_handle = actor_handle

        @property
        def methods(self):
            return self._actor_handle

    class ActorClass:
        def __init__(self, klass, default_opts):
            self._klass = klass
            self._default_opts = default_opts

        def remote(self, *args, **kwargs):
            if self._default_opts:
                handle = ray.remote(**self._default_opts)(self._klass).remote(
                    *args, **kwargs
                )
            else:
                handle = ray.remote(self._klass).remote(*args, **kwargs)
            return Actor(handle)

        def options(self, **opts):
            opts = {**self._default_opts, **opts}
            return ActorClassWrapper(self._klass, opts)

        def bind(self, *args, **kwargs):
            return ray.remote(**self._default_opts)(self._klass).bind(*args, **kwargs)

    class ActorClassWrapper:
        def __init__(self, klass, opts):
            self._klass = klass
            self._opts = opts

        def remote(self, *args, **kwargs):
            if self._opts:
                handle = ray.remote(**self._opts)(self._klass).remote(*args, **kwargs)
            else:
                handle = ray.remote(self._klass).remote(*args, **kwargs)
            return Actor(handle)

        def bind(self, *args, **kwargs):
            return ray.remote(**self._opts)(self._klass).bind(*args, **kwargs)


@overload
def remote_method(
    *, concurrency_group: str = ..., **kwargs
) -> Callable[[Callable[Concatenate[Any, _P], _R]], ActorMethod[_P, _R]]:
    ...


@overload
def remote_method(method: Callable[Concatenate[Any, _P], _R]) -> ActorMethod[_P, _R]:
    ...


def remote_method(*args, **kwargs):
    return args[0] if not kwargs else ray.method(**kwargs)


class ActorMixin:
    _default_ray_opts: ActorOptions

    def __init__(self) -> None:
        ...

    def __init_subclass__(cls, **default_ray_opts: Unpack[ActorOptions]) -> None:
        super().__init_subclass__()
        cls._default_ray_opts = default_ray_opts

    @classmethod
    def new_actor(cls: Callable[_P, _R]) -> ActorClass[_P, _R]:
        return ActorClass(cls, cls._default_ray_opts)  # type: ignore[attr-defined]
