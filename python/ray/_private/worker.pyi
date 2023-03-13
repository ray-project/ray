from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    TypeVar,
    Union,
    overload,
)
from ray import ObjectRef
if sys.version_info >= (3, 8):
    from typing import Literal, Protocol
else:
    from typing_extensions import Literal, Protocol
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# keep ray.remote type checking overloads in a separate file
# so we can exclude them from docs.
# refer to https://github.com/ray-project/ray/pull/33243
# for more context.

T0 = TypeVar("T0")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")
R = TypeVar("R")

DAGNode = TypeVar("DAGNode")


class RemoteFunctionNoArgs(Generic[R]):
    def __init__(self, function: Callable[[], R]) -> None:
        pass

    def remote(
        self,
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
    ) -> "DAGNode[R]":
        ...


class RemoteFunction0(Generic[R, T0]):
    def __init__(self, function: Callable[[T0], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction1(Generic[R, T0, T1]):
    def __init__(self, function: Callable[[T0, T1], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction2(Generic[R, T0, T1, T2]):
    def __init__(self, function: Callable[[T0, T1, T2], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction3(Generic[R, T0, T1, T2, T3]):
    def __init__(self, function: Callable[[T0, T1, T2, T3], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction4(Generic[R, T0, T1, T2, T3, T4]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction5(Generic[R, T0, T1, T2, T3, T4, T5]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction6(Generic[R, T0, T1, T2, T3, T4, T5, T6]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction7(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction8(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]):
    def __init__(
        self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
    ) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
        __arg8: "Union[T8, ObjectRef[T8]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
        __arg8: "Union[T8, DAGNode[T8]]",
    ) -> "DAGNode[R]":
        ...


class RemoteFunction9(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]):
    def __init__(
        self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
    ) -> None:
        pass

    def remote(
        self,
        __arg0: "Union[T0, ObjectRef[T0]]",
        __arg1: "Union[T1, ObjectRef[T1]]",
        __arg2: "Union[T2, ObjectRef[T2]]",
        __arg3: "Union[T3, ObjectRef[T3]]",
        __arg4: "Union[T4, ObjectRef[T4]]",
        __arg5: "Union[T5, ObjectRef[T5]]",
        __arg6: "Union[T6, ObjectRef[T6]]",
        __arg7: "Union[T7, ObjectRef[T7]]",
        __arg8: "Union[T8, ObjectRef[T8]]",
        __arg9: "Union[T9, ObjectRef[T9]]",
    ) -> "ObjectRef[R]":
        ...

    def bind(
        self,
        __arg0: "Union[T0, DAGNode[T0]]",
        __arg1: "Union[T1, DAGNode[T1]]",
        __arg2: "Union[T2, DAGNode[T2]]",
        __arg3: "Union[T3, DAGNode[T3]]",
        __arg4: "Union[T4, DAGNode[T4]]",
        __arg5: "Union[T5, DAGNode[T5]]",
        __arg6: "Union[T6, DAGNode[T6]]",
        __arg7: "Union[T7, DAGNode[T7]]",
        __arg8: "Union[T8, DAGNode[T8]]",
        __arg9: "Union[T9, DAGNode[T9]]",
    ) -> "DAGNode[R]":
        ...



class RemoteDecorator(Protocol):
    @overload
    def __call__(self, __function: Callable[[], R]) -> RemoteFunctionNoArgs[R]:
        ...

    @overload
    def __call__(self, __function: Callable[[T0], R]) -> RemoteFunction0[R, T0]:
        ...

    @overload
    def __call__(self, __function: Callable[[T0, T1], R]) -> RemoteFunction1[R, T0, T1]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2], R]
    ) -> RemoteFunction2[R, T0, T1, T2]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3], R]
    ) -> RemoteFunction3[R, T0, T1, T2, T3]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4], R]
    ) -> RemoteFunction4[R, T0, T1, T2, T3, T4]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5], R]
    ) -> RemoteFunction5[R, T0, T1, T2, T3, T4, T5]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]
    ) -> RemoteFunction6[R, T0, T1, T2, T3, T4, T5, T6]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]
    ) -> RemoteFunction7[R, T0, T1, T2, T3, T4, T5, T6, T7]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
    ) -> RemoteFunction8[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]:
        ...

    @overload
    def __call__(
        self, __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
    ) -> RemoteFunction9[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]:
        ...

    # Pass on typing actors for now. The following makes it so no type errors
    # are generated for actors.
    @overload
    def __call__(self, __t: type) -> Any:
        ...


# Only used for type annotations as a placeholder
Undefined: Any = object()


@overload
def remote(__function: Callable[[], R]) -> RemoteFunctionNoArgs[R]:
    ...


@overload
def remote(__function: Callable[[T0], R]) -> RemoteFunction0[R, T0]:
    ...


@overload
def remote(__function: Callable[[T0, T1], R]) -> RemoteFunction1[R, T0, T1]:
    ...


@overload
def remote(__function: Callable[[T0, T1, T2], R]) -> RemoteFunction2[R, T0, T1, T2]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3], R]
) -> RemoteFunction3[R, T0, T1, T2, T3]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4], R]
) -> RemoteFunction4[R, T0, T1, T2, T3, T4]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5], R]
) -> RemoteFunction5[R, T0, T1, T2, T3, T4, T5]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]
) -> RemoteFunction6[R, T0, T1, T2, T3, T4, T5, T6]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]
) -> RemoteFunction7[R, T0, T1, T2, T3, T4, T5, T6, T7]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]
) -> RemoteFunction8[R, T0, T1, T2, T3, T4, T5, T6, T7, T8]:
    ...


@overload
def remote(
    __function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]
) -> RemoteFunction9[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]:
    ...


# Pass on typing actors for now. The following makes it so no type errors
# are generated for actors.
@overload
def remote(__t: type) -> Any:
    ...


# Passing options
@overload
def remote(
    *,
    num_returns: Union[int, float] = Undefined,
    num_cpus: Union[int, float] = Undefined,
    num_gpus: Union[int, float] = Undefined,
    resources: Dict[str, float] = Undefined,
    accelerator_type: str = Undefined,
    memory: Union[int, float] = Undefined,
    max_calls: int = Undefined,
    max_restarts: int = Undefined,
    max_task_retries: int = Undefined,
    max_retries: int = Undefined,
    runtime_env: Dict[str, Any] = Undefined,
    retry_exceptions: bool = Undefined,
    scheduling_strategy: Union[
        None, Literal["DEFAULT"], Literal["SPREAD"], PlacementGroupSchedulingStrategy
    ] = Undefined,
) -> RemoteDecorator:
    ...
