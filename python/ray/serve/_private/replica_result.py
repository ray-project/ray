import inspect
import threading
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import Callable, Coroutine, Optional, Union

import ray
from ray.serve._private.common import RequestMetadata
from ray.serve._private.utils import calculate_remaining_timeout
from ray.serve.exceptions import RequestCancelledError


class ReplicaResult(ABC):
    @abstractmethod
    def get(self, timeout_s: Optional[float]):
        raise NotImplementedError

    @abstractmethod
    async def get_async(self):
        raise NotImplementedError

    @abstractmethod
    def __next__(self):
        raise NotImplementedError

    @abstractmethod
    async def __anext__(self):
        raise NotImplementedError

    @abstractmethod
    def add_done_callback(self, callback: Callable):
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abstractmethod
    def to_object_ref(self, timeout_s: Optional[float]) -> ray.ObjectRef:
        raise NotImplementedError

    @abstractmethod
    async def to_object_ref_async(self) -> ray.ObjectRef:
        raise NotImplementedError

    @abstractmethod
    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        # NOTE(edoakes): there is only a sync version of this method because it
        # does not block like `to_object_ref` (so there's also no timeout argument).
        raise NotImplementedError


class ActorReplicaResult(ReplicaResult):
    def __init__(
        self,
        obj_ref_or_gen: Union[ray.ObjectRef, ray.ObjectRefGenerator],
        metadata: RequestMetadata,
    ):
        self._obj_ref: Optional[ray.ObjectRef] = None
        self._obj_ref_gen: Optional[ray.ObjectRefGenerator] = None
        self._is_streaming: bool = metadata.is_streaming
        self._request_id: str = metadata.request_id
        self._object_ref_or_gen_sync_lock = threading.Lock()

        if isinstance(obj_ref_or_gen, ray.ObjectRefGenerator):
            self._obj_ref_gen = obj_ref_or_gen
        else:
            self._obj_ref = obj_ref_or_gen

        if self._is_streaming:
            assert (
                self._obj_ref_gen is not None
            ), "An ObjectRefGenerator must be passed for streaming requests."

    def _process_response(f: Union[Callable, Coroutine]):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            try:
                return f(self, *args, **kwargs)
            except ray.exceptions.TaskCancelledError:
                raise RequestCancelledError(self._request_id)

        @wraps(f)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await f(self, *args, **kwargs)
            except ray.exceptions.TaskCancelledError:
                raise RequestCancelledError(self._request_id)

        if inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return wrapper

    @_process_response
    def get(self, timeout_s: Optional[float]):
        assert (
            not self._is_streaming
        ), "get() can only be called on a unary ActorReplicaResult."

        start_time_s = time.time()
        object_ref = self.to_object_ref(timeout_s=timeout_s)
        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s,
            start_time_s=start_time_s,
            curr_time_s=time.time(),
        )
        return ray.get(object_ref, timeout=remaining_timeout_s)

    @_process_response
    async def get_async(self):
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a unary ActorReplicaResult."

        return await (await self.to_object_ref_async())

    @_process_response
    def __next__(self):
        assert (
            self._is_streaming
        ), "next() can only be called on a streaming ActorReplicaResult."

        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    @_process_response
    async def __anext__(self):
        assert (
            self._is_streaming
        ), "__anext__() can only be called on a streaming ActorReplicaResult."

        next_obj_ref = await self._obj_ref_gen.__anext__()
        return await next_obj_ref

    def add_done_callback(self, callback: Callable):
        if self._obj_ref_gen is not None:
            self._obj_ref_gen.completed()._on_completed(callback)
        else:
            self._obj_ref._on_completed(callback)

    def cancel(self):
        if self._obj_ref_gen is not None:
            ray.cancel(self._obj_ref_gen)
        else:
            ray.cancel(self._obj_ref)

    def to_object_ref(self, *, timeout_s: Optional[float] = None) -> ray.ObjectRef:
        assert (
            not self._is_streaming
        ), "to_object_ref can only be called on a unary ReplicaActorResult."

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__next__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None:
                obj_ref = self._obj_ref_gen._next_sync(timeout_s=timeout_s)
                if obj_ref.is_nil():
                    raise TimeoutError("Timed out resolving to ObjectRef.")

                self._obj_ref = obj_ref

        return self._obj_ref

    async def to_object_ref_async(self) -> ray.ObjectRef:
        assert (
            not self._is_streaming
        ), "to_object_ref_async can only be called on a unary ReplicaActorResult."

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__anext__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None:
                self._obj_ref = await self._obj_ref_gen.__anext__()

        return self._obj_ref

    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        assert (
            self._is_streaming
        ), "to_object_ref_gen can only be called on a streaming ReplicaActorResult."

        return self._obj_ref_gen
