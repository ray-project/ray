import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional, Union

import ray
from ray.serve._private.utils import calculate_remaining_timeout


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
    def add_callback(self, callback: Callable):
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError


class ActorReplicaResult(ReplicaResult):
    def __init__(
        self,
        obj_ref_or_gen: Union[ray.ObjectRef, ray.ObjectRefGenerator],
        is_streaming: bool,
    ):
        self._obj_ref: Optional[ray.ObjectRef] = None
        self._obj_ref_gen: Optional[ray.ObjectRefGenerator] = None
        self._is_streaming: bool = is_streaming
        self._object_ref_or_gen_sync_lock = threading.Lock()

        if isinstance(obj_ref_or_gen, ray.ObjectRefGenerator):
            self._obj_ref_gen = obj_ref_or_gen
        else:
            self._obj_ref = obj_ref_or_gen

    @property
    def obj_ref(self) -> Optional[ray.ObjectRef]:
        return self._obj_ref

    @property
    def obj_ref_gen(self) -> Optional[ray.ObjectRefGenerator]:
        return self._obj_ref_gen

    def resolve_gen_to_ref_if_necessary_sync(
        self, timeout_s: Optional[float] = None
    ) -> Optional[ray.ObjectRef]:
        """Returns the object ref pointing to the result."""

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__next__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None and not self._is_streaming:
                # Populate _obj_ref
                obj_ref = self._obj_ref_gen._next_sync(timeout_s=timeout_s)

                # Check for timeout
                if obj_ref.is_nil():
                    raise TimeoutError("Timed out resolving to ObjectRef.")

                self._obj_ref = obj_ref

        return self._obj_ref

    async def resolve_gen_to_ref_if_necessary_async(self) -> Optional[ray.ObjectRef]:
        """Returns the object ref pointing to the result."""

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__anext__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None and not self._is_streaming:
                self._obj_ref = await self._obj_ref_gen.__anext__()

        return self._obj_ref

    def get(self, timeout_s: Optional[float]):
        assert (
            self._obj_ref is not None or not self._is_streaming
        ), "get() can only be called on a non-streaming ActorReplicaResult"

        start_time_s = time.time()
        self.resolve_gen_to_ref_if_necessary_sync(timeout_s)

        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s,
            start_time_s=start_time_s,
            curr_time_s=time.time(),
        )
        return ray.get(self._obj_ref, timeout=remaining_timeout_s)

    async def get_async(self):
        assert (
            self._obj_ref is not None or not self._is_streaming
        ), "get_async() can only be called on a non-streaming ActorReplicaResult"

        await self.resolve_gen_to_ref_if_necessary_async()
        return await self._obj_ref

    def __next__(self):
        assert self._obj_ref_gen is not None, (
            "next() can only be called on an ActorReplicaResult initialized with a "
            "ray.ObjectRefGenerator"
        )

        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    async def __anext__(self):
        assert self._obj_ref_gen is not None, (
            "anext() can only be called on an ActorReplicaResult initialized with a "
            "ray.ObjectRefGenerator"
        )

        next_obj_ref = await self._obj_ref_gen.__anext__()
        return await next_obj_ref

    def add_callback(self, callback: Callable):
        if self._obj_ref_gen is not None:
            self._obj_ref_gen.completed()._on_completed(callback)
        else:
            self._obj_ref._on_completed(callback)

    def cancel(self):
        if self._obj_ref_gen is not None:
            ray.cancel(self._obj_ref_gen)
        else:
            ray.cancel(self._obj_ref)
