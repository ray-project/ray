import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional

import ray
from ray.serve._private.utils import calculate_remaining_timeout


class ResultWrapper(ABC):
    @abstractmethod
    def get(self, timeout_s: Optional[float]):
        raise NotImplementedError

    @abstractmethod
    async def get_async(self):
        raise NotImplementedError

    @abstractmethod
    def next(self):
        raise NotImplementedError

    @abstractmethod
    async def anext(self):
        raise NotImplementedError

    @abstractmethod
    async def add_callback(self, callback: Callable):
        raise NotImplementedError

    @abstractmethod
    async def cancel(self):
        raise NotImplementedError


class ActorResultWrapper(ResultWrapper):
    def __init__(self, obj_ref_or_gen, is_gen: bool):
        self._obj_ref: Optional[ray.ObjectRef] = None
        self._obj_ref_gen: Optional[ray.ObjectRefGenerator] = None
        self._is_gen: bool = is_gen
        self._object_ref_or_gen_sync_lock = threading.Lock()

        if isinstance(obj_ref_or_gen, ray.ObjectRefGenerator):
            self._obj_ref_gen = obj_ref_or_gen
        else:
            self._obj_ref = obj_ref_or_gen

        self._resolved = None

    @property
    def obj_ref(self) -> Optional[ray.ObjectRef]:
        self._obj_ref

    @property
    def obj_ref_gen(self) -> Optional[ray.ObjectRefGenerator]:
        self._obj_ref_gen

    def resolve_gen_to_ref_if_necessary_sync(
        self, timeout_s: Optional[float] = None
    ) -> Optional[ray.ObjectRef]:
        """Returns the object ref pointing to the result."""

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref or generator cached in order to avoid calling `__next__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None and self._obj_ref_gen is not None:
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
        # object ref or generator cached in order to avoid calling `__anext__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None and self._obj_ref_gen is not None:
                self._obj_ref = await self._obj_ref_gen.__anext__()

        return self._obj_ref

    def get(self, timeout_s: Optional[float]):
        start_time_s = time.time()
        self.resolve_gen_to_ref_if_necessary_sync(timeout_s)

        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s,
            start_time_s=start_time_s,
            curr_time_s=time.time(),
        )
        return ray.get(self._obj_ref, timeout=remaining_timeout_s)

    async def get_async(self):
        await self.resolve_gen_to_ref_if_necessary_async()
        return await self._obj_ref

    def next(self):
        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    async def anext(self):
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
