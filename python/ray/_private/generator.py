from typing import Callable

import ray._private.worker
from ray._raylet import ObjectRef
from ray.exceptions import RayTaskError, RayTaskStopIteration

# @PrivateAPI
class StreamingObjectRefGenerator:
    def __init__(
        self,
        session_id_object_ref: ObjectRef,
        actor: "ray.actor.ActorHandle",
        method_name: str,
        is_gencoroutine: bool,
    ):
        # The first return value of generator actor method
        # always returns the session_id.
        self._session_id_object_ref = session_id_object_ref
        import weakref

        self._actor_ref = weakref.ref(actor)
        self._actor_id = actor._actor_id
        self._method_name = method_name
        self._is_gencoroutine = is_gencoroutine

    def __iter__(self):
        return self

    def __next__(self) -> ObjectRef:
        try:
            if self._is_gencoroutine:
                return ray.get(
                    self._next(next_func=self._actor_ref().__ray_async_generator_next__)
                )
            else:
                return ray.get(
                    self._next(next_func=self._actor_ref().__ray_generator_next__)
                )
        except RayTaskError as e:
            if isinstance(e.cause, RayTaskStopIteration):
                raise StopIteration
            else:
                raise e

    def __aiter__(self):
        return self

    async def __anext__(self) -> ObjectRef:
        try:
            if self._is_gencoroutine:
                return await self._next(
                    next_func=self._actor_ref().__ray_async_generator_next__
                )
            else:
                return await self._next(
                    next_func=self._actor_ref().__ray_generator_next__
                )
        except RayTaskError as e:
            if isinstance(e.cause, RayTaskStopIteration):
                raise StopAsyncIteration
            else:
                raise e

    def _next(self, *, next_func: Callable) -> ObjectRef:
        # Since it is a weak ref, it could be GC'ed already if the caller
        # already del actor_handle.
        if self._actor_ref() is None:
            raise ValueError(
                f"The actor {self._actor_id} is already "
                "garbage collected. Did you call del?"
            )
        result_ref = next_func.options(name=f"{self._method_name}.next").remote(
            _ray_generator_session_id=self._session_id_object_ref
        )
        return result_ref

    def __del__(self):
        if self._actor_ref() is None:
            return
        self._actor_ref().__clean_generator__.remote(
            _ray_generator_session_id=self._session_id_object_ref
        )
