from __future__ import annotations

import asyncio
import collections
from typing import TYPE_CHECKING, Deque, Iterator, Optional

import ray
from ray.exceptions import ObjectRefStreamEndOfStreamError
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray._private.worker import Worker


@DeveloperAPI
class DynamicObjectRefGenerator:
    def __init__(self, refs: Deque["ray.ObjectRef"]):
        # TODO(swang): As an optimization, can also store the generator
        # ObjectID so that we don't need to keep individual ref counts for the
        # inner ObjectRefs.
        self._refs: Deque["ray.ObjectRef"] = collections.deque(refs)

    def __iter__(self) -> Iterator("ray.ObjectRef"):
        while self._refs:
            yield self._refs.popleft()

    def __len__(self) -> int:
        return len(self._refs)


@PublicAPI
class ObjectRefGenerator:
    """A generator to obtain object references from a task in a streaming manner.

    The class is compatible with the Python generator and async generator interfaces.

    The class is not thread-safe.

    Do not initialize the class and create an instance directly.
    The instance should be created by `.remote`.

    .. testcode::

        import ray
        from typing import Generator

        @ray.remote(num_returns="streaming")
        def gen() -> Generator[int, None, None]:
            for i in range(5):
                yield i

        obj_ref_gen: ray.ObjectRefGenerator = gen.remote()
        for obj_ref in obj_ref_gen:
            print("Got:", ray.get(obj_ref))
    """

    def __init__(self, generator_ref: "ray.ObjectRef", worker: "Worker"):
        # The reference to a generator task.
        self._generator_ref = generator_ref
        # True if an exception has been raised from the generator task.
        self._generator_task_raised = False
        # Ray's worker class. ray._private.worker.global_worker
        self.worker = worker
        self.worker.check_connected()
        assert hasattr(worker, "core_worker")

    # Public APIs

    def __iter__(self) -> "ObjectRefGenerator":
        return self

    def __next__(self) -> "ray.ObjectRef":
        """Waits until a next ref is available and returns the object ref.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).
        """
        return self._next_sync()

    def send(self, value):
        raise NotImplementedError("`gen.send` is not supported.")

    def throw(self, value):
        raise NotImplementedError("`gen.throw` is not supported.")

    def close(self):
        raise NotImplementedError("`gen.close` is not supported.")

    def __aiter__(self) -> "ObjectRefGenerator":
        return self

    async def __anext__(self):
        return await self._next_async()

    async def asend(self, value):
        raise NotImplementedError("`gen.asend` is not supported.")

    async def athrow(self, value):
        raise NotImplementedError("`gen.athrow` is not supported.")

    async def aclose(self):
        raise NotImplementedError("`gen.aclose` is not supported.")

    def completed(self) -> "ray.ObjectRef":
        """Returns an object ref that is ready when
        a generator task completes.

        If the task is failed unexpectedly (e.g., worker failure),
        the `ray.get(gen.completed())` raises an exception.

        The function returns immediately.
        """
        return self._generator_ref

    def next_ready(self) -> bool:
        """If True, it means the output of next(gen) is ready and
        ray.get(next(gen)) returns immediately. False otherwise.

        It returns False when next(gen) raises a StopIteration
        (this condition should be checked using is_finished).

        The function returns immediately.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker

        if self.is_finished():
            return False

        expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

        if is_ready:
            return True

        ready, _ = ray.wait([expected_ref], timeout=0, fetch_local=False)
        return len(ready) > 0

    def is_finished(self) -> bool:
        """If True, it means the generator is finished
        and all output is taken. False otherwise.

        When True, if next(gen) is called, it will raise StopIteration
        or StopAsyncIteration

        The function returns immediately.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker

        finished = core_worker.is_object_ref_stream_finished(self._generator_ref)

        if finished:
            if self._generator_task_raised:
                return True
            else:
                # We should try ray.get on a generator ref.
                # If it raises an exception and
                # _generator_task_raised is not set,
                # this means the last ref is not taken yet.
                try:
                    ray.get(self._generator_ref)
                except Exception:
                    # The exception from _generator_ref
                    # hasn't been taken yet.
                    return False
                else:
                    return True
        else:
            return False

    # Private APIs

    def _get_next_ref(self) -> "ray.ObjectRef":
        """Return the next reference from a generator.

        Note that the ObjectID generated from a generator
        is always deterministic.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker
        return core_worker.peek_object_ref_stream(self._generator_ref)[0]

    def _next_sync(self, timeout_s: Optional[int | float] = None) -> "ray.ObjectRef":
        """Waits for timeout_s and returns the object ref if available.

        If an object is not available within the given timeout, it
        returns a nil object reference.

        If -1 timeout is provided, it means it waits infinitely.

        Waiting is implemented as busy waiting.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).

        Args:
            timeout_s: If the next object is not ready within
                this timeout, it returns the nil object ref.

        Returns:
            ObjectRef corresponding to the next result in the stream.
        """
        core_worker = self.worker.core_worker

        # Wait for the next ObjectRef to become ready.
        expected_ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

        if not is_ready:
            _, unready = ray.wait([expected_ref], timeout=timeout_s, fetch_local=False)
            if len(unready) > 0:
                return ray.ObjectRef.nil()

        try:
            ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
            assert not ref.is_nil()
        except ObjectRefStreamEndOfStreamError:
            if self._generator_task_raised:
                # Exception has been returned.
                raise StopIteration from None

            try:
                # The generator ref contains an exception
                # if there's any failure. It contains nothing otherwise.
                # In that case, it should raise StopIteration.
                ray.get(self._generator_ref)
            except Exception:
                self._generator_task_raised = True
                return self._generator_ref
            else:
                # The task finished without an exception.
                raise StopIteration from None
        return ref

    async def _suppress_exceptions(self, ref: "ray.ObjectRef") -> None:
        # Wrap a streamed ref to avoid asyncio warnings about not retrieving
        # the exception when we are just waiting for the ref to become ready.
        # The exception will get returned (or warned) to the user once they
        # actually await the ref.
        try:
            await ref
        except Exception:
            pass

    async def _next_async(self, timeout_s: Optional[int | float] = None):
        """Same API as _next_sync, but it is for async context."""
        core_worker = self.worker.core_worker
        ref, is_ready = core_worker.peek_object_ref_stream(self._generator_ref)

        if not is_ready:
            # TODO(swang): Avoid fetching the value.
            _, unready = await asyncio.wait(
                [asyncio.create_task(self._suppress_exceptions(ref))], timeout=timeout_s
            )
            if len(unready) > 0:
                return ray.ObjectRef.nil()

        try:
            ref = core_worker.try_read_next_object_ref_stream(self._generator_ref)
            assert not ref.is_nil()
        except ObjectRefStreamEndOfStreamError:
            if self._generator_task_raised:
                # Exception has been returned.
                raise StopAsyncIteration from None

            try:
                # The generator ref contains an exception
                # if there's any failure. It contains nothing otherwise.
                # In that case, it should raise StopSyncIteration.
                await self._generator_ref
            except Exception:
                self._generator_task_raised = True
                return self._generator_ref
            else:
                # Meaning the task succeed without failure raise StopAsyncIteration.
                raise StopAsyncIteration from None

        return ref

    def __del__(self):
        if hasattr(self.worker, "core_worker"):
            # The stream is created when a task is first submitted.
            # NOTE: This can be called multiple times
            # because python doesn't guarantee __del__ is called
            # only once.
            self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)

    def __getstate__(self):
        raise TypeError(
            "You cannot return or pass a generator to other task. "
            "Serializing a ObjectRefGenerator is not allowed."
        )
