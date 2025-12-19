from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Sequence, Union

import ray
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray import ObjectRef
    from ray.remote_function import RemoteFunction


# ray.wait() has a default num_returns of 1.
# Using a slightly larger batch until the optimization is fully implemented, see
# https://github.com/ray-project/ray/issues/49905
DEFAULT_CHUNK_SIZE = 10
DEFAULT_BACKPRESSURE_SIZE = 100


def _wait_and_get_single_batch(
    refs: "Sequence[ObjectRef]",
    *,
    chunk_size: int,
    yield_obj_refs: bool = False,
    **kwargs,
) -> tuple[list[Union[Any, "ObjectRef"]], "list[ObjectRef]"]:
    """Call ray.wait and explicitly return the ready objects/results
    and remaining Ray remote refs.

    Args:
        refs: A list of Ray object refs.
        chunk_size: The `num_returns` parameter to pass to `ray.wait()`.
        yield_obj_refs: If True, return Ray remote refs instead of results (by calling :meth:`~ray.get`).
        **kwargs: Additional keyword arguments to pass to `ray.wait()`.

    Returns:
        A tuple of two lists, ready and not ready. This is the same as the return value of `ray.wait()`.
    """

    if chunk_size < 1:
        raise ValueError("`chunk_size` must be >= 1")

    kwargs = kwargs or {}

    # num_returns must be <= len(refs)
    ready, refs = ray.wait(
        refs,
        num_returns=min(chunk_size, len(refs)),
        **kwargs,
    )

    if not yield_obj_refs:
        return ray.get(ready), refs

    return ready, refs


@PublicAPI(stability="alpha")
def as_completed(
    refs: "Sequence[ObjectRef]",
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    yield_obj_refs: bool = False,
    **kwargs,
) -> Iterator[Union[Any, "ObjectRef"]]:
    """Given a list of Ray task references, yield results as they become available.

    Unlike calling :meth:`~ray.get` on a list of references (i.e., `ray.get(refs)`) which
    waits for all results to be ready, this function begins to yield result as soon as
    a batch of `chunk_size` results are ready.

    .. note::
        Generally there is no guarantee on the order of results. For example, the first result
        is not necessarily the first one completed, but rather the first one submitted in the
        first available batch (See :meth:`~ray.wait` for more details about
        preservation of submission order).

    .. note::
        Use this function instead of calling :meth:`~ray.get` inside a for loop. See
        https://docs.ray.io/en/latest/ray-core/patterns/ray-get-loop.html for more details.

    Example:
        Suppose we have a function that sleeps for x seconds depending on the input.
        We expect to obtain a partially sorted list of results.

        .. testcode:: python
            import ray
            import time

            @ray.remote
            def f(x):
                time.sleep(x)
                return x

            refs = [f.remote(i) for i in [10, 4, 6, 8, 2]]
            for x in ray.util.as_completed(refs, chunk_size=2):
                print(x)

        .. testoutput::
            :options: +MOCK

            # Output:
            4
            2
            6
            8
            10

    Args:
        refs: A list of Ray object refs.
        chunk_size: The number of tasks to wait for in each iteration (default 10).
            The parameter is passed as `num_returns` to :meth:`~ray.wait` internally.
        yield_obj_refs: If True, return Ray remote refs instead of results (by calling :meth:`~ray.get`).
        **kwargs: Additional keyword arguments to pass to :meth:`~ray.wait`, e.g.,
            `timeout` and `fetch_local`.

    Yields:
        Union[Any, ObjectRef]: The results (or optionally their Ray references) of the Ray tasks as they complete.
    """
    if chunk_size < 1:
        raise ValueError("`chunk_size` must be >= 1")

    if "num_returns" in kwargs:
        raise ValueError("Use the `chunksize` argument instead of `num_returns`.")

    while refs:
        results, refs = _wait_and_get_single_batch(
            refs,
            chunk_size=chunk_size,
            yield_obj_refs=yield_obj_refs,
            **kwargs,
        )
        yield from results


@PublicAPI(stability="alpha")
def map_unordered(
    fn: "RemoteFunction",
    items: Iterable[Any],
    *,
    backpressure_size: Optional[int] = DEFAULT_BACKPRESSURE_SIZE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    yield_obj_refs: bool = False,
    **kwargs,
) -> Iterator[Union[Any, "ObjectRef"]]:
    """Apply a Ray remote function to a list of items and return an iterator that yields
    the completed results as they become available.

    This helper function applies backpressure to control the number of pending tasks, following the
    design pattern described in
    https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html.

    .. note::
        There is generally no guarantee on the order of results.

    Example:
        Suppose we have a function that sleeps for x seconds depending on the input.
        We expect to obtain a partially sorted list of results.

        .. testcode:: python

            import ray
            import time

            @ray.remote
            def f(x):
                time.sleep(x)
                return x

            # Example 1: chunk_size=2
            for x in ray.util.map_unordered(f, [10, 4, 6, 8, 2], chunk_size=2):
                print(x)

        .. testoutput::
            :options: +MOCK

            4
            2
            6
            8
            10

        .. testcode:: python

            # Example 2: backpressure_size=2, chunk_size=1
            for x in ray.util.map_unordered(f, [10, 4, 6, 8, 2], backpressure_size=2, chunk_size=1):
                print(x)

        .. testoutput::
            :options: +MOCK

            4
            10
            6
            8
            2

    Args:
        fn: A remote function to apply to the list of items. For more complex use cases, use Ray Data's
            :meth:`~ray.data.Dataset.map` / :meth:`~ray.data.Dataset.map_batches` instead.
        items: An iterable of items to apply the function to.
        backpressure_size: Maximum number of in-flight tasks allowed before
            calling a blocking :meth:`~ray.wait` (default 100). If None, no backpressure is applied.
        chunk_size: The number of tasks to wait for when the number of in-flight tasks exceeds
            `backpressure_size`. The parameter is passed as `num_returns` to :meth:`~ray.wait` internally.
        yield_obj_refs: If True, return Ray remote refs instead of results (by calling :meth:`~ray.get`).
        **kwargs: Additional keyword arguments to pass to :meth:`~ray.wait`, e.g.,
            `timeout` and `fetch_local`.

    Yields:
        Union[Any, ObjectRef]: The results (or optionally their Ray references) of the Ray tasks as they complete.

    .. seealso::

        :meth:`~ray.util.as_completed`
            Call this method for an existing list of Ray object refs.

        :meth:`~ray.data.Dataset.map`
            Use Ray Data APIs (e.g., :meth:`~ray.data.Dataset.map` and :meth:`~ray.data.Dataset.map_batches`)
            for better control and complex use cases, e.g., functions with multiple arguments.

    .. note::

        This is an altenative to `pool.imap_unordered()` in Ray's Actor-based `multiprocessing.Pool`.
        See https://docs.ray.io/en/latest/ray-more-libs/multiprocessing.html for more details.

    """

    if backpressure_size is None:
        backpressure_size: float = float("inf")
    elif backpressure_size <= 0:
        raise ValueError("backpressure_size must be positive.")

    if chunk_size < 1:
        raise ValueError("`chunk_size` must be >= 1")

    if "num_returns" in kwargs:
        raise ValueError("Use the `chunk_size` argument instead of `num_returns`.")

    refs = []
    for item in items:
        refs.append(fn.remote(item))

        if len(refs) >= backpressure_size:
            results, refs = _wait_and_get_single_batch(
                refs,
                chunk_size=chunk_size,
                yield_obj_refs=yield_obj_refs,
                **kwargs,
            )
            yield from results
    else:
        yield from as_completed(
            refs,
            chunk_size=chunk_size,
            yield_obj_refs=yield_obj_refs,
            **kwargs,
        )
