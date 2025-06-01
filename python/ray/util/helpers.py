from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Sequence, Union
import ray
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray import ObjectRef
    from ray.remote_function import RemoteFunction


DEFAULT_RAY_WAIT_NUM_RETURNS = 10
DEFAULT_BACKPRESSURE_SIZE = 100


def _wait_and_get_single_batch(
    refs: "Sequence[ObjectRef]",
    return_objrefs: bool = False,
    **kwargs,
) -> tuple[list[Union[Any, "ObjectRef"]], "list[ObjectRef]"]:
    """Call ray.wait and explicitly return the ready objects/results
    and remaining Ray remote refs

    Args:
        refs: A list of Ray object refs.
        return_objrefs: Ff True, return Ray remote refs instead of results
        **kwargs: Additional keyword arguments to pass to `ray.wait()`.
    """

    # ray.wait() has a default num_returns of 1.
    # Using a slightly larger batch until the optimization is fully implemented, see
    # https://github.com/ray-project/ray/issues/49905
    kwargs = kwargs or {"num_returns": DEFAULT_RAY_WAIT_NUM_RETURNS}

    # ray.wait() raise error if num_returns < len(refs)
    if "num_returns" in kwargs:
        kwargs["num_returns"] = min(
            kwargs["num_returns"],
            len(refs),
        )

    ready, refs = ray.wait(refs, **kwargs)

    if not return_objrefs:
        ready = ray.get(ready)

    return ready, refs


@PublicAPI(stability="alpha")
def as_completed(
    refs: "Sequence[ObjectRef]",
    return_objrefs: bool = False,
    **kwargs,
) -> Iterator[Union[Any, "ObjectRef"]]:
    """Given a list of Ray task references, yield results as they become available.

    .. note::
        Unlike calling ::meth:`~ray.get` on a list of references (i.e., `ray.get(refs)`),
        this function yields partial results as soon as they are ready.

    .. note::
        Use this function instead of calling :meth:`~ray.get` inside a for loop. See
        https://docs.ray.io/en/latest/ray-core/patterns/ray-get-loop.html for more details.

    Internally, this function waits for a batch of results to be ready in each iteration. The batch
    size is controlled by the `num_returns` keyword argument (passing to :meth:`~ray.wait`).

    Args:
        refs: A list of Ray object refs.
        return_objrefs: If True, return Ray remote refs instead of results.
        **kwargs: Additional keyword arguments to pass to :meth:`~ray.wait`. In particular,
            `num_returns` controls the number of tasks to wait for in each iteration.

    """

    while refs:
        results, refs = _wait_and_get_single_batch(
            refs,
            return_objrefs=return_objrefs,
            **kwargs,
        )
        yield from results


@PublicAPI(stability="alpha")
def map_unordered(
    fn: "RemoteFunction",
    items: Iterable[Any],
    return_objrefs: bool = False,
    *,
    backpressure_size: Optional[int] = DEFAULT_BACKPRESSURE_SIZE,
    **kwargs,
) -> Iterator[Union[Any, "ObjectRef"]]:
    """Apply a Ray remote function to a list of items and return an iterator that yields
    the completed results as they become available.

    This helper function applies a backpressure to control the number of pending tasks, following the
    design pattern described in
    https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html.

    Args:
        fn: A remote function to apply to the list of items. For more complex use cases, use Ray Data's
            :meth:`~ray.data.Dataset.map` / :meth:`~ray.data.Dataset.map_batches` instead.
        items: An iterable of items to apply the function to.
        backpressure_size: Maximum number of in-flight tasks allowed before
            calling a blocking :meth:`~ray.wait`. If None, no backpressure is applied.
        return_objrefs: If True, return Ray remote refs instead of results (by calling :meth:`~ray.get`).
        **kwargs: Additional keyword arguments to pass to :meth:`~ray.wait`. In particular,
            `num_returns` controls the number of tasks to wait for whenever the number of
            in-flight tasks exceeds `backpressure_size`.

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
        backpressure_size = float("inf")

    refs = []
    for item in items:
        refs.append(fn.remote(item))

        if len(refs) >= backpressure_size:
            results, refs = _wait_and_get_single_batch(
                refs,
                return_objrefs=return_objrefs,
                **kwargs,
            )
            yield from results
    else:
        yield from as_completed(
            refs,
            return_objrefs=return_objrefs,
            **kwargs,
        )
