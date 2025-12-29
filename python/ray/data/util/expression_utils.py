"""Utility functions for expression-based operations."""

from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    from ray.data.expressions import Expr


def create_callable_class_udf_init_fn(
    exprs: List["Expr"],
) -> Optional[Callable[[], None]]:
    """Create an init_fn to initialize all callable class UDFs in expressions.

    This function collects all _CallableClassUDF instances from the given expressions,
    deduplicates them by their callable_class_spec key, and returns an init_fn that
    initializes each unique UDF at actor startup.

    Args:
        exprs: List of expressions to collect callable class UDFs from.

    Returns:
        An init_fn that initializes all callable class UDFs, or None if there are
        no callable class UDFs in the expressions.
    """
    from ray.data._internal.planner.plan_expression.expression_visitors import (
        _CallableClassUDFCollector,
    )

    callable_class_udfs = []
    for expr in exprs:
        collector = _CallableClassUDFCollector()
        collector.visit(expr)
        callable_class_udfs.extend(collector.get_callable_class_udfs())

    if not callable_class_udfs:
        return None

    # Deduplicate by callable_class_spec key
    seen_keys = set()
    unique_udfs = []
    for udf in callable_class_udfs:
        key = udf.callable_class_spec.make_key()
        if key not in seen_keys:
            seen_keys.add(key)
            unique_udfs.append(udf)

    def init_fn():
        for udf in unique_udfs:
            udf.init()

    return init_fn


def _call_udf_instance_with_async_bridge(
    instance: Any,
    async_loop: Optional[Any],
    *args,
    **kwargs,
) -> Any:
    """Call a UDF instance, bridging from sync context to async if needed.

    This handles the complexity of calling callable class UDF instances that may
    be sync, async coroutine, or async generator functions.

    Args:
        instance: The callable instance to call
        async_loop: The async event loop (if available)
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        The result of calling the instance
    """
    import asyncio
    import inspect

    # Check if the instance's __call__ is async
    if inspect.iscoroutinefunction(instance.__call__):
        # Async coroutine: bridge from sync to async
        if async_loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                instance(*args, **kwargs), async_loop
            )
            return future.result()
        else:
            return asyncio.run(instance(*args, **kwargs))
    elif inspect.isasyncgenfunction(instance.__call__):
        # Async generator: collect results
        async def _collect():
            results = []
            async for item in instance(*args, **kwargs):
                results.append(item)
            # In expressions, the UDF must return a single array with the same
            # length as the input (one output element per input row).
            # If the async generator yields multiple arrays, we take the last one
            # since expressions don't support multi-batch output semantics.
            if not results:
                return None
            elif len(results) == 1:
                return results[0]
            else:
                import logging

                logging.warning(
                    f"Async generator yielded {len(results)} values in expression context; "
                    "only the last (most recent) is returned. Use map_batches for multi-yield support."
                )
                return results[-1]

        if async_loop is not None:
            future = asyncio.run_coroutine_threadsafe(_collect(), async_loop)
            return future.result()
        else:
            return asyncio.run(_collect())
    else:
        # Synchronous instance - direct call
        return instance(*args, **kwargs)
