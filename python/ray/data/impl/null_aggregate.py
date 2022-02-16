from typing import Tuple, Callable, Any, Union
from types import ModuleType

import numpy as np

from ray.data.block import T, U, KeyType, AggType


# This module contains aggregation helpers for handling nulls.
# The null handling policy is:
#   1. Mix of values and nulls - ignore_nulls=True:   Ignore the nulls, return
#                                                     aggregation of non-null values.
#   2. Mix of values and nulls - ignore_nulls=False:  Return None.
#   3. All nulls:                                     Return None.
#   4. Empty dataset:                                 Return None.
#
# This is accomplished by checking rows for null values and by propagating nulls
# if found AND if we're not ignoring them. If not ignoring nulls, in order to delineate
# between found null rows and an empty block accumulation when merging (the latter of
# which we want to propagate; the former of which we do not), we attach a boolean flag
# indicating whether or not an accumulation contains valid data to intermediate block
# accumulations via _wrap_acc() and _unwrap_acc(). This allows us to properly merge
# intermediate block accumulations under a streaming constraint.


def _wrap_acc(a: AggType, has_data: bool) -> AggType:
    """
    Wrap accumulation with a numeric boolean flag indicating whether or not
    this accumulation contains real data; if it doesn't, we consider it to be
    empty.

    Args:
        a: The accumulation value.
        has_data: Whether the accumulation contains real data.

    Returns:
        An AggType list with the last element being a numeric boolean flag indicating
        whether or not this accumulation contains real data. If the input a has length
        n, the returned AggType has length n + 1.
    """
    if not isinstance(a, list):
        a = [a]
    return a + [1 if has_data else 0]


def _unwrap_acc(a: AggType) -> Tuple[AggType, bool]:
    """
    Unwrap the accumulation, which we assume has been wrapped (via _wrap_acc) with a
    numeric boolean flag indicating whether or not this accumulation contains real data.

    Args:
        a: The wrapped accumulation value that we wish to unwrap.

    Returns:
        A tuple containing the unwrapped accumulation value and a boolean indicating
        whether the accumulation contains real data.
    """
    has_data = a[-1] == 1
    a = a[:-1]
    if len(a) == 1:
        a = a[0]
    return a, has_data


def _null_wrap_init(init: Callable[[KeyType], AggType]) -> Callable[[KeyType], AggType]:
    """
    Wraps an accumulation initializer with null handling.

    The returned initializer function adds on a has_data field that the accumulator
    uses to track whether an aggregation is empty.

    Args:
        init: The core init function to wrap.

    Returns:
        A new accumulation initializer function that can handle nulls.
    """

    def _init(k: KeyType) -> AggType:
        a = init(k)
        # Initializing accumulation, so indicate that the accumulation doesn't represent
        # real data yet.
        return _wrap_acc(a, has_data=False)

    return _init


def _null_wrap_accumulate(
    ignore_nulls: bool,
    on_fn: Callable[[T], T],
    accum: Callable[[AggType, T], AggType],
) -> Callable[[AggType, T], AggType]:
    """
    Wrap accumulator function with null handling.

    The returned accumulate function expects a to be either None or of the form:
    a = [acc_data_1, ..., acc_data_n, has_data].

    This performs an accumulation subject to the following null rules:
    1. If r is null and ignore_nulls=False, return None.
    2. If r is null and ignore_nulls=True, return a.
    3. If r is non-null and a is None, return None.
    5. If r is non-null and a is non-None, return accum(a[:-1], r).

    Args:
        ignore_nulls: Whether nulls should be ignored or cause a None result.
        on_fn: Function selecting a subset of the row to apply the aggregation.
        accum: The core accumulator function to wrap.

    Returns:
        A new accumulator function that handles nulls.
    """

    def _accum(a: AggType, r: T) -> AggType:
        r = on_fn(r)
        if _is_null(r):
            if ignore_nulls:
                # Ignoring nulls, return the current accumulation, ignoring r.
                return a
            else:
                # Not ignoring nulls, so propagate the null.
                return None
        else:
            if a is None:
                # Accumulation is None so (1) a previous row must have been null, and
                # (2) we must be propagating nulls, so continue to pragate this null.
                return None
            else:
                # Row is non-null and accumulation is non-null, so we now apply the core
                # accumulation.
                a, _ = _unwrap_acc(a)
                a = accum(a, r)
                return _wrap_acc(a, has_data=True)

    return _accum


def _null_wrap_merge(
    ignore_nulls: bool,
    merge: Callable[[AggType, AggType], AggType],
) -> AggType:
    """
    Wrap merge function with null handling.

    The returned merge function expects a1 and a2 to be either None or of the form:
    a = [acc_data_1, ..., acc_data_2, has_data].

    This merges two accumulations subject to the following null rules:
    1. If a1 is empty and a2 is empty, return empty accumulation.
    2. If a1 (a2) is empty and a2 (a1) is None, return None.
    3. If a1 (a2) is empty and a2 (a1) is non-None, return a2 (a1).
    4. If a1 (a2) is None, return a2 (a1) if ignoring nulls, None otherwise.
    5. If a1 and a2 are both non-null, return merge(a1, a2).

    Args:
        ignore_nulls: Whether nulls should be ignored or cause a None result.
        merge: The core merge function to wrap.

    Returns:
        A new merge function that handles nulls.
    """

    def _merge(a1: AggType, a2: AggType) -> AggType:
        if a1 is None:
            # If we're ignoring nulls, propagate a2; otherwise, propagate None.
            return a2 if ignore_nulls else None
        unwrapped_a1, a1_has_data = _unwrap_acc(a1)
        if not a1_has_data:
            # If a1 is empty, propagate a2.
            # No matter whether a2 is a real value, empty, or None,
            # propagating each of these is correct if a1 is empty.
            return a2
        if a2 is None:
            # If we're ignoring nulls, propagate a1; otherwise, propagate None.
            return a1 if ignore_nulls else None
        unwrapped_a2, a2_has_data = _unwrap_acc(a2)
        if not a2_has_data:
            # If a2 is empty, propagate a1.
            return a1
        a = merge(unwrapped_a1, unwrapped_a2)
        return _wrap_acc(a, has_data=True)

    return _merge


def _null_wrap_finalize(
    finalize: Callable[[AggType], AggType]
) -> Callable[[AggType], U]:
    """
    Wrap finalizer with null handling.

    If the accumulation is empty or None, the returned finalizer returns None.

    Args:
        finalize: The core finalizing function to wrap.

    Returns:
        A new finalizing function that handles nulls.
    """

    def _finalize(a: AggType) -> U:
        if a is None:
            return None
        a, has_data = _unwrap_acc(a)
        if not has_data:
            return None
        return finalize(a)

    return _finalize


LazyModule = Union[None, bool, ModuleType]
_pandas: LazyModule = None


def _lazy_import_pandas() -> LazyModule:
    global _pandas
    if _pandas is None:
        try:
            import pandas as _pandas
        except ModuleNotFoundError:
            # If module is not found, set _pandas to False so we won't
            # keep trying to import it on every _lazy_import_pandas() call.
            _pandas = False
    return _pandas


def _is_null(r: Any):
    pd = _lazy_import_pandas()
    if pd:
        return pd.isnull(r)
    try:
        return np.isnan(r)
    except TypeError:
        return r is None
