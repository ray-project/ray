from typing import Callable, Optional, Union, Any
from ray.util.annotations import PublicAPI
from ray.data.block import T, U, KeyType, AggType

AggregateOnT = Union[Callable[[T], Any], str]


@PublicAPI(stability="beta")
class AggregateFn(object):
    def __init__(self,
                 init: Callable[[KeyType], AggType],
                 accumulate: Callable[[AggType, T], AggType],
                 merge: Callable[[AggType, AggType], AggType],
                 finalize: Callable[[AggType], U] = lambda a: a,
                 name: Optional[str] = None):
        """Defines an aggregate function in the accumulator style.

        Aggregates a collection of inputs of type T into
        a single output value of type U.
        See https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
        for more details about accumulator-based aggregation.

        Args:
            init: This is called once for each key
                to return the empty accumulator.
                For example, an empty accumulator for a sum would be 0.
            accumulate: This is called once per row of the same key.
                This combines the accumulator and the row,
                returns the updated accumulator.
            merge: This may be called multiple times, each time to merge
                two accumulators into one.
            finalize: This is called once to compute the final aggregation
                result from the fully merged accumulator.
            name: The name of the aggregation. This will be used as the output
                column name in the case of arrow dataset.
        """
        self.init = init
        self.accumulate = accumulate
        self.merge = merge
        self.finalize = finalize
        self.name = name


class Count(AggregateFn):
    """Defines count aggregation"""

    def __init__(self):
        super().__init__(
            init=lambda k: 0,
            accumulate=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2,
            name="count()")


class Sum(AggregateFn):
    """Defines sum aggregation"""

    def __init__(self, on: AggregateOnT = lambda r: r):
        on_fn = on
        if isinstance(on, str):

            def func(r):
                return r[on]

            on_fn = func
        super().__init__(
            init=lambda k: 0,
            accumulate=lambda a, r: a + on_fn(r),
            merge=lambda a1, a2: a1 + a2,
            name=(f"sum({on})" if isinstance(on, str) else None))


class Min(AggregateFn):
    """Defines min aggregation"""

    def __init__(self, on: AggregateOnT = lambda r: r):
        on_fn = on
        if isinstance(on, str):

            def func(r):
                return r[on]

            on_fn = func
        super().__init__(
            init=lambda k: None,
            accumulate=  # noqa: E251
            lambda a, r: on_fn(r) if a is None else min(a, on_fn(r)),
            merge=lambda a1, a2: min(a1, a2),
            name=(f"min({on})" if isinstance(on, str) else None))


class Max(AggregateFn):
    """Defines max aggregation"""

    def __init__(self, on: AggregateOnT = lambda r: r):
        on_fn = on
        if isinstance(on, str):

            def func(r):
                return r[on]

            on_fn = func
        super().__init__(
            init=lambda k: None,
            accumulate=  # noqa: E251
            lambda a, r: on_fn(r) if a is None else max(a, on_fn(r)),
            merge=lambda a1, a2: max(a1, a2),
            name=(f"max({on})" if isinstance(on, str) else None))


class Mean(AggregateFn):
    """Defines mean aggregation"""

    def __init__(self, on: AggregateOnT = lambda r: r):
        on_fn = on
        if isinstance(on, str):

            def func(r):
                return r[on]

            on_fn = func
        super().__init__(
            init=lambda k: [0, 0],
            accumulate=lambda a, r: [a[0] + on_fn(r), a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name=(f"mean({on})" if isinstance(on, str) else None))
