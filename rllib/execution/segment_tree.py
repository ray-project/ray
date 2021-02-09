import operator
from typing import Any, Optional


class SegmentTree:
    """A Segment Tree data structure.

    https://en.wikipedia.org/wiki/Segment_tree

    Can be used as regular array, but with two important differences:

      a) Setting an item's value is slightly slower. It is O(lg capacity),
         instead of O(1).
      b) Offers efficient `reduce` operation which reduces the tree's values
         over some specified contiguous subsequence of items in the array.
         Operation could be e.g. min/max/sum.

    The data is stored in a list, where the length is 2 * capacity.
    The second half of the list stores the actual values for each index, so if
    capacity=8, values are stored at indices 8 to 15. The first half of the
    array contains the reduced-values of the different (binary divided)
    segments, e.g. (capacity=4):
    0=not used
    1=reduced-value over all elements (array indices 4 to 7).
    2=reduced-value over array indices (4 and 5).
    3=reduced-value over array indices (6 and 7).
    4-7: values of the tree.
    NOTE that the values of the tree are accessed by indices starting at 0, so
    `tree[0]` accesses `internal_array[4]` in the above example.
    """

    def __init__(self,
                 capacity: int,
                 operation: Any,
                 neutral_element: Optional[Any] = None):
        """Initializes a Segment Tree object.

        Args:
            capacity (int): Total size of the array - must be a power of two.
            operation (operation): Lambda obj, obj -> obj
                The operation for combining elements (eg. sum, max).
                Must be a mathematical group together with the set of
                possible values for array elements.
            neutral_element (Optional[obj]): The neutral element for
                `operation`. Use None for automatically finding a value:
                max: float("-inf"), min: float("inf"), sum: 0.0.
        """

        assert capacity > 0 and capacity & (capacity - 1) == 0, \
            "Capacity must be positive and a power of 2!"
        self.capacity = capacity
        if neutral_element is None:
            neutral_element = 0.0 if operation is operator.add else \
                float("-inf") if operation is max else float("inf")
        self.neutral_element = neutral_element
        self.value = [self.neutral_element for _ in range(2 * capacity)]
        self.operation = operation

    def reduce(self, start: int = 0, end: Optional[int] = None) -> Any:
        """Applies `self.operation` to subsequence of our values.

        Subsequence is contiguous, includes `start` and excludes `end`.

          self.operation(
              arr[start], operation(arr[start+1], operation(... arr[end])))

        Args:
            start (int): Start index to apply reduction to.
            end (Optional[int]): End index to apply reduction to (excluded).

        Returns:
            any: The result of reducing self.operation over the specified
                range of `self._value` elements.
        """
        if end is None:
            end = self.capacity
        elif end < 0:
            end += self.capacity

        # Init result with neutral element.
        result = self.neutral_element
        # Map start/end to our actual index space (second half of array).
        start += self.capacity
        end += self.capacity

        # Example:
        # internal-array (first half=sums, second half=actual values):
        # 0 1 2 3 | 4 5 6 7
        # - 6 1 5 | 1 0 2 3

        # tree.sum(0, 3) = 3
        # internally: start=4, end=7 -> sum values 1 0 2 = 3.

        # Iterate over tree starting in the actual-values (second half)
        # section.
        # 1) start=4 is even -> do nothing.
        # 2) end=7 is odd -> end-- -> end=6 -> add value to result: result=2
        # 3) int-divide start and end by 2: start=2, end=3
        # 4) start still smaller end -> iterate once more.
        # 5) start=2 is even -> do nothing.
        # 6) end=3 is odd -> end-- -> end=2 -> add value to result: result=1
        #    NOTE: This adds the sum of indices 4 and 5 to the result.

        # Iterate as long as start != end.
        while start < end:

            # If start is odd: Add its value to result and move start to
            # next even value.
            if start & 1:
                result = self.operation(result, self.value[start])
                start += 1

            # If end is odd: Move end to previous even value, then add its
            # value to result. NOTE: This takes care of excluding `end` in any
            # situation.
            if end & 1:
                end -= 1
                result = self.operation(result, self.value[end])

            # Divide both start and end by 2 to make them "jump" into the
            # next upper level reduce-index space.
            start //= 2
            end //= 2

            # Then repeat till start == end.

        return result

    def __setitem__(self, idx: int, val: float) -> None:
        """
        Inserts/overwrites a value in/into the tree.

        Args:
            idx (int): The index to insert to. Must be in [0, `self.capacity`[
            val (float): The value to insert.
        """
        assert 0 <= idx < self.capacity

        # Index of the leaf to insert into (always insert in "second half"
        # of the tree, the first half is reserved for already calculated
        # reduction-values).
        idx += self.capacity
        self.value[idx] = val

        # Recalculate all affected reduction values (in "first half" of tree).
        idx = idx >> 1  # Divide by 2 (faster than division).
        while idx >= 1:
            update_idx = 2 * idx  # calculate only once
            # Update the reduction value at the correct "first half" idx.
            self.value[idx] = self.operation(self.value[update_idx],
                                             self.value[update_idx + 1])
            idx = idx >> 1  # Divide by 2 (faster than division).

    def __getitem__(self, idx: int) -> Any:
        assert 0 <= idx < self.capacity
        return self.value[idx + self.capacity]


class SumSegmentTree(SegmentTree):
    """A SegmentTree with the reduction `operation`=operator.add."""

    def __init__(self, capacity: int):
        super(SumSegmentTree, self).__init__(
            capacity=capacity, operation=operator.add)

    def sum(self, start: int = 0, end: Optional[Any] = None) -> Any:
        """Returns the sum over a sub-segment of the tree."""
        return self.reduce(start, end)

    def find_prefixsum_idx(self, prefixsum: float) -> int:
        """Finds highest i, for which: sum(arr[0]+..+arr[i - i]) <= prefixsum.

        Args:
            prefixsum (float): `prefixsum` upper bound in above constraint.

        Returns:
            int: Largest possible index (i) satisfying above constraint.
        """
        assert 0 <= prefixsum <= self.sum() + 1e-5
        # Global sum node.
        idx = 1

        # While non-leaf (first half of tree).
        while idx < self.capacity:
            update_idx = 2 * idx
            if self.value[update_idx] > prefixsum:
                idx = update_idx
            else:
                prefixsum -= self.value[update_idx]
                idx = update_idx + 1
        return idx - self.capacity


class MinSegmentTree(SegmentTree):
    def __init__(self, capacity: int):
        super(MinSegmentTree, self).__init__(capacity=capacity, operation=min)

    def min(self, start: int = 0, end: Optional[Any] = None) -> Any:
        """Returns min(arr[start], ...,  arr[end])"""
        return self.reduce(start, end)
