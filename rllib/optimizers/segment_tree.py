import operator


class SegmentTree:
    """A Segment Tree data structure.

    https://en.wikipedia.org/wiki/Segment_tree

    Can be used as regular array, but with two important differences:

      a) Setting an item's value is slightly slower. It is O(lg capacity),
         instead of O(1).
      b) Offers efficient `reduce` operation which reduces the tree's values
         over some specified contiguous subsequence of items in the array.
         Operation could be e.g. min/max/sum.
    """

    def __init__(self, capacity, operation, neutral_element=None):
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

    def reduce(self, start=0, end=None):
        """Applies `self.operation` to subsequence of our values.

        Subsequence is contiguous, includes `start` and excludes `end`.

          self.operation(
              arr[start], operation(arr[start+1], operation(... arr[end])))

        Args:
            start (int): Start index to apply reduction to.
            end (int): Start index to apply reduction to (excluded).

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
        start += self.capacity
        end += self.capacity

        while start < end:
            if start & 1:
                result = self.operation(result, self.value[start])
                start += 1
            if end & 1:
                end -= 1
                result = self.operation(result, self.value[end])
            start = start >> 1
            end = end >> 1
        return result

    def __setitem__(self, idx, val):
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

        # Reclculate all affected reduction values (in "first half" of tree).
        idx = idx >> 1  # Divide by 2 (fater than division).
        while idx >= 1:
            update_idx = 2 * idx  # calculate only once
            # Update the reduction value at the correct "first half" idx.
            self.value[idx] = self.operation(
                self.value[update_idx], self.value[update_idx + 1])
            idx = idx >> 1  # Divide by 2 (fater than division).

    def __getitem__(self, idx):
        assert 0 <= idx < self.capacity
        return self.value[idx + self.capacity]


class SumSegmentTree(SegmentTree):
    """A SegmentTree with the reduction `operation`=operator.add."""
    def __init__(self, capacity):
        super(SumSegmentTree, self).__init__(
            capacity=capacity, operation=operator.add)

    def sum(self, start=0, end=None):
        """Returns the sum over a sub-segment of the tree."""
        return self.reduce(start, end)

    def find_prefixsum_idx(self, prefixsum):
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
    def __init__(self, capacity):
        super(MinSegmentTree, self).__init__(capacity=capacity, operation=min)

    def min(self, start=0, end=None):
        """Returns min(arr[start], ...,  arr[end])"""
        return self.reduce(start, end)
