from abc import abstractmethod
from typing import List

from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlocksToBatchesMapTransformFn,
    BlocksToRowsMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformFn,
    MapTransformFnCategory,
    RowMapTransformFn,
)
from ray.data._internal.logical.rules.zero_copy_map_fusion import ZeroCopyMapFusionRule


class BaseRedundantMapTransformPruning(ZeroCopyMapFusionRule):
    def _optimize(self, transform_fns: List[MapTransformFn]) -> List[MapTransformFn]:
        """
        Detects and removes redundant MapTransformFn(s) in a sequence by
        checking for start -> intermediate(s) -> end -> start.

        Args:
        - transform_fns: List of MapTransformFn(s).
        Returns:
        - List of optimized MapTransformFn(s) with redundant patterns removed.
        """
        # Group MapTransformFn(s) based on MapTransformFnCategory PreProcess and
        # PostProcess.
        grouped_transform_fns = self._group_transformations(transform_fns)

        # Merge contiguous groups if applicable by detecting and removing redundant
        # MapTransformFn(s) in a sequence by checking for
        # start -> intermediate(s) -> end -> start.
        merged_groups = self._merge_contiguous_groups(grouped_transform_fns)

        # Flatten the merged groups and return the optimized transformations.
        return [fn for group in merged_groups for fn in group]

    def _group_transformations(
        self, transform_fns: List[MapTransformFn]
    ) -> List[List[MapTransformFn]]:
        """
        Group MapTransformFn(s) based on MapTransformFnCategory PreProcess and
        PostProcess.

        Args:
        - transform_fns: List of MapTransformFn(s) to be grouped.

        Returns:
        - A list of grouped MapTransformFn(s).
        """
        # List to store grouped transformation functions
        groups = []
        # Temporary list to hold the current group of functions
        current_group = []

        idx = 0
        while idx < len(transform_fns):
            fn = transform_fns[idx]

            if fn.category == MapTransformFnCategory.PreProcess:
                # If the function is of category PreProcess, start a new group:
                if current_group:
                    # If there is an existing group, finalize it and add to `groups`
                    groups.append(current_group)
                # Start a new group with the current function
                current_group = [fn]

            elif fn.category == MapTransformFnCategory.PostProcess:
                # If the function is of category PostProcess, end the current group:
                if current_group:
                    # Add the current function to the group and finalize the group
                    current_group.append(fn)
                    groups.append(current_group)
                # Reset the current group as the PostProcess marks the end of a group
                current_group = []

            else:
                # For other categories, simply add the function to the current group
                current_group.append(fn)

            idx += 1

        # Handle any remaining functions in the current group
        if current_group:
            groups.append(current_group)

        return groups

    def _merge_contiguous_groups(
        self, grouped_transform_fns: List[List[MapTransformFn]]
    ) -> List[List[MapTransformFn]]:
        """
        Merge contiguous groups if applicable by detecting and removing redundant
        MapTransformFn(s) in a sequence by checking for
        start -> intermediate(s) -> end -> start.

        Args:
        - grouped_transform_fns: List of grouped MapTransformFn(s).

        Returns:
        - A list of optimized grouped MapTransformFn(s).
        """
        merged_groups = []
        idx = 0
        copy_grouped_transform_fns = grouped_transform_fns

        while idx < len(copy_grouped_transform_fns):
            current_group = copy_grouped_transform_fns[idx]

            # Check if the next group exists for merging
            if idx + 1 < len(copy_grouped_transform_fns):
                next_group = copy_grouped_transform_fns[idx + 1]
                if self._can_merge(current_group, next_group):
                    # Merge the two groups: remove redundant start and end functions
                    # Remove the end function from the current group
                    del current_group[-1]
                    # Remove the start function from the next group
                    del next_group[0]

                    # Merge the groups together
                    current_group.extend(next_group)

                    # Update the copy_grouped_transform_fns list to reflect the merged group
                    copy_grouped_transform_fns[idx] = current_group
                    # Remove the now-merged next group
                    del copy_grouped_transform_fns[idx + 1]

                    # Recheck the merged group with the next group, so no idx increment
                else:
                    # No merge possible, finalize the current group
                    merged_groups.append(current_group)
                    idx += 1
            else:
                # No next group to merge with, finalize the current group
                merged_groups.append(current_group)
                idx += 1

        return merged_groups

    def _can_merge(
        self,
        group1: List[MapTransformFn],
        group2: List[MapTransformFn],
    ) -> bool:
        """
        Determines if two groups can be merged based on their start, intermediate and
        end functions.

        Args:
        - group1: The first group of MapTransformFn(s).
        - group2: The second group of MapTransformFn(s).

        Returns:
        - True if the groups can be merged, False otherwise.
        """
        return (
            # group1 matches end MapTransformFn in sequence
            self._is_end_fn(group1[-1])
            # group2 matches start MapTransformFn in sequence
            and self._is_start_fn(group2[0])
            # Both groups have identical start MapTransformFn including args
            and self._start_fn_values_match(group1[0], group2[0])
            # group1 matches matches intermediate MapTransformFn in
            # sequence
            and self._intermediate_functions_match(group1)
            # group2 matches matches intermediate MapTransformFn in
            # sequence
            and self._intermediate_functions_match(group2)
        )

    def _intermediate_functions_match(
        self, grouped_transform_fns: List[MapTransformFn]
    ) -> bool:
        """
        Validates that the intermediate MapTransformFn(s) in grouped_transform_fns.

        Args:
        - grouped_transform_fns: Group of MapTransformFn(s).

        Returns:
        - True if the intermediate MapTransformFn(s) match, False otherwise.
        """
        # Ensure all functions in intermediate_transform_fns match
        intermediate_transform_fns = grouped_transform_fns[1:-1]
        for fn in intermediate_transform_fns:
            if not self._is_intermediate_fn(fn):
                return False

        return True

    @abstractmethod
    def _is_start_fn(self, fn):
        """Detect start MapTransformFn function in sequence:
        start -> intermediate(s) -> end -> start.
        """
        ...

    @abstractmethod
    def _is_intermediate_fn(self, fn):
        """Detect intermediate MapTransformFn function in sequence:
        start -> intermediate(s) -> end -> start.
        """
        ...

    @abstractmethod
    def _is_end_fn(self, fn):
        """Detect end MapTransformFn function in sequence:
        start -> intermediate(s) -> end -> start.i
        """
        ...

    def _start_fn_values_match(self, start_fn1, start_fn2):
        """Check if start MapTransformFn functions logically match. Note that this is
        enforced with __eq__ in MapTransforFn derived classes.
        """
        return start_fn1 == start_fn2


class RedundantMapTransformRowPruning(BaseRedundantMapTransformPruning):
    """Prunes redundant row-based map transform sequences.

    The pattern being checked:
    BlocksToRowsMapTransformFn ->
    RowMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Row) ->
    BlocksToRowsMapTransformFn

    For Eg:
    Input MapTransformFn(s):
    ------------------------
    BlocksToRowsMapTransformFn ->
    RowMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Row) ->
    BlocksToRowsMapTransformFn ->
    RowMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Row) ->

    Optimized MapTransformFn(s):
    --------------------------
    BlocksToRowsMapTransformFn ->
    RowMapTransformFn ->
    RowMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Row)

    Note:
    1. Redundant MapTransformFn(s) BuildOutputBlocksMapTransformFn and
    BlocksToRowsMapTransformFn have been pruned.
    2. BlocksToRowsMapTransformFn must have same set of arguments enforced by
    __eq__ check.
    """

    def _is_start_fn(self, fn):
        """Detect start row MapTransformFn, BlocksToRowsMapTransformFn."""
        return isinstance(fn, BlocksToRowsMapTransformFn)

    def _is_intermediate_fn(self, fn):
        """Detect intermediate row MapTransformFn, RowMapTransformFn."""
        return isinstance(fn, RowMapTransformFn)

    def _is_end_fn(self, fn):
        """Detect end row MapTransformFn, BuildOutputBlocksMapTransformFn."""
        return isinstance(fn, BuildOutputBlocksMapTransformFn)


class RedundantMapTransformBatchPruning(BaseRedundantMapTransformPruning):
    """Prunes redundant batch-based map transform sequences.

    The pattern being checked:
    BlocksToBatchesMapTransformFn ->
    BatchMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Batch) ->
    BlocksToBatchesMapTransformFn


    For Eg:
    Input MapTransformFn(s):
    ------------------------
    BlocksToBatchesMapTransformFn ->
    BatchMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Batch) ->
    BlocksToBatchesMapTransformFn ->
    BatchMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Batch) ->

    Optimized MapTransformFn(s):
    --------------------------
    BlocksToRowsMapTransformFn ->
    BatchMapTransformFn ->
    BatchMapTransformFn ->
    BuildOutputBlocksMapTransformFn(input_type=MapTransformFnDataType.Batch)

    Note:
    1.Redundant MapTransformFn(s) BuildOutputBlocksMapTransformFn and
    BatchMapTransformFn have been pruned.
    2. BlocksToBatchesMapTransformFn must have same set of arguments enforced byi
    __eq__ check.
    """

    def _is_start_fn(self, fn):
        """Detect start batch MapTransformFn, BlocksToBatchesMapTransformFn."""
        return isinstance(fn, BlocksToBatchesMapTransformFn)

    def _is_intermediate_fn(self, fn):
        """Detect intermediate batch MapTransformFn, BatchMapTransformFn."""
        return isinstance(fn, BatchMapTransformFn)

    def _is_end_fn(self, fn):
        """Detect end batch MapTransformFn, BuildOutputBatchMapTransformFn."""
        return isinstance(fn, BuildOutputBlocksMapTransformFn)
