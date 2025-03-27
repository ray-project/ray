from typing import List, Callable
import copy

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


class RedundantMapTransformPruning(ZeroCopyMapFusionRule):
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
        copy_grouped_transform_fns = copy.deepcopy(grouped_transform_fns)

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
        first_group: List[MapTransformFn],
        second_group: List[MapTransformFn],
    ) -> bool:
        """
        Determines if two groups can be merged based on their start, intermediate and
        end functions for row or batch map transform functions.

        Args:
        - first_group: The first group of MapTransformFn(s).
        - second_group: The second group of MapTransformFn(s).

        Returns:
        - True if the groups can be merged, False otherwise.
        """
        return self._can_merge_batch_transforms(
            first_group, second_group
        ) or self._can_merge_row_transforms(first_group, second_group)

    def _can_merge_row_transforms(
        self,
        first_group: List[MapTransformFn],
        second_group: List[MapTransformFn],
    ) -> bool:
        """Check redundant row-based map transform sequences.

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

        def is_intermediate_fn(fn):
            """Detect intermediate row MapTransformFn, RowMapTransformFn."""
            return isinstance(fn, RowMapTransformFn)

        can_merge = (
            # first_group end matches BuildOutputBlocksMapTransformFn in sequence
            isinstance(first_group[-1], BuildOutputBlocksMapTransformFn)
            # second_group start matches BlocksToRowsMapTransformFn in sequence
            and isinstance(second_group[0], BlocksToRowsMapTransformFn)
            # Both groups have identical start MapTransformFn including args
            and first_group[0] == second_group[0]
        )

        if can_merge:
            assert (
                # first_group matches matches intermediate MapTransformFn in sequence
                self._intermediate_functions_match(first_group, is_intermediate_fn)
                and
                # second_group matches matches intermediate MapTransformFn in sequence
                self._intermediate_functions_match(second_group, is_intermediate_fn)
            )

        return can_merge

    def _can_merge_batch_transforms(
        self,
        first_group: List[MapTransformFn],
        second_group: List[MapTransformFn],
    ) -> bool:
        """Check for redundant batch-based map transform sequences.

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

        def is_intermediate_fn(fn):
            """Detect intermediate batch MapTransformFn, BatchMapTransformFn."""
            return isinstance(fn, BatchMapTransformFn)

        can_merge = (
            # first_group end matches BuildOutputBlocksMapTransformFn in sequence
            isinstance(first_group[-1], BuildOutputBlocksMapTransformFn)
            # second_group start matches BlocksToBatchesMapTransformFn in sequence
            and isinstance(second_group[0], BlocksToBatchesMapTransformFn)
            # Both groups have identical start MapTransformFn including args
            and first_group[0] == second_group[0]
        )

        if can_merge:
            assert (
                # first_group matches matches intermediate MapTransformFn in sequence
                self._intermediate_functions_match(first_group, is_intermediate_fn)
                and
                # second_group matches matches intermediate MapTransformFn in sequence
                self._intermediate_functions_match(second_group, is_intermediate_fn)
            )

        return can_merge

    def _intermediate_functions_match(
        self,
        grouped_transform_fns: List[MapTransformFn],
        is_intermediate_fn: Callable[[MapTransformFn], bool],
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
            if not is_intermediate_fn(fn):
                return False

        return True


class FuseRepartitionOutputBlocks(ZeroCopyMapFusionRule):
    """This rule fuses BuildOutputBlocksMapTransformFn intended to repartition on
    target_num_rows_per_block with the previous fn, if the previous fn is also
    BuildOutputBlocksMapTransformFn.
    """

    def _optimize(self, transform_fns: List[MapTransformFn]) -> List[MapTransformFn]:
        new_transform_fns = [transform_fns[0]]
        for cur_fn in transform_fns[1:]:
            prev_fn = new_transform_fns[-1]
            if isinstance(cur_fn, BuildOutputBlocksMapTransformFn) and isinstance(
                prev_fn, BuildOutputBlocksMapTransformFn
            ):
                assert (
                    cur_fn.category
                    == prev_fn.category
                    == MapTransformFnCategory.PostProcess
                )
                prev_fn.set_target_num_rows_per_block(cur_fn.target_num_rows_per_block)
            else:
                new_transform_fns.append(cur_fn)

        return new_transform_fns
