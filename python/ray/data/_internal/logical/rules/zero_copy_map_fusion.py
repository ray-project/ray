from abc import abstractmethod
from typing import List

from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformFn,
    MapTransformFnDataType,
)
from ray.data._internal.logical.interfaces.optimizer import Rule
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan


class ZeroCopyMapFusionRule(Rule):
    """Base class for zero-copy map fusion rules.

    Subclasses implement the optimization strategies for different combinations of
    fused map operators, by dropping unnecessary data conversion `MapTransformFn`s.
    """

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._traverse(plan.dag)
        return plan

    def _traverse(self, op):
        if isinstance(op, MapOperator):
            map_transformer = op.get_map_transformer()
            transform_fns = map_transformer._transform_fns
            new_transform_fns = self._optimize(transform_fns)
            # Physical operators won't be shared,
            # so it's safe to modify the transform_fns in place.
            map_transformer._transform_fns = new_transform_fns

        for input_op in op.input_dependencies:
            self._traverse(input_op)

    @abstractmethod
    def _optimize(self, transform_fns: List[MapTransformFn]) -> List[MapTransformFn]:
        """Optimize the transform_fns chain of a MapOperator.

        Args:
            transform_fns: The old transform_fns chain.
        Returns:
            The optimized transform_fns chain.
        """
        pass


class ReadOpZeroCopyMapFusion(ZeroCopyMapFusionRule):
    """Optimize Read -> Map/Write."""

    def _optimize(self, transform_fns: List[MapTransformFn]) -> List[MapTransformFn]:
        # For Read -> Map/Write, transform_fns will contain the following subsequence:
        # 1. BlockMapTransformFn
        # 2. BuildOutputBlocksMapTransformFn
        # 3. Any MapTransformFn with block input.
        # In this case, we can drop the BuildOutputBlocksMapTransformFn.
        new_transform_fns = []

        for i in range(len(transform_fns)):
            cur_fn = transform_fns[i]
            drop = False
            if (
                i > 0
                and i < len(transform_fns) - 1
                and isinstance(cur_fn, BuildOutputBlocksMapTransformFn)
            ):
                prev_fn = transform_fns[i - 1]
                next_fn = transform_fns[i + 1]
                if (
                    isinstance(prev_fn, BlockMapTransformFn)
                    and next_fn.input_type == MapTransformFnDataType.Block
                ):
                    drop = True
            if not drop:
                new_transform_fns.append(cur_fn)

        return new_transform_fns
