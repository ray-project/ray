from abc import abstractmethod
from typing import List, Type

from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BuildOutputBlocksMapTransformFn,
    MapTransformFn,
    MapTransformFnCategory,
    MapTransformFnDataType,
)
from ray.data._internal.logical.interfaces.optimizer import Rule
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
from ray.data._internal.logical.rules.operator_fusion import FuseOperators


class ZeroCopyMapFusionRule(Rule):
    """Base abstract class for all zero-copy map fusion rules.

    A zero-copy map fusion rule is a rule that optimizes the transform_fn chain of
    a fused MapOperator. The optimization is usually done by removing unnecessary
    data conversions.

    This base abstract class defines the common util functions. And subclasses
    should implement the `_optimize` method for the concrete optimization
    strategy.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        return [FuseOperators]

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._traverse(plan.dag)
        return plan

    def _traverse(self, op):
        """Traverse the DAG and apply the optimization to each MapOperator."""
        if isinstance(op, MapOperator):
            map_transformer = op.get_map_transformer()
            transform_fns = map_transformer.get_transform_fns()
            new_transform_fns = self._optimize(transform_fns)
            # Physical operators won't be shared,
            # so it's safe to modify the transform_fns in place.
            map_transformer.set_transform_fns(new_transform_fns)

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
        ...


class EliminateBuildOutputBlocks(ZeroCopyMapFusionRule):
    """This rule eliminates unnecessary BuildOutputBlocksMapTransformFn
    (which is of category MapTransformFnCategory.PostProcess), if the previous fn
    already outputs blocks.

    This happens for the "Read -> Map/Write" fusion.
    """

    def _optimize(self, transform_fns: List[MapTransformFn]) -> List[MapTransformFn]:
        # For the following subsquence,
        # 1. Any MapTransformFn with block output.
        # 2. BuildOutputBlocksMapTransformFn
        # 3. Any MapTransformFn with block input.
        # We drop the BuildOutputBlocksMapTransformFn in the middle.
        new_transform_fns = []

        for i in range(len(transform_fns)):
            cur_fn = transform_fns[i]
            drop = False
            if (
                i > 0
                and i < len(transform_fns) - 1
                and isinstance(cur_fn, BuildOutputBlocksMapTransformFn)
            ):
                assert cur_fn.category == MapTransformFnCategory.PostProcess
                prev_fn = transform_fns[i - 1]
                next_fn = transform_fns[i + 1]
                if (
                    prev_fn.output_type == MapTransformFnDataType.Block
                    and next_fn.input_type == MapTransformFnDataType.Block
                ):
                    assert prev_fn.category == MapTransformFnCategory.DataProcess
                    drop = True
            if not drop:
                new_transform_fns.append(cur_fn)

        return new_transform_fns
