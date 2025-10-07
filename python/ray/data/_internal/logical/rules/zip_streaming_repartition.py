from copy import copy

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import StreamingRepartition
from ray.data._internal.logical.operators.n_ary_operator import Zip


class InsertStreamingRepartitionBeforeZip(Rule):
    """Insert StreamingRepartition on each Zip input to normalize rows per block.

    The target rows-per-block is read from DataContext via the
    "zip_streaming_repartition_target_rows_per_block" config key, defaulting to 128.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        ctx = plan.context
        target = ctx.get_config("zip_streaming_repartition_target_rows_per_block", 128)

        def transform(op: LogicalOperator) -> LogicalOperator:
            if not isinstance(op, Zip):
                return op
            print("Inserting StreamingRepartition before Zip")
            new_inputs = []
            for inp in op.input_dependencies:
                # Avoid double-inserting if already normalized upstream
                if isinstance(inp, StreamingRepartition):
                    new_inputs.append(inp)
                else:
                    new_inputs.append(
                        StreamingRepartition(inp, target_num_rows_per_block=target)
                    )

            new_zip = copy(op)
            new_zip._input_dependencies = new_inputs
            for ni in new_inputs:
                ni._output_dependencies.append(new_zip)
            return new_zip

        optimized = plan.dag._apply_transform(transform)
        return LogicalPlan(dag=optimized, context=ctx)
