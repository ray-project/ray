from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators.one_to_one_operator import Download
from ray.data._internal.logical.rules.operator_fusion import are_remote_args_compatible


class CombineDownloads(Rule):
    """Combines consecutive ``Download`` operators into a single operator that downloads
    multiple columns.

    This optimization improves performance by reducing data movement and avoiding
    fragmented resource allocation.

    This rule only combines operators if they have identical resource requirements
    (`ray_remote_args`).

    Example:

        Before optimization::

            ds.with_column("bytes1", download("uri1"))
              .with_column("bytes2", download("uri2"))
              .with_column("bytes3", download("uri3"))

        Creates three separate ``Download`` operators in the DAG.

        After optimization:

            A single ``Download`` operator with:

                uri_column_names = ["uri1", "uri2", "uri3"]
                output_bytes_column_names = ["bytes1", "bytes2", "bytes3"]
    """

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, LogicalPlan)

        def _combine_downloads(op: LogicalOperator) -> LogicalOperator:
            if not isinstance(op, Download):
                return op

            assert len(op.input_dependencies) == 1, (
                f"Expected `Download` to have exactly one input dependency, but got "
                f"{len(op.input_dependencies)}"
            )
            input_op = op.input_dependencies[0]

            # Check if the input is also a Download operator
            if isinstance(input_op, Download):
                # Only combine if they have the same resource requirements
                if not are_remote_args_compatible(
                    input_op.ray_remote_args, op.ray_remote_args
                ):
                    return op

                # Combine the two Download operators
                combined_uri_columns = input_op.uri_column_names + op.uri_column_names
                combined_output_columns = (
                    input_op.output_bytes_column_names + op.output_bytes_column_names
                )

                return Download(
                    uri_column_names=combined_uri_columns,
                    output_bytes_column_names=combined_output_columns,
                    ray_remote_args=op.ray_remote_args,
                    input_dependencies=[input_op.input_dependencies[0]],
                )

            return op

        original_dag = plan.dag
        transformed_dag = original_dag._apply_transform(_combine_downloads)

        if transformed_dag is original_dag:
            return plan

        return LogicalPlan(
            dag=transformed_dag,
            context=plan.context,
        )
