from typing import TYPE_CHECKING, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators.one_to_one_operator import Download
from ray.data._internal.logical.rules.operator_fusion import are_remote_args_compatible

if TYPE_CHECKING:
    import pyarrow.fs


def _are_filesystems_compatible(
    fs1: Optional["pyarrow.fs.FileSystem"],
    fs2: Optional["pyarrow.fs.FileSystem"],
) -> bool:
    """Returns whether two ``Download`` operators can share one filesystem.

    A ``None`` filesystem means "infer it from the URI scheme at execution time",
    which isn't interchangeable with an explicitly supplied filesystem. So the two
    are only compatible when both are ``None`` or both compare equal.
    """
    if fs1 is None or fs2 is None:
        return fs1 is None and fs2 is None
    if fs1 is fs2:
        return True
    try:
        return bool(fs1 == fs2)
    except Exception:
        # Filesystems aren't required to implement comparison. Stay conservative
        # and leave the operators unfused rather than risk dropping one.
        return False


class CombineDownloads(Rule):
    """Combines consecutive ``Download`` operators into a single operator that downloads
    multiple columns.

    This optimization improves performance by reducing data movement and avoiding
    fragmented resource allocation.

    This rule only combines operators if they have identical resource requirements
    (`ray_remote_args`) and read through the same `filesystem`.

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
            input_op = op.input_dependencies[0]

            # Check if the input is also a Download operator
            if isinstance(input_op, Download):
                # Only combine if they have the same resource requirements
                if not are_remote_args_compatible(
                    input_op.ray_remote_args, op.ray_remote_args
                ):
                    return op

                # Only combine if both read through the same filesystem, since the
                # merged operator can carry just one.
                if not _are_filesystems_compatible(input_op.filesystem, op.filesystem):
                    return op

                # Combine the two Download operators
                combined_uri_columns = input_op.uri_column_names + op.uri_column_names
                combined_output_columns = (
                    input_op.output_bytes_column_names + op.output_bytes_column_names
                )

                return Download(
                    uri_column_names=combined_uri_columns,
                    output_bytes_column_names=combined_output_columns,
                    # Take the upstream operator's args, matching `FuseOperators`.
                    # `are_remote_args_compatible` lets the upstream operator carry
                    # inheritable args (`scheduling_strategy`, `label_selector`) that
                    # the downstream one omits, but not the reverse -- so upstream is
                    # the superset here. Using the downstream args instead would
                    # silently drop those settings.
                    ray_remote_args=input_op.ray_remote_args,
                    # Guaranteed equal to `op.filesystem` by the check above; take
                    # the upstream one for consistency with `ray_remote_args`.
                    filesystem=input_op.filesystem,
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
