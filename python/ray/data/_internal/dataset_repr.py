from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import numpy as np

import ray
from ray.data._internal.logical.interfaces import SourceOperator
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.exceptions import RayError
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data._internal.logical.interfaces.operator import Operator
    from ray.data.dataset import Dataset, Schema

_DATASET_REPR_ELLIPSIS = "…"  # Ellipsis marker for truncated cells/rows.
_DATASET_REPR_MAX_ROWS = 10  # Total preview row budget when materialized.
_DATASET_REPR_HEAD_ROWS = 5  # Number of head rows to show before the gap.
_DATASET_REPR_MAX_COLUMN_WIDTH = 40  # Max width per column cell in the table.
_DATASET_REPR_GET_TIMEOUT_S = 30.0  # Timeout for fetching preview blocks.

__all__ = [
    "build_dataset_ascii_repr",
    "build_dataset_summary_repr",
]


def build_dataset_ascii_repr(
    dataset: "Dataset",
    schema: "Schema",
    is_materialized: bool,
) -> str:
    """Render the dataset as a multi-line tabular string."""
    columns = list(schema.names)
    if not columns:
        return build_dataset_summary_repr(dataset)

    num_rows = dataset._meta_count()
    head_rows: List[List[str]] = []
    tail_rows: List[List[str]] = []
    if is_materialized:
        try:
            head_data, tail_data, _ = _collect_materialized_rows_for_repr(
                dataset, num_rows
            )
            head_rows = _format_rows_for_repr(head_data, columns)
            tail_rows = _format_rows_for_repr(tail_data, columns)
        except RayError:
            head_rows = []
            tail_rows = []

    return _build_dataset_ascii_repr_from_rows(
        schema=schema,
        num_rows=num_rows,
        dataset_name=dataset.name,
        is_materialized=is_materialized,
        head_rows=head_rows,
        tail_rows=tail_rows,
    )


def _build_dataset_ascii_repr_from_rows(
    *,
    schema: "Schema",
    num_rows: Optional[int],
    dataset_name: Optional[str],
    is_materialized: bool,
    head_rows: List[List[str]],
    tail_rows: List[List[str]],
) -> str:
    """Render the dataset repr given schema metadata and preview rows."""
    columns = list(schema.names)
    num_cols = len(columns)
    shape_line = f"shape: ({num_rows if num_rows is not None else '?'}, {num_cols})"

    # Build header rows from schema.
    dtype_strings = [_repr_format_dtype(t) for t in schema.types]
    column_headers = [
        _truncate_to_cell_width(str(col), _DATASET_REPR_MAX_COLUMN_WIDTH)
        for col in columns
    ]
    dtype_headers = [
        _truncate_to_cell_width(dtype, _DATASET_REPR_MAX_COLUMN_WIDTH)
        for dtype in dtype_strings
    ]
    separator_row = ["---"] * len(columns)

    # Assemble rows, including an ellipsis gap if needed.
    show_gap = bool(head_rows) and bool(tail_rows)
    display_rows: List[List[str]] = []
    display_rows.extend(head_rows)
    if show_gap:
        display_rows.append([_DATASET_REPR_ELLIPSIS] * len(columns))
    display_rows.extend(tail_rows)

    # Render the table with computed column widths.
    column_widths = _compute_column_widths(
        column_headers, dtype_headers, separator_row, display_rows
    )

    table_lines = _render_table_lines(
        column_headers,
        dtype_headers,
        separator_row,
        display_rows,
        column_widths,
    )

    # Append a summary line describing row coverage.
    num_rows_shown = len(head_rows) + len(tail_rows)
    summary_line = (
        f"(Showing {num_rows_shown} of {num_rows} rows)"
        if is_materialized
        else "(Dataset isn't materialized)"
    )
    if is_materialized and num_rows is None:
        summary_line = f"(Showing {num_rows_shown} of ? rows)"

    components = []
    if dataset_name is not None:
        components.append(f"name: {dataset_name}")
    components.extend([shape_line, "\n".join(table_lines), summary_line])
    return "\n".join(components)


def _repr_format_dtype(dtype: object) -> str:
    """Format a dtype into a compact string for the schema row.

    Dtypes may come from PyArrow, pandas/NumPy, or be plain Python types.
    """
    if isinstance(dtype, type):
        return dtype.__name__
    name = getattr(dtype, "name", None)
    if isinstance(name, str):
        return name
    return str(dtype)


def _collect_materialized_rows_for_repr(
    dataset: "Dataset",
    num_rows: Optional[int],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    """Collect head/tail rows for preview and whether to show a gap row."""
    block_entries: List[Tuple[ObjectRef, BlockMetadata]] = []
    for ref_bundle in dataset.iter_internal_ref_bundles():
        block_entries.extend(zip(ref_bundle.block_refs, ref_bundle.metadata))

    if not block_entries:
        return [], [], False

    # Compute how many head/tail rows to show within the preview budget.
    head_row_limit, tail_row_limit = _determine_preview_row_targets(num_rows)
    block_cache: Dict[ObjectRef, Block] = {}

    def _resolve_block(block_ref: ObjectRef) -> Block:
        if block_ref not in block_cache:
            block_cache[block_ref] = ray.get(
                block_ref, timeout=_DATASET_REPR_GET_TIMEOUT_S
            )
        return block_cache[block_ref]

    head_rows: List[Dict[str, Any]] = []
    head_remaining = head_row_limit
    for block_ref, _ in block_entries:
        if head_remaining <= 0:
            break
        block = _resolve_block(block_ref)
        accessor = BlockAccessor.for_block(block)
        for row in accessor.iter_rows(public_row_format=True):
            head_rows.append(row)
            head_remaining -= 1
            if head_remaining <= 0:
                break

    tail_rows: List[Dict[str, Any]] = []
    tail_remaining = tail_row_limit
    tail_parts: List[List[Dict[str, Any]]] = []
    if tail_remaining > 0:
        for block_ref, metadata in reversed(block_entries):
            if tail_remaining <= 0:
                break
            block = _resolve_block(block_ref)
            accessor = BlockAccessor.for_block(block)
            total_rows = metadata.num_rows
            if total_rows is None:
                total_rows = accessor.num_rows()
            if total_rows == 0:
                continue
            start = max(0, total_rows - tail_remaining)
            sliced_block = accessor.slice(start, total_rows, copy=False)
            slice_accessor = BlockAccessor.for_block(sliced_block)
            block_rows = list(slice_accessor.iter_rows(public_row_format=True))
            tail_parts.append(block_rows)
            tail_remaining -= len(block_rows)
            if tail_remaining <= 0:
                break

    for part in reversed(tail_parts):
        tail_rows.extend(part)

    show_gap = bool(head_rows) and bool(tail_rows)
    return head_rows, tail_rows, show_gap


def _determine_preview_row_targets(num_rows: Optional[int]) -> Tuple[int, int]:
    """Compute how many head and tail rows to preview."""
    max_rows = _DATASET_REPR_MAX_ROWS
    if num_rows is None or num_rows <= max_rows:
        head = num_rows if num_rows is not None else max_rows
        return head, 0

    head = min(_DATASET_REPR_HEAD_ROWS, max_rows)
    tail = max_rows - head
    return head, tail


def _format_rows_for_repr(
    rows: List[Dict[str, Any]],
    column_names: List[str],
) -> List[List[str]]:
    """Format row dicts into string cell rows for table rendering."""
    formatted_rows: List[List[str]] = []
    for row in rows:
        formatted_row = []
        for column in column_names:
            value = row.get(column)
            formatted_value = _format_value(value)
            formatted_row.append(
                _truncate_to_cell_width(formatted_value, _DATASET_REPR_MAX_COLUMN_WIDTH)
            )
        formatted_rows.append(formatted_row)
    return formatted_rows


def _format_value(value: Any) -> str:
    if isinstance(value, np.generic):
        value = value.item()
    return str(value).replace("\n", " ").replace("\r", " ")


def _truncate_to_cell_width(value: str, max_width: int) -> str:
    """Truncate a single cell to the configured max width."""
    if max_width is None:
        return value
    if max_width <= 0:
        return _DATASET_REPR_ELLIPSIS if value else ""
    if len(value) <= max_width:
        return value
    if max_width == 1:
        return _DATASET_REPR_ELLIPSIS
    return value[: max_width - 1] + _DATASET_REPR_ELLIPSIS


def _compute_column_widths(
    headers: List[str],
    dtype_headers: List[str],
    separator_row: List[str],
    data_rows: List[List[str]],
) -> List[int]:
    """Compute per-column widths for table rendering."""
    column_widths: List[int] = []
    for idx in range(len(headers)):
        widths = [
            len(headers[idx]),
            len(dtype_headers[idx]),
            len(separator_row[idx]),
        ]
        for row in data_rows:
            widths.append(len(row[idx]))
        column_widths.append(max(widths))
    return column_widths


def _render_table_lines(
    headers: List[str],
    dtype_headers: List[str],
    separator_row: List[str],
    data_rows: List[List[str]],
    column_widths: List[int],
) -> List[str]:
    """Render the full table (borders, headers, data) as lines."""
    lines: List[str] = []
    top = _render_border("╭", "┬", "╮", "─", column_widths)
    header_row = _render_row(headers, column_widths)
    separator_line = _render_row(separator_row, column_widths)
    dtype_row = _render_row(dtype_headers, column_widths)
    lines.extend([top, header_row, separator_line, dtype_row])

    if data_rows:
        middle = _render_border("╞", "╪", "╡", "═", column_widths)
        lines.append(middle)
        for row in data_rows:
            lines.append(_render_row(row, column_widths))

    bottom = _render_border("╰", "┴", "╯", "─", column_widths)
    lines.append(bottom)
    return lines


def _render_border(
    left: str, middle: str, right: str, fill: str, column_widths: List[int]
) -> str:
    """Render a table border line given column widths."""
    segments = [fill * (width + 2) for width in column_widths]
    return f"{left}{middle.join(segments)}{right}"


def _render_row(values: List[str], column_widths: List[int]) -> str:
    """Render a single table row with padding."""
    cells = []
    for idx, value in enumerate(values):
        padded = value.ljust(column_widths[idx])
        cells.append(f" {padded} ")
    return f"│{'┆'.join(cells)}│"


def _format_operator_dag(
    op: "Operator",
    curr_str: str = "",
    depth: int = 0,
    including_source: bool = True,
    show_op_repr: bool = False,
) -> Tuple[str, int]:
    """Traverse (DFS) the Plan DAG and
    return a string representation of the operators."""
    if not including_source and isinstance(op, SourceOperator):
        return curr_str, depth

    curr_max_depth = depth

    # For logical plan, only show the operator name like "Aggregate".
    # But for physical plan, show the operator class name as well like
    # "AllToAllOperator[Aggregate]".
    op_str = repr(op) if show_op_repr else op.name

    if depth == 0:
        curr_str += f"{op_str}\n"
    else:
        trailing_space = " " * ((depth - 1) * 3)
        curr_str += f"{trailing_space}+- {op_str}\n"

    for input in op.input_dependencies:
        curr_str, input_max_depth = _format_operator_dag(
            input, curr_str, depth + 1, including_source, show_op_repr
        )
        curr_max_depth = max(curr_max_depth, input_max_depth)
    return curr_str, curr_max_depth


def build_dataset_summary_repr(dataset: "Dataset") -> str:
    """Create a cosmetic string representation of a dataset.

    This is used for Dataset.__repr__ when no tabular preview is available.
    Must be very cheap — never forces execution.
    """
    from ray.data.dataset import MaterializedDataset

    dataset_cls = type(dataset)
    logical_plan = dataset._logical_plan
    dataset_name = dataset._plan._dataset_name

    plan_str = ""
    plan_max_depth = 0

    if not dataset._plan.has_computed_output():
        plan_str, plan_max_depth = _format_operator_dag(
            logical_plan.dag, including_source=False
        )

    schema = dataset._base_schema(fetch_if_missing=False)
    count = dataset._plan._cache.get_num_rows(logical_plan.dag)

    if schema is None or count is None:
        has_n_ary_operator = False
        dag = logical_plan.dag

        while not isinstance(dag, SourceOperator):
            if len(dag.input_dependencies) > 1:
                has_n_ary_operator = True
                break

            dag = dag.input_dependencies[0]

        # TODO(@bveeramani): Handle schemas for n-ary operators like `Union`.
        if not has_n_ary_operator:
            assert isinstance(dag, SourceOperator), dag
            # We infer from logical plan's dag directly as we know that
            # we don't have any cached values, so inferring is the only
            # option left.
            if schema is None:
                schema = dag.infer_schema()
            if count is None:
                count = dag.infer_metadata().num_rows

    if schema is None:
        schema_str = "Unknown schema"
    elif isinstance(schema, type):
        schema_str = str(schema)
    else:
        schema_str = []
        for n, t in zip(schema.names, schema.types):
            if hasattr(t, "__name__"):
                t = t.__name__
            schema_str.append(f"{n}: {t}")
        schema_str = ", ".join(schema_str)
        schema_str = "{" + schema_str + "}"

    if count is None:
        count = "?"

    num_blocks = None
    if dataset_cls == MaterializedDataset:
        num_blocks = logical_plan.initial_num_blocks()
        assert num_blocks is not None

    name_str = "name={}, ".format(dataset_name) if dataset_name is not None else ""
    num_blocks_str = f"num_blocks={num_blocks}, " if num_blocks else ""

    dataset_str = "{}({}{}num_rows={}, schema={})".format(
        dataset_cls.__name__,
        name_str,
        num_blocks_str,
        count,
        schema_str,
    )

    # If the resulting string representation fits in one line, use it directly.
    SCHEMA_LINE_CHAR_LIMIT = 80
    MIN_FIELD_LENGTH = 10
    INDENT_STR = " " * 3
    trailing_space = INDENT_STR * plan_max_depth

    if len(dataset_str) > SCHEMA_LINE_CHAR_LIMIT:
        # If the resulting string representation exceeds the line char limit,
        # first try breaking up each `Dataset` parameter into its own line
        # and check if each line fits within the line limit. We check the
        # `schema` param's length, since this is likely the longest string.
        schema_str_on_new_line = f"{trailing_space}{INDENT_STR}schema={schema_str}"
        if len(schema_str_on_new_line) > SCHEMA_LINE_CHAR_LIMIT:
            # If the schema cannot fit on a single line, break up each field
            # into its own line.
            schema_str = []
            for n, t in zip(schema.names, schema.types):
                if hasattr(t, "__name__"):
                    t = t.__name__
                col_str = f"{trailing_space}{INDENT_STR * 2}{n}: {t}"
                # If the field line exceeds the char limit, abbreviate
                # the field name to fit while maintaining the full type
                if len(col_str) > SCHEMA_LINE_CHAR_LIMIT:
                    shortened_suffix = f"...: {str(t)}"
                    # Show at least 10 characters of the field name, even if
                    # we have already hit the line limit with the type.
                    chars_left_for_col_name = max(
                        SCHEMA_LINE_CHAR_LIMIT - len(shortened_suffix),
                        MIN_FIELD_LENGTH,
                    )
                    col_str = f"{col_str[:chars_left_for_col_name]}{shortened_suffix}"
                schema_str.append(col_str)
            schema_str = ",\n".join(schema_str)
            schema_str = "{\n" + schema_str + f"\n{trailing_space}{INDENT_STR}" + "}"
        name_str = (
            f"\n{trailing_space}{INDENT_STR}name={dataset_name},"
            if dataset_name is not None
            else ""
        )
        num_blocks_str = (
            f"\n{trailing_space}{INDENT_STR}num_blocks={num_blocks},"
            if num_blocks
            else ""
        )
        dataset_str = (
            f"{dataset_cls.__name__}("
            f"{name_str}"
            f"{num_blocks_str}"
            f"\n{trailing_space}{INDENT_STR}num_rows={count},"
            f"\n{trailing_space}{INDENT_STR}schema={schema_str}"
            f"\n{trailing_space})"
        )

    if plan_max_depth == 0:
        plan_str += dataset_str
    else:
        plan_str += f"{INDENT_STR * (plan_max_depth - 1)}+- {dataset_str}"
    return plan_str
